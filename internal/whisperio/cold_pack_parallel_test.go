package whisperio

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"os/exec"
	"reflect"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/cockroachdb/pebble/vfs"
	store "github.com/go-graphite/go-carbon/internal/chunkstore"
)

type parallelOracleCodec struct {
	ContinuationCodec
	afterPlan func(context.Context, string) error
}

func (c *parallelOracleCodec) Create(ctx context.Context, kv store.ContinuationIO, m store.Metadata, now func() time.Time) error {
	if err := c.ContinuationCodec.Create(ctx, kv, m, now); err != nil {
		return err
	}
	return kv.Set([]byte("oracle-name"), []byte(m.Name))
}
func (c *parallelOracleCodec) Repack(ctx context.Context, kv store.ContinuationIO, r store.ColdPackRequest) (store.RepackStats, uint64, bool, error) {
	stats, next, done, err := c.ContinuationCodec.Repack(ctx, kv, r)
	if err == nil && c.afterPlan != nil {
		name, e := kv.Get([]byte("oracle-name"))
		if e != nil {
			return stats, next, done, e
		}
		err = c.afterPlan(ctx, string(name))
	}
	return stats, next, done, err
}

func TestColdPackParallelSerialOracle(t *testing.T) {
	// Real-codec fixtures flush SSTs. Pebble's global pool retains channels, so
	// isolate virtual-clock work from other tests and repeated invocations.
	if os.Getenv("COLD_PACK_PARALLEL_ORACLE_CHILD") != "1" {
		ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
		defer cancel()
		cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestColdPackParallelSerialOracle$", "-test.timeout=2m")
		cmd.Env = append(os.Environ(), "COLD_PACK_PARALLEL_ORACLE_CHILD=1")
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("oracle subprocess: %v\n%s", err, out)
		}
		return
	}
	synctest.Test(t, func(t *testing.T) {
		ctx := context.Background()
		fs := vfs.NewMem()
		parallelCodec := &parallelOracleCodec{}
		open := func(dir string, codec *parallelOracleCodec) *store.Store {
			opts := coldOptions(codec).WithFS(fs)
			opts.Now = func() time.Time { return time.Unix(coldBase+14*86400, 0) }
			db, err := store.Open(dir, opts)
			if err != nil {
				t.Fatal(err)
			}
			db.StopBackground()
			t.Cleanup(func() { db.Close() })
			return db
		}
		serial, parallel := open("/serial", &parallelOracleCodec{}), open("/parallel", parallelCodec)
		const count = store.ColdPackPageSize + 11
		name := func(i int) string { return fmt.Sprintf("oracle.%03d", i) }
		points := make([]store.Point, 10000)
		for i := range points {
			points[i] = store.Point{Timestamp: coldBase + int64(i*60), Value: 1000 + math.Sin(float64(i))*.0123456789012345}
		}
		for i := range count {
			n := 64
			if i%8 == 0 {
				n = len(points)
			}
			for _, db := range []*store.Store{serial, parallel} {
				if _, err := db.Create(ctx, store.MetricConfig{Name: name(i), Retentions: []store.Retention{{Step: 60, Count: 50000}, {Step: 3600, Count: 8760}}, AggregationMethod: store.Average}); err != nil {
					t.Fatal(err)
				}
				if err := db.UpdateMany(ctx, name(i), points[:n]); err != nil {
					t.Fatal(err)
				}
			}
		}
		planned, written := make(chan struct{}), make(chan error, 1)
		var once sync.Once
		parallelCodec.afterPlan = func(ctx context.Context, metric string) error {
			if metric != name(0) {
				return nil
			}
			var err error
			once.Do(func() {
				close(planned)
				select {
				case err = <-written:
				case <-ctx.Done():
					err = ctx.Err()
				}
			})
			return err
		}
		go func() {
			<-planned
			// Change an already planned range, append to a sparse series, and delete
			// another admitted name. The revision fence must reject stale publication.
			for _, db := range []*store.Store{parallel, serial} {
				if err := db.UpdateMany(ctx, name(0), []store.Point{{Timestamp: coldBase + 60, Value: 123456.125}}); err != nil {
					written <- err
					return
				}
				if err := db.UpdateMany(ctx, name(2), []store.Point{{Timestamp: coldBase + 70*60, Value: -7.25}}); err != nil {
					written <- err
					return
				}
				if err := db.Delete(ctx, name(1)); err != nil {
					written <- err
					return
				}
			}
			written <- nil
		}()
		opts := store.ColdPackOptions{Mode: "exact", Workers: 8, CPUFraction: 1, BytesPerSecond: 1 << 40, WriteWindow: time.Second, MaxCompactionDebt: 1 << 40}
		if err := parallel.ConfigureColdPack(opts); err != nil {
			t.Fatal(err)
		}
		if err := parallel.RunColdPack(ctx); err != nil {
			t.Fatal(err)
		}
		if st := parallel.ColdPackStats(); st.Conflicts == 0 || st.Passes != 1 {
			t.Fatalf("write did not conflict: %+v", st)
		}
		// Retry conflicts in a subsequent pass, as with serial maintenance.
		if err := parallel.RunColdPack(ctx); err != nil {
			t.Fatal(err)
		}
		opts.Workers = 1
		if err := serial.ConfigureColdPack(opts); err != nil {
			t.Fatal(err)
		}
		if err := serial.RunColdPack(ctx); err != nil {
			t.Fatal(err)
		}
		for i := range count {
			if i == 1 {
				for _, db := range []*store.Store{serial, parallel} {
					if _, err := db.Metadata(ctx, name(i)); !errors.Is(err, store.ErrNotFound) {
						t.Fatalf("deleted name resurrected: %v", err)
					}
				}
				continue
			}
			want, err := serial.Snapshot(ctx, name(i))
			if err != nil {
				t.Fatal(err)
			}
			got, err := parallel.Snapshot(ctx, name(i))
			if err != nil {
				t.Fatal(err)
			}
			if err := store.EqualSnapshots(want, got); err != nil {
				t.Fatalf("%s points: %v", name(i), err)
			}
			a, err := serial.ExportTransfer(ctx, []string{name(i)})
			if err != nil {
				t.Fatal(err)
			}
			b, err := parallel.ExportTransfer(ctx, []string{name(i)})
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(a[0].Keys, b[0].Keys) {
				t.Fatalf("%s encoded bytes differ", name(i))
			}
		}
	})
}
