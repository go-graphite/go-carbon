package whisperio

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
)

type pausedOwnerCodec struct {
	*ContinuationCodec
	prepared, release chan struct{}
	fail              bool
}

func (c *pausedOwnerCodec) Update(ctx context.Context, kv store.ContinuationIO, m store.Metadata, p []store.Point, r int, clock func() time.Time, maintenance *store.ContinuationMaintenance) error {
	if err := c.ContinuationCodec.Update(ctx, kv, m, p, r, clock, maintenance); err != nil {
		return err
	}
	if c.prepared != nil {
		close(c.prepared)
		<-c.release
	}
	if c.fail {
		return errors.New("injected before commit")
	}
	return nil
}
func TestOwnerHeadsReadDuringPreparation(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(map[bool]string{false: "commit", true: "abort"}[fail], func(t *testing.T) {
			ctx := context.Background()
			codec := &pausedOwnerCodec{ContinuationCodec: &ContinuationCodec{OwnerHeads: NewOwnerHeads(1, 64<<10)}}
			opts := coldOptions(codec)
			opts.Now = classicTestClock
			db, err := openMemoryTestStore(t, t.TempDir(), opts)
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			db.StopBackground()
			cfg := store.MetricConfig{Name: "visible", Retentions: []store.Retention{{Step: 1, Count: 3600}}, AggregationMethod: store.Last}
			if _, err := db.Create(ctx, cfg); err != nil {
				t.Fatal(err)
			}
			if err := db.UpdateMany(ctx, cfg.Name, []store.Point{{Timestamp: 999990, Value: 1}}); err != nil {
				t.Fatal(err)
			}
			before, err := db.Snapshot(ctx, cfg.Name)
			if err != nil {
				t.Fatal(err)
			}
			codec.prepared, codec.release, codec.fail = make(chan struct{}), make(chan struct{}), fail
			done := make(chan error, 1)
			go func() { done <- db.UpdateMany(ctx, cfg.Name, []store.Point{{Timestamp: 999990, Value: 2}}) }()
			select {
			case <-codec.prepared:
			case <-time.After(5 * time.Second):
				t.Fatal("preparation blocked")
			}
			read := make(chan error, 1)
			go func() {
				during, e := db.Snapshot(ctx, cfg.Name)
				if e == nil {
					e = store.EqualSnapshots(before, during)
				}
				read <- e
			}()
			select {
			case err := <-read:
				if err != nil {
					t.Error(err)
				}
			case <-time.After(5 * time.Second):
				t.Error("read blocked on retained writer")
			}
			close(codec.release)
			err = <-done
			if (err != nil) != fail {
				t.Fatalf("unexpected commit outcome: %v", err)
			}
			codec.prepared = nil
			codec.fail = false
			after, err := db.Snapshot(ctx, cfg.Name)
			if err != nil {
				t.Fatal(err)
			}
			if fail && !reflect.DeepEqual(before.Metadata, after.Metadata) {
				t.Fatal("failed transaction published revision")
			}
			want := float64(2)
			if fail {
				want = 1
			}
			if len(after.Archives[0].Points) != 1 || after.Archives[0].Points[0].Value != want {
				t.Fatalf("bad visible state: %+v", after)
			}
			// A retry after failure must start from the committed, older revision.
			if err := db.UpdateMany(ctx, cfg.Name, []store.Point{{Timestamp: 999991, Value: 3}}); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestOwnerHeadsStoreIsolation(t *testing.T) {
	ctx := context.Background()
	codec := &ContinuationCodec{OwnerHeads: NewOwnerHeads(1, 128<<10)}
	opts := coldOptions(codec)
	opts.Now = classicTestClock
	a, err := openMemoryTestStore(t, t.TempDir(), opts)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	a.StopBackground()
	b, err := openMemoryTestStore(t, t.TempDir(), opts)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	b.StopBackground()
	cfg := store.MetricConfig{Name: "same.identity", Retentions: []store.Retention{{Step: 1, Count: 3600}}, AggregationMethod: store.Last}
	for i, db := range []*store.Store{a, b} {
		if _, err := db.Create(ctx, cfg); err != nil {
			t.Fatal(err)
		}
		if err := db.UpdateMany(ctx, cfg.Name, []store.Point{{Timestamp: 999990, Value: float64(i + 1)}}); err != nil {
			t.Fatal(err)
		}
	}
	if err := a.UpdateMany(ctx, cfg.Name, []store.Point{{Timestamp: 999991, Value: 3}}); err != nil {
		t.Fatal(err)
	}
	x, err := a.Snapshot(ctx, cfg.Name)
	if err != nil {
		t.Fatal(err)
	}
	if len(x.Archives[0].Points) != 2 || x.Archives[0].Points[0].Value != 1 {
		t.Fatalf("reused another store's retained group: %+v", x)
	}
	y, err := b.Snapshot(ctx, cfg.Name)
	if err != nil {
		t.Fatal(err)
	}
	if len(y.Archives[0].Points) != 1 || y.Archives[0].Points[0].Value != 2 {
		t.Fatalf("changed other store: %+v", y)
	}
}
