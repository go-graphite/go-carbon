package carbon

import (
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/receiver/tcp"
	whisper "github.com/go-graphite/go-whisper"
	"github.com/lomik/zapwriter"
)

func makeIntakeOriginal(path string, stamp int64) error {
	if err := os.MkdirAll(filepath.Dir(path), 0750); err != nil {
		return err
	}
	w, err := whisper.CreateWithOptions(path, whisper.NewRetentionsNoPointer([]whisper.Retention{whisper.NewRetention(1, 86400)}), whisper.Average, .5, &whisper.Options{Compressed: true, OutOfOrder: true, FLock: true})
	if err != nil {
		return err
	}
	defer w.Close()
	return w.UpdateMany([]*whisper.TimeSeriesPoint{{Time: int(stamp), Value: 17}, {Time: int(stamp + 1), Value: 18}})
}

// Exercise both original-file and already-owned targets with an overlapping
// live correction, then resume conversion and compare the final target data.
func TestLegacyRestoreIncrementalIntake(t *testing.T) {
	if isolatedTestProcess(t) {
		return
	}
	defer zapwriter.Test()()
	path, cfg, stamp := legacyIntakeFixture(t, 64, 10)
	cfg.Whisper.StoreIncrementalMigration = true
	// A legacy dump may be restored into a fresh continuation destination.
	cfg.Whisper.StoreDir = t.TempDir()
	cfg.Whisper.FLock = true
	cfg.Carbonserver.FileListCache = filepath.Join(t.TempDir(), "files.flc")
	cfg.Whisper.MaxUpdatesPerSecond = 20
	cfg.Whisper.StoreMigrationControlFile = filepath.Join(t.TempDir(), "control.json")
	incrementalLease(t, cfg, 1, 0)
	for _, name := range []string{"anchor", "saved.metric00063", "original.only"} {
		p := filepath.Join(cfg.Whisper.DataDir, strings.ReplaceAll(name, ".", string(filepath.Separator))+".wsp")
		if err := makeIntakeOriginal(p, stamp); err != nil {
			t.Fatal(err)
		}
	}
	app := New(path)
	app.Config = cfg
	if err := app.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(app.Stop)
	if !app.Carbonserver.MetricExists("original.only") {
		t.Fatal("mixed discovery omitted the unconverted original")
	}
	ctx := context.Background()
	if _, err := app.MetricStore.Create(ctx, store.MetricConfig{Name: "anchor", Retentions: []store.Retention{{Step: 1, Count: 86400}}, AggregationMethod: store.Average, XFilesFactor: .5}); err != nil {
		t.Fatal(err)
	}
	if err := app.MetricStore.UpdateMany(ctx, "anchor", []store.Point{{Timestamp: stamp + 1, Value: 44}}); err != nil {
		t.Fatal(err)
	}
	if app.Cache.RestoreOutstanding() == 0 {
		t.Fatal("intake waited for drain")
	}
	conn, err := net.Dial("tcp", app.Receivers[0].Receiver.(*tcp.TCP).Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	_, err = fmt.Fprintf(conn, "saved.metric00063 999.125 %d\nanchor 777.125 %d\n", stamp+1, stamp)
	conn.Close()
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(20 * time.Second)
	for !app.Cache.IsEmpty() {
		if time.Now().After(deadline) {
			t.Fatal("prefix did not drain")
		}
		time.Sleep(time.Millisecond)
	}
	incrementalLease(t, cfg, 2, 1)
	for _, tc := range []struct {
		name  string
		stamp int64
		value float64
	}{{"saved.metric00063", stamp + 1, 999.125}, {"anchor", stamp, 777.125}, {"anchor", stamp + 1, 44}} {
		for {
			s, err := app.MetricStore.Snapshot(context.Background(), tc.name)
			found := false
			if err == nil {
				for _, a := range s.Archives {
					for _, p := range a.Points {
						if p.Timestamp == tc.stamp && p.Value == tc.value {
							found = true
						}
					}
				}
			}
			if found {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("live correction lost for %s: %+v %v", tc.name, s, err)
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
	// Differential oracle: every restored timestamp/value must survive, with
	// only the two deliberate later corrections changing the dump's values.
	for i := range 64 {
		name := fmt.Sprintf("saved.metric%05d", i)
		snapshot, err := app.MetricStore.Snapshot(ctx, name)
		if err != nil {
			t.Fatal(name, err)
		}
		actual := make(map[int64]float64)
		for _, a := range snapshot.Archives {
			for _, p := range a.Points {
				actual[p.Timestamp] = p.Value
			}
		}
		if len(actual) != 10 {
			t.Fatalf("%s has %d points, want 10", name, len(actual))
		}
		for j := range 10 {
			want := float64(i*10+j) + .125
			if i == 0 && j == 1 {
				want = 777.125
			}
			if i == 63 && j == 1 {
				want = 999.125
			}
			if got, ok := actual[stamp+int64(j)]; !ok || got != want {
				t.Fatalf("%s point%d = %v/%v, want %v", name, j, got, ok, want)
			}
		}
	}
}
