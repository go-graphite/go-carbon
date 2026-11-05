package chunkstore

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/pebble/vfs"
)

func TestColdPackStoredPointsCheckpoint(t *testing.T) {
	// Keep virtual-clock trials in the shared isolated Pebble subprocess.
	pebbleSynctest(t, func(t *testing.T) {
		for _, workers := range []int{1, 8} {
			func() {
				t.Logf("workers=%d", workers)
				codec := &parallelCursorCodec{}
				o := parallelOptions(vfs.NewMem(), codec, workers)
				s, _ := openTestStore(t, "/store", o)
				s.StopBackground()
				const metrics = ColdPackPageSize + 6
				for i := range metrics {
					createTestMetric(t, s, fmt.Sprintf("count.%03d", i))
				}
				check := func(points, metrics uint64) {
					t.Helper()
					if st := s.ColdPackStats(); st.StoredPoints != points || st.StoredMetrics != metrics {
						t.Fatalf("stored points: %+v; want %d points / %d metrics", st, points, metrics)
					}
				}
				check(0, 0)
				if s.ColdPackStats().StoredAt != 0 {
					t.Fatal("new store has a completed count")
				}
				cursorTestSteps(t, s, 4) // checkpoint inside a metric, after a completed wave
				check(0, 0)
				closeTestStore(t, s)
				s, _ = openTestStore(t, "/store", o)
				s.StopBackground()
				check(0, 0)
				if !cursorTestSteps(t, s, metrics*4) {
					t.Fatal("resumed pass did not finish")
				}
				check(metrics*600, metrics)
				completed := s.ColdPackStats().StoredAt
				if completed == 0 {
					t.Fatal("missing completion timestamp")
				}
				closeTestStore(t, s)
				s, _ = openTestStore(t, "/store", o)
				s.StopBackground()
				defer closeTestStore(t, s)
				check(metrics*600, metrics)
				cursorTestSteps(t, s, 1)
				check(metrics*600, metrics) // publish no running partial sum
				if s.ColdPackStats().StoredAt != completed {
					t.Fatal("unfinished pass changed completion timestamp")
				}
				// Policy resets discard unfinished counts but retain the last pass.
				o.ColdPack.Mode = "unpack"
				if err := s.ConfigureColdPack(o.ColdPack); err != nil {
					t.Fatal(err)
				}
				codec.skip = true
				if !cursorTestSteps(t, s, metrics*4) {
					t.Fatal("skipped pass did not finish")
				}
				check(0, 0) // unsupported or unread metrics do not imply zero points
				codec.skip = false
				if err := s.RunColdPack(context.Background()); err != nil {
					t.Fatal(err)
				}
				check(metrics*600, metrics)
			}()
		}
	})
}
