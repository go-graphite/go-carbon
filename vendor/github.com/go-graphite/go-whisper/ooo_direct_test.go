package whisper

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

// TestDirectLateWritesNeedPatchableStorage checks that a plain file, which
// cannot patch blocks, keeps diverting late points to the sidecar with
// OutOfOrderDirect set, and counts why.
func TestDirectLateWritesNeedPatchableStorage(t *testing.T) {
	rets, err := ParseRetentionDefs("1s:10m,1m:1h")
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "m.wsp")
	now := 1_700_000_040
	options := func() *Options {
		return &Options{Compressed: true, OutOfOrder: true, OutOfOrderDirect: true, Now: func() time.Time { return time.Unix(int64(now), 0) }}
	}
	w, err := CreateWithOptions(path, rets, Average, 0, options())
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	write := func(points []*TimeSeriesPoint) {
		t.Helper()
		w, err := OpenWithOptions(path, options())
		if err != nil {
			t.Fatal(err)
		}
		if err := w.UpdateMany(points); err != nil {
			t.Fatal(err)
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
	}
	write(seriesPoints(now-300, now, 1))
	before := DirectLatePoints(DirectUnsupported)
	now++
	write([]*TimeSeriesPoint{{Time: now - 200, Value: 7}})
	if _, err := os.Stat(OutOfOrderSidecarPath(path)); err != nil {
		t.Fatalf("late point not diverted to the sidecar: %v", err)
	}
	if DirectLatePoints(DirectUnsupported) == before {
		t.Fatal("unsupported storage not counted")
	}
}
