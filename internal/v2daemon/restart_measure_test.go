package v2daemon

import (
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/internal/v2core"
)

func restartMeasureConfig() Config {
	segment, _ := strconv.ParseInt(os.Getenv("V2_RESTART_MEASURE_SEGMENT"), 10, 64)
	if segment == 0 {
		segment = 1 << 20
	}
	shards, _ := strconv.Atoi(os.Getenv("V2_RESTART_MEASURE_SHARDS"))
	if shards == 0 {
		shards = 8
	}
	return Config{Layer: 3, Producers: 8, Shards: shards, GroupInterval: time.Hour, SealInterval: time.Hour}
}

// TestCoreOpenMeasure times core open of a kept measurement store at several
// GOMAXPROCS values; the open limit is min(16, GOMAXPROCS). Env-gated.
func TestCoreOpenMeasure(t *testing.T) {
	root := os.Getenv("V2_CORE_OPEN_DIR")
	if root == "" {
		t.Skip("set V2_CORE_OPEN_DIR")
	}
	for _, procs := range []int{2, 4, 8, 16, 2, 4, 8, 16} {
		dir := t.TempDir()
		if err := os.CopyFS(dir, os.DirFS(root)); err != nil {
			t.Fatal(err)
		}
		old := runtime.GOMAXPROCS(procs)
		t0 := time.Now()
		e, err := v2core.Open(filepath.Join(dir, "placement-group-00000001", "core"), coreConfig(restartMeasureConfig()), v2core.Hooks{})
		open := time.Since(t0)
		runtime.GOMAXPROCS(old)
		if err != nil {
			t.Fatal(err)
		}
		if err = e.Close(); err != nil {
			t.Fatal(err)
		}
		t.Logf("gomaxprocs=%d core-open=%.2fs", procs, open.Seconds())
		_ = os.RemoveAll(dir)
	}
}
