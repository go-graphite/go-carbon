package chunkstore

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"regexp"
	"runtime"
	"runtime/debug"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/cockroachdb/pebble/objstorage/objstorageprovider"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/vfs"
)

// pebbleSynctest gives each Pebble bubble its own process. Pebble pools sstable
// write tasks with channels that retain the bubble in which they were created,
// even when writer parallelism is disabled. Sharing those tasks with another
// bubble or a normal test is a fatal runtime error. Keep all Pebble work inside f;
// pure clock tests can continue to use synctest.Test directly.
func pebbleSynctest(t *testing.T, f func(*testing.T)) {
	t.Helper()
	// A named subtest also isolates repeated bubbles in the same test function.
	t.Run("bubble", func(t *testing.T) {
		t.Helper()
		const childEnv = "GO_CARBON_PEBBLE_SYNCTEST_CHILD"
		if os.Getenv(childEnv) == t.Name() {
			synctest.Test(t, f)
			return
		}
		parts := strings.Split(t.Name(), "/")
		for i := range parts {
			parts[i] = "^" + regexp.QuoteMeta(parts[i]) + "$"
		}
		// This real-time deadline only detects a hung child; assertions use the
		// virtual clock in the bubble, unchanged.
		ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
		defer cancel()
		cmd := exec.CommandContext(ctx, os.Args[0], "-test.run="+strings.Join(parts, "/"),
			"-test.count=1", "-test.timeout=1m", fmt.Sprintf("-test.short=%t", testing.Short()))
		cmd.Env = append(os.Environ(), childEnv+"="+t.Name())
		output, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("Pebble bubble subprocess: %v\n%s", err, output)
		}
		t.Logf("%s", output)
	})
}

func TestPebbleSynctestIsolation(t *testing.T) {
	// Keep the synchronous writer on one P and prevent GC from discarding its
	// pooled write tasks between bubbles. With direct synctest.Test calls the
	// second writer reuses a compressionDone channel owned by the first bubble.
	procs := runtime.GOMAXPROCS(1)
	defer runtime.GOMAXPROCS(procs)
	gc := debug.SetGCPercent(-1)
	defer debug.SetGCPercent(gc)

	t.Run("between_bubbles", func(t *testing.T) {
		for range 2 {
			pebbleSynctest(t, writeSynctestTable)
		}
	})
	t.Run("outside_bubble", func(t *testing.T) {
		pebbleSynctest(t, writeSynctestTable)
		t.Run("outside", writeSynctestTable)
	})
}

func writeSynctestTable(t *testing.T) {
	t.Helper()
	fs := vfs.NewMem()
	file, err := fs.Create("table.sst")
	if err != nil {
		t.Fatal(err)
	}
	writer := sstable.NewWriter(objstorageprovider.NewFileWritable(file), sstable.WriterOptions{
		BlockSize: 1024,
	})
	value := bytes.Repeat([]byte("value"), 256)
	for i := range 16 {
		if err := writer.Set(fmt.Appendf(nil, "%04d", i), value); err != nil {
			t.Fatal(err)
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
}
