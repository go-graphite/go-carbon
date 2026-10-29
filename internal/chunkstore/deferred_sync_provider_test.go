package chunkstore

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"testing"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/objstorage"
	"github.com/cockroachdb/pebble/objstorage/objstorageprovider"
	"github.com/cockroachdb/pebble/vfs"
)

// Stock Pebble's object provider syncs its directory only when an object was
// created or removed since its previous sync, and clears that flag before
// syncing. With deferred table syncs that directory sync is the barrier
// making a job's finished tables durable before the job records them in the
// manifest. Here job A creates its table, job B creates, finishes and syncs
// its own (clearing the flag A's create set), then A finishes and syncs the
// provider. Pebble would now commit both tables; after a crash both must
// hold their data. Without AlwaysSyncDir, A's sync skips the barrier and A's
// table is empty after the crash.
func TestDeferredSyncBarrierAfterAnotherJobsProviderSync(t *testing.T) {
	ctx := context.Background()
	strict := vfs.NewStrictMem()
	const dir = "/store"
	if err := strict.MkdirAll(dir, 0755); err != nil {
		t.Fatal(err)
	}
	if err := syncDir(strict, "/"); err != nil {
		t.Fatal(err)
	}
	fs := newDeferredSyncFS(strict, dir, 4)
	settings := objstorageprovider.DefaultSettings(fs, dir)
	settings.Local.AlwaysSyncDir = true
	provider, err := objstorageprovider.Open(settings)
	if err != nil {
		t.Fatal(err)
	}
	const table = 2 // base.FileTypeTable
	create := func(n int) objstorage.Writable {
		t.Helper()
		w, _, err := provider.Create(ctx, table, pebble.FileNum(n).DiskFileNum(), objstorage.CreateOptions{})
		if err != nil {
			t.Fatal(err)
		}
		return w
	}
	finish := func(w objstorage.Writable, data string) {
		t.Helper()
		if err := w.Write([]byte(data)); err != nil {
			t.Fatal(err)
		}
		if err := w.Finish(); err != nil {
			t.Fatal(err)
		}
	}
	a := create(1)
	b := create(2)
	finish(b, "table b")
	if err := provider.Sync(); err != nil {
		t.Fatal(err)
	}
	finish(a, "table a")
	if err := provider.Sync(); err != nil {
		t.Fatal(err)
	}
	strict.SetIgnoreSyncs(true)
	strict.ResetToSyncedState()
	strict.SetIgnoreSyncs(false)
	for n, want := range map[int]string{1: "table a", 2: "table b"} {
		name := strict.PathJoin(dir, fmt.Sprintf("%06d.sst", n))
		f, err := strict.Open(name)
		if err != nil {
			t.Fatalf("%s lost after its provider sync and a crash: %v", name, err)
		}
		got, err := io.ReadAll(f)
		_ = f.Close()
		if err != nil || !bytes.Equal(got, []byte(want)) {
			t.Fatalf("%s holds %q (%v) after its provider sync and a crash, want %q", name, got, err, want)
		}
	}
	if err := provider.Close(); err != nil {
		t.Fatal(err)
	}
}

// The store asks Pebble for the directory sync exactly when it defers table
// syncs to it.
func TestDeferredSyncAlwaysSyncsDirectory(t *testing.T) {
	t.Parallel()
	for _, parallel := range []int{0, 8} {
		s, _ := openTestStore(t, t.TempDir(), Options{ParallelTableSyncs: parallel})
		if got := s.pebbleOptions.Local.AlwaysSyncDir; got != (parallel > 0) {
			t.Errorf("ParallelTableSyncs %d: AlwaysSyncDir %v", parallel, got)
		}
		closeTestStore(t, s)
	}
}
