package whisperio

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
)

func TestStopBackgroundThenRunColdPack(t *testing.T) {
	db, err := store.Open(filepath.Join(t.TempDir(), "store"), coldOptions(&ContinuationCodec{}))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.StopBackground()
	want := coldFixture(t, db)
	ctx := context.Background()
	for _, mode := range []string{"exact", "unpack"} {
		if err := db.ConfigureColdPack(store.ColdPackOptions{Mode: mode, WriteWindow: time.Second, CPUFraction: 1, BytesPerSecond: 1 << 40}); err != nil {
			t.Fatal(err)
		}
		if err := db.RunColdPack(ctx); err != nil {
			t.Fatalf("%s after StopBackground: %v", mode, err)
		}
		if stats := db.ColdPackStats(); stats.Blocks == 0 || stats.Errors != 0 || stats.Skipped != 0 || stats.Conflicts != 0 {
			t.Fatalf("%s did not rewrite blocks cleanly: %+v", mode, stats)
		}
		got, err := db.Snapshot(ctx, want.Metadata.Name)
		if err != nil {
			t.Fatal(err)
		}
		coldEqual(t, want, got, false)
	}
	if err := db.DowngradeColdPack(ctx); err != nil {
		t.Fatal(err)
	}
	if db.TransferFormat() != 7 {
		t.Fatal("unpack did not restore v7")
	}
}
