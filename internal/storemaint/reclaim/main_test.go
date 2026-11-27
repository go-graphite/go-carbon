package reclaim

import (
	"context"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/internal/whisperio"
)

// The unreferenced-ID mode opens the store the way the command does: read-only
// for the dry run, writable with -apply, and it refuses a fence too close to
// the allocator watermark.
func TestUnreferencedIDsCommand(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "store")
	db, err := store.Open(dir, store.Options{Continuation: &whisperio.ContinuationCodec{}, ExactBlocks: true, TimeBuckets: true, SyncInterval: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"a.b", "a.c", "a.d"} {
		if _, err := db.Create(context.Background(), store.MetricConfig{Name: name, Retentions: []store.Retention{{Step: 60, Count: 60}}, AggregationMethod: store.Average}); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if err := Main([]string{"-store", dir, "-unreferenced-ids"}); err == nil || !strings.Contains(err.Error(), "-below-id") {
		t.Fatalf("missing fence: %v", err)
	}
	if err := Main([]string{"-store", dir, "-unreferenced-ids", "-below-id", "3"}); err == nil || !strings.Contains(err.Error(), "watermark") {
		t.Fatalf("default margin must refuse a fence near the watermark: %v", err)
	}
	for _, apply := range []bool{false, true} {
		args := []string{"-store", dir, "-unreferenced-ids", "-below-id", "3", "-margin-ids", "0", "-apply=" + strconv.FormatBool(apply)}
		if err := Main(args); err != nil {
			t.Fatalf("apply=%v: %v", apply, err)
		}
	}
}
