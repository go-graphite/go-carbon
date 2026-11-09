package downsize

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/cockroachdb/pebble/vfs"
	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/internal/whisperio"
)

func TestNativeCaptureAndDifferentialEvaluator(t *testing.T) {
	fs := vfs.NewMem()
	source, err := store.Open("/db", store.Options{ExactBlocks: true, TimeBuckets: true, Continuation: &whisperio.ContinuationCodec{}, CacheSize: 1 << 20, MemTableSize: 1 << 20}.WithFS(fs))
	if err != nil {
		t.Fatal(err)
	}
	defer source.Close()
	names := []string{"capture.alpha", "capture.beta", "capture.gamma"}
	for i, name := range names {
		addPoints(t, source, name, float64(i+1))
	}
	server := httptest.NewServer(nativeControllerService(t, source))
	defer server.Close()
	dir := t.TempDir()
	namesFile := filepath.Join(dir, "names.json")
	out := filepath.Join(dir, "native.json")
	data, _ := json.Marshal(names)
	if err := os.WriteFile(namesFile, data, 0600); err != nil {
		t.Fatal(err)
	}
	if err := NativeCaptureMain(context.Background(), []string{"--source", server.URL, "--names", namesFile, "--out", out}); err != nil {
		t.Fatal(err)
	}
	if err := BulkEvalMain([]string{"--native-capture", out, "--scratch", dir, "--native", "--memory", "--sample-names", "2", "--batch", "2", "--workers", "2", "--overlap-percent", "34"}); err != nil {
		t.Fatal(err)
	}
}
