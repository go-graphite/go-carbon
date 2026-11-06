package downsize

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/cockroachdb/pebble/vfs"
	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/internal/whisperio"
)

func TestNativeCombineCaptureWithRealSurvivorArchives(t *testing.T) {
	ctx := context.Background()
	source, err := store.Open("/db", store.Options{ExactBlocks: true, TimeBuckets: true, Continuation: &whisperio.ContinuationCodec{}, CacheSize: 1 << 20, MemTableSize: 1 << 20}.WithFS(vfs.NewMem()))
	if err != nil {
		t.Fatal(err)
	}
	defer source.Close()
	name := "capture.combine"
	addPoints(t, source, name, 1)
	server := httptest.NewServer(nativeControllerService(t, source))
	defer server.Close()
	dir := t.TempDir()
	names := filepath.Join(dir, "names.json")
	capturePath := filepath.Join(dir, "capture.json")
	if err := os.WriteFile(names, []byte(`["capture.combine"]`), 0600); err != nil {
		t.Fatal(err)
	}
	if err := NativeCaptureMain(ctx, []string{"--source", server.URL, "--names", names, "--out", capturePath}); err != nil {
		t.Fatal(err)
	}
	snap, err := source.Snapshot(ctx, name)
	if err != nil {
		t.Fatal(err)
	}
	if len(snap.Archives[0].Points) == 0 {
		t.Fatal("empty fixture")
	}
	snap.Archives[0].Points[0].Value = 42
	target := filepath.Join(dir, "target.wsp")
	if err := whisperio.New(nil).ExportSnapshot(ctx, snap, target); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(capturePath)
	if err != nil {
		t.Fatal(err)
	}
	var capture nativeCapture
	if err := json.Unmarshal(data, &capture); err != nil {
		t.Fatal(err)
	}
	data, err = os.ReadFile(target)
	if err != nil {
		t.Fatal(err)
	}
	hash := sha256.Sum256(data)
	capture.Targets = map[string]string{name: "target.wsp"}
	capture.TargetSHA256 = map[string]string{name: hex.EncodeToString(hash[:])}
	save := func() {
		data, err := json.Marshal(capture)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(capturePath, data, 0600); err != nil {
			t.Fatal(err)
		}
	}
	save()
	args := []string{"--native-capture", capturePath, "--native", "--combine-existing", "--memory", "--scratch", dir}
	if err := BulkEvalMain(args); err != nil {
		t.Fatal(err)
	}
	capture.TargetSHA256[name] = strings.Repeat("0", 64)
	save()
	if err := BulkEvalMain(args); err == nil || !strings.Contains(err.Error(), "survivor fixture integrity mismatch") {
		t.Fatal("unverified survivor fixture accepted", err)
	}
}
