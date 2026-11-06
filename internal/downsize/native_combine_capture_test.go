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
	originalPoints := len(snap.Archives[0].Points)
	last := snap.Archives[0].Points[originalPoints-1]
	snap.Archives[0].Points = append(snap.Archives[0].Points, store.Point{Timestamp: last.Timestamp + int64(snap.Archives[0].Retention.Step), Value: 91})
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
	out, err := os.Create(filepath.Join(dir, "result.json"))
	if err != nil {
		t.Fatal(err)
	}
	stdout := os.Stdout
	os.Stdout = out
	defer func() { os.Stdout = stdout; out.Close() }()
	if err := BulkEvalMain(args); err != nil {
		t.Fatal(err)
	}
	os.Stdout = stdout
	if err := out.Close(); err != nil {
		t.Fatal(err)
	}
	data, err = os.ReadFile(out.Name())
	if err != nil {
		t.Fatal(err)
	}
	var result struct {
		EqualBits int          `json:"equal_point_bits"`
		Shape     combineShape `json:"time_range_shape"`
	}
	if err := json.Unmarshal(data, &result); err != nil {
		t.Fatal(err)
	}
	if result.EqualBits != originalPoints+1 || len(result.Shape.Archives) != 1 || result.Shape.Archives[0].SurvivorPoints != originalPoints+1 || result.Shape.Archives[0].SurvivorMax != last.Timestamp+int64(snap.Archives[0].Retention.Step) {
		t.Fatalf("survivor image was replaced by donor history: %+v", result)
	}
	capture.TargetSHA256[name] = strings.Repeat("0", 64)
	save()
	if err := BulkEvalMain(args); err == nil || !strings.Contains(err.Error(), "survivor fixture integrity mismatch") {
		t.Fatal("unverified survivor fixture accepted", err)
	}
}
