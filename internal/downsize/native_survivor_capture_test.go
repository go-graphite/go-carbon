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

func TestNativeCombineCaptureWithNativeSurvivor(t *testing.T) {
	ctx := context.Background()
	open := func() *store.Store {
		db, err := store.Open("/db", store.Options{ExactBlocks: true, TimeBuckets: true, Continuation: &whisperio.ContinuationCodec{}, CacheSize: 1 << 20, MemTableSize: 1 << 20}.WithFS(vfs.NewMem()))
		if err != nil {
			t.Fatal(err)
		}
		db.StopBackground()
		t.Cleanup(func() {
			if err := db.Close(); err != nil {
				t.Error(err)
			}
		})
		return db
	}
	donor, survivor := open(), open()
	name := "capture.native.pair"
	addPoints(t, donor, name, 1)
	records, err := donor.ExportTransfer(ctx, []string{name})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := survivor.ImportTransfer(ctx, records); err != nil {
		t.Fatal(err)
	}
	if err := survivor.UpdateMany(ctx, name, []store.Point{{Timestamp: testMetricNow - 10, Value: 42}, {Timestamp: testMetricNow, Value: 91}}); err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	namesPath := filepath.Join(dir, "names.json")
	if err := os.WriteFile(namesPath, []byte(`["capture.native.pair"]`), 0600); err != nil {
		t.Fatal(err)
	}
	captures := make([]nativeCapture, 2)
	for i, db := range []*store.Store{donor, survivor} {
		server := httptest.NewServer(nativeControllerService(t, db))
		defer server.Close()
		path := filepath.Join(dir, []string{"donor.json", "survivor.json"}[i])
		if err := NativeCaptureMain(ctx, []string{"--source", server.URL, "--names", namesPath, "--out", path}); err != nil {
			t.Fatal(err)
		}
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal(data, &captures[i]); err != nil {
			t.Fatal(err)
		}
	}
	captures[0].TargetNative = &captures[1]
	wirePath := filepath.Join(dir, captures[0].WireFile)
	wire, err := os.ReadFile(wirePath)
	if err != nil {
		t.Fatal(err)
	}
	wire = append([]byte(evalNativePairMagic), wire...)
	if err := os.WriteFile(wirePath, wire, 0600); err != nil {
		t.Fatal(err)
	}
	hash := sha256.Sum256(wire)
	captures[0].SHA256 = hex.EncodeToString(hash[:])
	captures[0].Bytes = int64(len(wire))
	path := filepath.Join(dir, "pair.json")
	save := func() {
		data, err := json.Marshal(captures[0])
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, data, 0600); err != nil {
			t.Fatal(err)
		}
	}
	save()
	out, err := os.Create(filepath.Join(dir, "result.json"))
	if err != nil {
		t.Fatal(err)
	}
	stdout := os.Stdout
	os.Stdout = out
	defer func() { os.Stdout = stdout; out.Close() }()
	args := []string{"--native-capture", path, "--native", "--combine-existing", "--memory", "--scratch", dir}
	if err := BulkEvalMain(args); err != nil {
		t.Fatal(err)
	}
	os.Stdout = stdout
	if err := out.Close(); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(out.Name())
	if err != nil {
		t.Fatal(err)
	}
	var result struct {
		Native        bool         `json:"native_survivor_capture"`
		SourceLayouts int          `json:"original_native_layouts_verified"`
		TargetLayouts int          `json:"native_survivor_layouts_verified"`
		Bits          int          `json:"equal_point_bits"`
		Shape         combineShape `json:"time_range_shape"`
	}
	if err := json.Unmarshal(data, &result); err != nil {
		t.Fatal(err)
	}
	if !result.Native || result.SourceLayouts != 1 || result.TargetLayouts != 1 || result.Bits != 3 || result.Shape.Archives[0].SurvivorPoints != 3 {
		t.Fatalf("native survivor was not preserved: %+v", result)
	}
	captures[0].TargetNative = nil
	save()
	if err := BulkEvalMain(args); err == nil {
		t.Fatal("paired input silently downgraded to an absent survivor")
	}
	captures[0].TargetNative = &captures[1]
	captures[1].SHA256 = strings.Repeat("0", 64)
	save()
	if err := BulkEvalMain(args); err == nil || !strings.Contains(err.Error(), "native survivor capture") {
		t.Fatal("unverified native survivor accepted", err)
	}
}
