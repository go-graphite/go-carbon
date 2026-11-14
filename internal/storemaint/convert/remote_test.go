package convert

import (
	"bytes"
	"context"
	"encoding/binary"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/buckyd"
	"github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/internal/whisperio"
	whisper "github.com/go-graphite/go-whisper"
)

func TestRemoteConversionPreservesArchivesAndTargetWins(t *testing.T) {
	root := t.TempDir()
	source := filepath.Join(root, "source")
	if err := os.Mkdir(source, 0700); err != nil {
		t.Fatal(err)
	}
	now := int(time.Now().Unix() / 60 * 60)
	path := filepath.Join(source, "metric.wsp")
	w, err := whisper.CreateWithOptions(path, whisper.MustParseRetentionDefs("60s:1d,300s:7d"), whisper.Average, 0, &whisper.Options{Compressed: true, PointsPerBlock: 4})
	if err != nil {
		t.Fatal(err)
	}
	input := make([]*whisper.TimeSeriesPoint, 100)
	for i := range input {
		input[i] = &whisper.TimeSeriesPoint{Time: now - (100-i)*60, Value: 1}
	}
	input[len(input)-1].Value = 2
	if err := w.UpdateMany(input); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	// Corrupt only a physical block's count hint: ordinary archive reads
	// remain valid, but strict continuation conversion must reject the block.
	readOnly := os.O_RDONLY
	original, err := whisper.OpenWithOptions(path, &whisper.Options{OpenFileFlag: &readOnly})
	if err != nil {
		t.Fatal(err)
	}
	layout, err := original.ContinuationLayout()
	if err != nil {
		t.Fatal(err)
	}
	original.Close()
	block := layout.Blocks[0]
	descriptor := make([]byte, 12)
	binary.BigEndian.PutUint32(descriptor, uint32(block.First))
	binary.BigEndian.PutUint32(descriptor[4:], uint32(block.Last))
	binary.BigEndian.PutUint32(descriptor[8:], uint32(block.Count-1))
	offset := bytes.Index(layout.Header, descriptor)
	if offset < 0 {
		t.Fatal("block descriptor missing")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	binary.BigEndian.PutUint32(data[offset+8:], uint32(block.Count+6))
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	original, err = whisper.OpenWithOptions(path, &whisper.Options{OpenFileFlag: &readOnly})
	if err != nil {
		t.Fatal(err)
	}
	damaged, err := original.ContinuationLayout()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := original.ContinuationBlockPoints(damaged.Blocks[0]); err == nil {
		t.Fatal("damaged physical continuation accepted")
	}
	original.Close()
	ctx := context.Background()
	expected, err := whisperio.ReadSnapshotIn(ctx, "metric", path, root)
	if err != nil {
		t.Fatal(err)
	}
	targetSource := filepath.Join(root, "target-source")
	if err := os.Mkdir(targetSource, 0700); err != nil {
		t.Fatal(err)
	}
	db, err := chunkstore.Open(filepath.Join(root, "target"), chunkstore.Options{ExactBlocks: true, Continuation: &whisperio.ContinuationCodec{}, IncrementalSource: targetSource, CacheSize: 1 << 20, MemTableSize: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Create(ctx, expected.Metadata.MetricConfig); err != nil {
		t.Fatal(err)
	}
	if err := db.UpdateMany(ctx, "metric", []chunkstore.Point{{Timestamp: int64(now - 60), Value: 99}, {Timestamp: int64(now), Value: 100}}); err != nil {
		t.Fatal(err)
	}
	secret := filepath.Join(root, "secret")
	if err := os.WriteFile(secret, []byte("test-secret"), 0600); err != nil {
		t.Fatal(err)
	}
	service, err := buckyd.New(buckyd.Config{JWTSecretFile: secret, TmpDir: root, MaxTransfers: 4, MaxBodyBytes: 64 << 20}, db)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(service.Handler())
	defer server.Close()
	scratch := filepath.Join(root, "scratch")
	if err := os.Mkdir(scratch, 0700); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if err := convertRemote(ctx, source, server.URL, secret, scratch); err != nil {
			t.Fatal(err)
		}
	}
	got, err := db.Snapshot(ctx, "metric")
	if err != nil {
		t.Fatal(err)
	}
	values := map[int64]float64{}
	for _, p := range got.Archives[0].Points {
		values[p.Timestamp] = p.Value
	}
	if values[int64(now-120)] != 1 || values[int64(now-60)] != 99 || values[int64(now)] != 100 {
		t.Fatalf("points: %v", values)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("original removed: %v", err)
	}
}
