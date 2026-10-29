package whisperio

import (
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	whisper "github.com/go-graphite/go-whisper"
)

// mapContinuationIO is an in-memory store.ContinuationIO.
type mapContinuationIO map[string][]byte

func (m mapContinuationIO) Get(k []byte) ([]byte, error) {
	v, ok := m[string(k)]
	if !ok {
		return nil, store.ErrNotFound
	}
	return append([]byte(nil), v...), nil
}
func (m mapContinuationIO) Set(k, v []byte) error {
	m[string(k)] = append([]byte(nil), v...)
	return nil
}
func (m mapContinuationIO) Delete(k []byte) error { delete(m, string(k)); return nil }
func (m mapContinuationIO) DeletePrefix(p []byte) error {
	for k := range m {
		if strings.HasPrefix(k, string(p)) {
			delete(m, k)
		}
	}
	return nil
}
func (m mapContinuationIO) Visit(p []byte, fn func([]byte, []byte) error) error {
	var keys []string
	for k := range m {
		if strings.HasPrefix(k, string(p)) {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	for _, k := range keys {
		if err := fn([]byte(k), m[k]); err != nil {
			return err
		}
	}
	return nil
}

// Production CWhisper files hold blocks ending in corrupt, unaligned far-future
// points. CWhisper reads and appends to them; the continuation must store them
// exactly instead of rejecting the conversion.
func TestContinuationKeepsUnalignedSourcePoints(t *testing.T) {
	now := time.Unix(1792432800, 0)
	path := filepath.Join(t.TempDir(), "m.wsp")
	w, err := whisper.CreateWithOptions(path, whisper.MustParseRetentionDefs("10m:2d,1h:9d"), whisper.Average, 0,
		&whisper.Options{Compressed: true, Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	var seed []*whisper.TimeSeriesPoint
	for i := 1; i <= 20; i++ {
		seed = append(seed, &whisper.TimeSeriesPoint{Time: int(now.Unix()) - i*600, Value: float64(i)})
	}
	if err := w.UpdateMany(seed); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	source, err := whisper.OpenWithOptions(path, &whisper.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer source.Close()
	physical, err := source.ContinuationLayout()
	if err != nil {
		t.Fatal(err)
	}
	var block whisper.ContinuationBlock
	var points []whisper.TimeSeriesPoint
	for _, b := range physical.Blocks {
		if p, err := source.ContinuationBlockPointsBuffered(b, &whisper.ContinuationBlockBuffer{}); err == nil && len(p) > 0 && b.Archive == 1 {
			block, points = b, append([]whisper.TimeSeriesPoint(nil), p...)
		}
	}
	if len(points) == 0 {
		t.Fatal("no populated archive-1 block")
	}
	corrupt := whisper.TimeSeriesPoint{Time: 0xF0000000, Value: 3.5}
	if corrupt.Time%block.Step == 0 {
		t.Fatal("fixture point must be unaligned")
	}
	points = append(points, corrupt)
	block.Count, block.Last = len(points), corrupt.Time
	fs := newContinuationFiles(mapContinuationIO{}, false)
	out, err := fs.OpenFile(fs.main(), os.O_CREATE|os.O_EXCL|os.O_RDWR, 0600)
	if err != nil {
		t.Fatal(err)
	}
	f := out.(*continuationFile)
	if err := f.ConfigureStorage(source.StorageLayout()); err != nil {
		t.Fatal(err)
	}
	if err := f.AppendStorageBlock(block, 0, points); err != nil {
		t.Fatalf("unaligned source point rejected: %v", err)
	}
	got, err := f.ReadStorageBlock(block)
	if err != nil {
		t.Fatalf("stored unaligned point unreadable: %v", err)
	}
	if err := sameContinuationPoints(points, got); err != nil {
		t.Fatal(err)
	}
	// Ordering is still enforced.
	bad := append(append([]whisper.TimeSeriesPoint(nil), points...), whisper.TimeSeriesPoint{Time: points[0].Time, Value: 1})
	block.Count, block.Last = len(bad), bad[len(bad)-1].Time
	if err := f.AppendStorageBlock(block, 0, bad); err == nil {
		t.Fatal("non-increasing point accepted")
	}
}

// TestUnalignedStoredPointReadsWholeBlock covers the test above for raw
// reads: a block holding an unaligned source point reads point by point
// (ReadStorageBlock, which the in-place paths use) and as raw bytes (ReadAt,
// which go-whisper's rewrites and replays use, e.g. buildRewrite's
// "archives[i].blocks[j].file.read"), and those bytes decode to exactly the
// stored points. Re-encoding such a block used to require every point on the
// archive's grid, so a merge or replay of the metric failed with "merge
// out-of-order points: archives[0].blocks[0].file.read: continuation points
// must increase on their archive grid" (patches/go-whisper-off-grid-blocks.patch).
func TestUnalignedStoredPointReadsWholeBlock(t *testing.T) {
	now := time.Unix(1792432800, 0)
	path := filepath.Join(t.TempDir(), "m.wsp")
	w, err := whisper.CreateWithOptions(path, whisper.MustParseRetentionDefs("10m:2d,1h:9d"), whisper.Average, 0,
		&whisper.Options{Compressed: true, Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	var seed []*whisper.TimeSeriesPoint
	for i := 1; i <= 20; i++ {
		seed = append(seed, &whisper.TimeSeriesPoint{Time: int(now.Unix()) - i*600, Value: float64(i)})
	}
	if err := w.UpdateMany(seed); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	source, err := whisper.OpenWithOptions(path, &whisper.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer source.Close()
	physical, err := source.ContinuationLayout()
	if err != nil {
		t.Fatal(err)
	}
	var block whisper.ContinuationBlock
	var points []whisper.TimeSeriesPoint
	for _, b := range physical.Blocks {
		if p, err := source.ContinuationBlockPointsBuffered(b, &whisper.ContinuationBlockBuffer{}); err == nil && len(p) > 0 && b.Archive == 0 {
			block, points = b, append([]whisper.TimeSeriesPoint(nil), p...)
			break
		}
	}
	if len(points) == 0 {
		t.Fatal("no populated archive-0 block")
	}
	points = append(points, whisper.TimeSeriesPoint{Time: 0xF0000000 + 7, Value: 3.5})
	block.Count, block.Last = len(points), points[len(points)-1].Time
	fs := newContinuationFiles(mapContinuationIO{}, false)
	out, err := fs.OpenFile(fs.main(), os.O_CREATE|os.O_EXCL|os.O_RDWR, 0600)
	if err != nil {
		t.Fatal(err)
	}
	f := out.(*continuationFile)
	if err := f.ConfigureStorage(source.StorageLayout()); err != nil {
		t.Fatal(err)
	}
	if err := f.AppendStorageBlock(block, 0, points); err != nil {
		t.Fatal(err)
	}
	if _, err := f.ReadStorageBlock(block); err != nil {
		t.Fatalf("point read: %v", err)
	}
	raw := make([]byte, block.Size)
	if _, err := f.ReadAt(raw, int64(block.Offset)); err != nil {
		t.Fatalf("whole-block read of an unaligned point: %v", err)
	}
	got, err := whisper.DecodeContinuationBlock(block, raw)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != len(points) {
		t.Fatalf("whole-block read decodes %d points, want %d", len(got), len(points))
	}
	for i := range got {
		if got[i].Time != points[i].Time || math.Float64bits(got[i].Value) != math.Float64bits(points[i].Value) {
			t.Fatalf("whole-block read point %d: %+v, want %+v", i, got[i], points[i])
		}
	}
}
