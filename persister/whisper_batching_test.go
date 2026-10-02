package persister

import (
	"math"
	"path/filepath"
	"regexp"
	"strconv"
	"testing"
	"time"

	whisper "github.com/go-graphite/go-whisper"

	"github.com/go-graphite/go-carbon/points"
)

// TestStoreBatchingReplay keeps a differential oracle around the cache batching
// boundary. It includes ordered samples, late fills, duplicate corrections, and
// values on both sides of the raw-retention boundary.
func TestStoreBatchingReplay(t *testing.T) {
	const now = 1_700_000_000
	freezeWhisperNow(t, now)
	base := now - 15
	base -= base % 10

	sequence := []*points.Points{
		batchPoints("batch.replay", base-40, 10, base-30, 20, base-20, 30, base-10, 40),
		batchPoints("batch.replay", base-8, 50, base-6, 60, base-4, 70, base-2, 80),
		// These are deliberately later than the initial writes.
		batchPoints("batch.replay", base-30, 21, base-29, 21, base-8, 51, base-7, 52),
		batchPoints("batch.replay", base-3, 71, base-1, 81, base-50, 5, base-49, 6),
	}

	baseline := runBatchingReplay(t, sequence, 4, now)
	batched := runBatchingReplay(t, sequence, 8, now)
	for _, result := range []replayResult{baseline, batched} {
		assertReplayValues(t, result.beforeMerge, base)
		assertReplayValues(t, result.afterMerge, base)
	}
	assertReplayEqual(t, "small versus combined before compaction", baseline.beforeMerge, batched.beforeMerge)
	assertReplayEqual(t, "small versus combined after compaction", baseline.afterMerge, batched.afterMerge)
	assertReplayEqual(t, "small compaction preserves reads", baseline.beforeMerge, baseline.afterMerge)
	assertReplayEqual(t, "combined compaction preserves reads", batched.beforeMerge, batched.afterMerge)
}

// These smaller cases make the two observed failure modes explicit. They
// prevent a future batching implementation from treating the larger replay as
// a timing artifact or as an expected coarse-archive sidecar limitation.
func TestStoreBatchingReplayMinimalRegressions(t *testing.T) {
	const now = 1_700_000_000
	freezeWhisperNow(t, now)
	base := now - 15
	base -= base % 10

	tests := []struct {
		name     string
		sequence []*points.Points
	}{
		{
			name: "late coarse correction",
			sequence: []*points.Points{
				batchPoints("batch.replay", base-40, 10, base-30, 20, base-20, 30, base-10, 40),
				batchPoints("batch.replay", base-30, 21),
			},
		},
		{
			name: "raw values mixed with expired points",
			sequence: []*points.Points{
				batchPoints("batch.replay", base-8, 50, base-6, 60, base-4, 70, base-2, 80),
				batchPoints("batch.replay", base-3, 71, base-1, 81, base-50, 5, base-49, 6),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			small := runBatchingReplay(t, tt.sequence, 4, now)
			combined := runBatchingReplay(t, tt.sequence, 8, now)
			assertReplayEqual(t, "small versus combined before compaction", small.beforeMerge, combined.beforeMerge)
			assertReplayEqual(t, "small versus combined after compaction", small.afterMerge, combined.afterMerge)
		})
	}
}

func TestRetentionCutoffKeepsBoundary(t *testing.T) {
	const now = 1_700_000_000
	freezeWhisperNow(t, now)

	retentions := whisper.MustParseRetentionDefs("1s:10s,10s:1m")
	for _, tt := range []struct {
		name       string
		compressed bool
	}{
		{name: "classic"},
		{name: "compressed", compressed: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "retention.wsp")
			w, err := whisper.CreateWithOptions(path, retentions, whisper.Sum, 0, &whisper.Options{Compressed: tt.compressed})
			if err != nil {
				t.Fatalf("create: %v", err)
			}
			if err := w.UpdateMany([]*whisper.TimeSeriesPoint{
				{Time: now - 10, Value: 1}, // exact raw cutoff
				{Time: now - 11, Value: 2}, // one second older
			}); err != nil {
				_ = w.Close()
				t.Fatalf("update: %v", err)
			}
			if err := w.Close(); err != nil {
				t.Fatalf("close: %v", err)
			}

			w, err = whisper.OpenWithOptions(path, &whisper.Options{Compressed: tt.compressed})
			if err != nil {
				t.Fatalf("reopen: %v", err)
			}
			defer w.Close()
			raw, err := w.ArchivePoints(0)
			if err != nil {
				t.Fatalf("read raw archive: %v", err)
			}
			if len(raw) != 1 || raw[0].Time != now-10 || raw[0].Value != 1 {
				t.Fatalf("raw archive = %+v; want only cutoff point", raw)
			}
			coarse, err := w.ArchivePoints(1)
			if err != nil {
				t.Fatalf("read coarse archive: %v", err)
			}
			if !containsWhisperPoint(coarse, now-20, 2) {
				t.Fatalf("coarse archive = %+v; want retained older point", coarse)
			}
		})
	}
}

func TestThreeArchiveDirectCorrectionRecomputesLowerArchive(t *testing.T) {
	const now = 1_700_000_000
	freezeWhisperNow(t, now)
	base := now - 180
	base -= base % 60
	path := filepath.Join(t.TempDir(), "three-archive.wsp")
	w, err := whisper.CreateWithOptions(path, whisper.MustParseRetentionDefs("1s:40s,10s:5m,60s:1h"), whisper.Sum, 0.5, &whisper.Options{Compressed: true, OutOfOrder: true})
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	var initial []*whisper.TimeSeriesPoint
	for window := 0; window < 3; window++ {
		for slot := 0; slot < 6; slot++ {
			initial = append(initial, &whisper.TimeSeriesPoint{Time: base + window*60 + slot*10, Value: float64(slot + 1)})
		}
	}
	if err := w.UpdateMany(initial); err != nil {
		_ = w.Close()
		t.Fatalf("initial write: %v", err)
	}
	if err := w.UpdateMany([]*whisper.TimeSeriesPoint{{Time: base + 20, Value: 100}}); err != nil {
		_ = w.Close()
		t.Fatalf("historical correction: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	w, err = whisper.OpenWithOptions(path, &whisper.Options{Compressed: true, OutOfOrder: true})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer w.Close()
	if got := archivePointValue(t, w, 1, base+20); got != 100 {
		t.Fatalf("direct 10-second correction = %v; want 100", got)
	}
	if got := archivePointValue(t, w, 2, base); got != 118 {
		t.Fatalf("recomputed 60-second sum = %v; want 118", got)
	}
}

type replayResult struct {
	beforeMerge replaySnapshot
	afterMerge  replaySnapshot
}

type replaySnapshot struct {
	raw    seriesSnapshot
	coarse seriesSnapshot
}

type seriesSnapshot struct {
	from   int
	step   int
	values []float64
}

func freezeWhisperNow(t *testing.T, now int) {
	t.Helper()
	previousNow := whisper.Now
	whisper.Now = func() time.Time { return time.Unix(int64(now), 0) }
	t.Cleanup(func() { whisper.Now = previousNow })
}

func runBatchingReplay(t *testing.T, sequence []*points.Points, batchSize, now int) replayResult {
	t.Helper()

	dir := t.TempDir()
	cache := &fakeCache{}
	retentionStr := "1s:40s,10s:5m"
	retentions, err := ParseRetentionDefs(retentionStr)
	if err != nil {
		t.Fatalf("parse retention definitions: %v", err)
	}
	compressed := true
	p := NewWhisper(dir, WhisperSchemas{{
		Name:         "batch-replay",
		Pattern:      regexp.MustCompile(".*"),
		RetentionStr: retentionStr,
		Retentions:   retentions,
		Priority:     1,
		Compressed:   &compressed,
	}}, NewWhisperAggregation(), nil, cache.pop, cache.confirm, cache.pop)
	p.SetRequeue(cache.requeue)
	p.SetCompressed(true)
	p.SetFLock(true)
	p.EnableOutOfOrder(0, 1<<30)

	metric := "batch.replay"
	var pending []points.Point
	for _, input := range sequence {
		pending = append(pending, input.Data...)
		for len(pending) >= batchSize {
			batch := append([]points.Point(nil), pending[:batchSize]...)
			pending = pending[batchSize:]
			cache.data = map[string]*points.Points{metric: {Metric: metric, Data: batch}}
			p.store(metric)
		}
	}
	if len(pending) > 0 {
		cache.data = map[string]*points.Points{metric: {Metric: metric, Data: append([]points.Point(nil), pending...)}}
		p.store(metric)
	}

	path := filepath.Join(dir, "batch", "replay.wsp")
	before := readReplaySnapshot(t, path, now)
	w, err := whisper.OpenWithOptions(path, &whisper.Options{Compressed: true, FLock: true, OutOfOrder: true})
	if err != nil {
		t.Fatalf("open before merge: %v", err)
	}
	if err := w.MergeOutOfOrder(); err != nil {
		_ = w.Close()
		t.Fatalf("merge out-of-order: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close after merge: %v", err)
	}
	after := readReplaySnapshot(t, path, now)

	return replayResult{beforeMerge: before, afterMerge: after}
}

func batchPoints(metric string, values ...int) *points.Points {
	p := &points.Points{Metric: metric}
	for i := 0; i < len(values); i += 2 {
		p.Data = append(p.Data, points.Point{Timestamp: int64(values[i]), Value: float64(values[i+1])})
	}
	return p
}

func readReplaySnapshot(t *testing.T, path string, now int) replaySnapshot {
	t.Helper()
	w, err := whisper.OpenWithOptions(path, &whisper.Options{Compressed: true, FLock: true, OutOfOrder: true})
	if err != nil {
		t.Fatalf("open for snapshot: %v", err)
	}
	defer w.Close()

	return replaySnapshot{
		raw:    fetchReplaySeries(t, w, now-35, now),
		coarse: fetchReplaySeries(t, w, now-180, now-45),
	}
}

func fetchReplaySeries(t *testing.T, w *whisper.Whisper, from, until int) seriesSnapshot {
	t.Helper()
	ts, err := w.Fetch(from, until)
	if err != nil {
		t.Fatalf("fetch %d..%d: %v", from, until, err)
	}
	if ts == nil {
		t.Fatalf("fetch %d..%d returned no series", from, until)
	}
	return seriesSnapshot{from: ts.FromTime(), step: ts.Step(), values: append([]float64(nil), ts.Values()...)}
}

func assertReplayEqual(t *testing.T, name string, want, got replaySnapshot) {
	t.Helper()
	assertSeriesEqual(t, name+" raw", want.raw, got.raw)
	assertSeriesEqual(t, name+" coarse", want.coarse, got.coarse)
}

func assertReplayValues(t *testing.T, snapshot replaySnapshot, base int) {
	t.Helper()
	for _, tt := range []struct {
		timestamp int
		value     float64
	}{
		{base - 10, 40},
		{base - 8, 51},
		{base - 7, 52},
		{base - 6, 60},
		{base - 4, 70},
		{base - 3, 71},
		{base - 2, 80},
		{base - 1, 81},
	} {
		if got := snapshotValue(snapshot.raw, tt.timestamp); got != tt.value {
			t.Errorf("raw value at %d = %v; want %v", tt.timestamp, got, tt.value)
		}
	}
	if got := snapshotValue(snapshot.coarse, base-30); got != 21 {
		t.Errorf("coarse correction at %d = %v; want 21", base-30, got)
	}
}

func snapshotValue(series seriesSnapshot, timestamp int) float64 {
	index := (timestamp - series.from) / series.step
	if index < 0 || index >= len(series.values) || series.from+index*series.step != timestamp {
		return math.NaN()
	}
	return series.values[index]
}

func archivePointValue(t *testing.T, w *whisper.Whisper, archive, timestamp int) float64 {
	t.Helper()
	points, err := w.ArchivePoints(archive)
	if err != nil {
		t.Fatalf("read archive %d: %v", archive, err)
	}
	for _, point := range points {
		if point.Time == timestamp {
			return point.Value
		}
	}
	return math.NaN()
}

func containsWhisperPoint(points []whisper.TimeSeriesPoint, timestamp int, value float64) bool {
	for _, point := range points {
		if point.Time == timestamp && point.Value == value {
			return true
		}
	}
	return false
}

func assertSeriesEqual(t *testing.T, name string, want, got seriesSnapshot) {
	t.Helper()
	if want.from != got.from || want.step != got.step || len(want.values) != len(got.values) {
		t.Errorf("%s shape = from=%d step=%d len=%d; want from=%d step=%d len=%d", name, got.from, got.step, len(got.values), want.from, want.step, len(want.values))
		return
	}
	for i := range want.values {
		if (math.IsNaN(want.values[i]) && math.IsNaN(got.values[i])) || want.values[i] == got.values[i] {
			continue
		}
		t.Errorf("%s point at %d = %v; want %v", name, want.from+want.step*i, got.values[i], want.values[i])
		return
	}
}

func BenchmarkWhisperBatchWrite(b *testing.B) {
	for _, pointCount := range []int{3, 4, 8} {
		b.Run("points="+strconv.Itoa(pointCount), func(b *testing.B) {
			dir := b.TempDir()
			path := filepath.Join(dir, "batch.wsp")
			retentions := whisper.MustParseRetentionDefs("1s:2h,10s:1d")
			db, err := whisper.CreateWithOptions(path, retentions, whisper.Average, 0.5, &whisper.Options{Compressed: true, FLock: true, OutOfOrder: true})
			if err != nil {
				b.Fatalf("create whisper: %v", err)
			}
			if err := db.Close(); err != nil {
				b.Fatalf("close created whisper: %v", err)
			}

			now := int(time.Now().Unix())
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				points := make([]*whisper.TimeSeriesPoint, pointCount)
				for j := range points {
					points[j] = &whisper.TimeSeriesPoint{Time: now - 30 + j, Value: float64(i*pointCount + j)}
				}
				w, err := whisper.OpenWithOptions(path, &whisper.Options{Compressed: true, FLock: true, OutOfOrder: true})
				if err != nil {
					b.Fatal(err)
				}
				if err := w.UpdateMany(points); err != nil {
					_ = w.Close()
					b.Fatal(err)
				}
				if err := w.Close(); err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*pointCount), "ns/point")
			b.ReportMetric(float64(b.N*pointCount)/b.Elapsed().Seconds(), "points/s")
		})
	}
}
