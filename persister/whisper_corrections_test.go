package persister

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	whisper "github.com/go-graphite/go-whisper"
)

func TestHistoricalCorrectionIncludesExistingSidecar(t *testing.T) {
	const start = 1699999800 // aligned to 60 seconds
	const now = start + 300
	previousNow := whisper.Now
	whisper.Now = func() time.Time { return time.Unix(now, 0) }
	defer func() { whisper.Now = previousNow }()
	path := filepath.Join(t.TempDir(), "metric.wsp")
	rets := whisper.MustParseRetentionDefs("1s:20s,10s:10m,60s:1h")
	opts := &whisper.Options{Compressed: true, FLock: true, OutOfOrder: true}
	w, err := whisper.CreateWithOptions(path, rets, whisper.Average, 0.25, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	if err := w.UpdateMany([]*whisper.TimeSeriesPoint{{Time: start, Value: 10}, {Time: start + 20, Value: 30}, {Time: start + 180, Value: 1}, {Time: start + 240, Value: 1}}); err != nil {
		t.Fatal(err)
	}
	// A sparse propagated aggregate fills a coarse main-file hole. Its value
	// must participate when an explicit correction recomputes the lower archive.
	side, err := whisper.Create(whisper.OutOfOrderSidecarPath(path), rets, whisper.Average, 0.25)
	if err != nil {
		t.Fatal(err)
	}
	if err := side.ReplaceArchivePoints(1, []whisper.TimeSeriesPoint{{Time: start + 10, Value: 20}}); err != nil {
		t.Fatal(err)
	}
	if err := side.Close(); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	w, err = whisper.OpenWithOptions(path, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	if err := w.UpdateMany([]*whisper.TimeSeriesPoint{{Time: start, Value: 100}}); err != nil {
		t.Fatal(err)
	}
	assertArchiveValue(t, w, 1, start, 100)
	assertArchiveValue(t, w, 1, start+10, 20)
	assertArchiveValue(t, w, 2, start, 50) // (100 + 20 + 30) / 3
	if err := w.MergeOutOfOrder(); err != nil {
		t.Fatal(err)
	}
	assertArchiveValue(t, w, 2, start, 50)
}

func TestHistoricalCorrectionRewriteFailureCanRetry(t *testing.T) {
	const now = 1700000000
	previousNow := whisper.Now
	whisper.Now = func() time.Time { return time.Unix(now, 0) }
	defer func() { whisper.Now = previousNow }()
	path := filepath.Join(t.TempDir(), "metric.wsp")
	opts := &whisper.Options{Compressed: true, FLock: true, OutOfOrder: true}
	w, err := whisper.CreateWithOptions(path, whisper.MustParseRetentionDefs("1s:40s,10s:5m"), whisper.Average, 0.5, opts)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.UpdateMany([]*whisper.TimeSeriesPoint{{Time: now - 60, Value: 10}, {Time: now - 50, Value: 20}}); err != nil {
		t.Fatal(err)
	}
	// A nonempty directory prevents the temporary rewrite file being created.
	blocked := path + ".correct"
	if err := os.Mkdir(blocked, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(blocked, "block"), []byte("block"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := w.UpdateMany([]*whisper.TimeSeriesPoint{{Time: now - 60, Value: 99}}); err == nil {
		t.Fatal("rewrite unexpectedly succeeded")
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.RemoveAll(blocked); err != nil {
		t.Fatal(err)
	}
	w, err = whisper.OpenWithOptions(path, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	assertArchiveValue(t, w, 1, now-60, 10)
	if err := w.UpdateMany([]*whisper.TimeSeriesPoint{{Time: now - 60, Value: 99}}); err != nil {
		t.Fatal(err)
	}
	assertArchiveValue(t, w, 1, now-60, 99)
	assertArchiveValue(t, w, 1, now-50, 20)
}

func assertArchiveValue(t *testing.T, w *whisper.Whisper, archive, timestamp int, want float64) {
	t.Helper()
	values, err := w.ArchivePoints(archive)
	if err != nil {
		t.Fatal(err)
	}
	for _, value := range values {
		if value.Time == timestamp {
			if value.Value != want {
				t.Fatalf("archive %d time %d = %v; want %v", archive, timestamp, value.Value, want)
			}
			return
		}
	}
	t.Fatalf("archive %d missing time %d (want %v)", archive, timestamp, want)
}

func BenchmarkWhisperHistoricalCorrection(b *testing.B) {
	for _, withSidecar := range []bool{false, true} {
		name := "coarse"
		if withSidecar {
			name = "coarse-and-sidecar"
		}
		b.Run(name, func(b *testing.B) {
			path := filepath.Join(b.TempDir(), "metric.wsp")
			opts := &whisper.Options{Compressed: true, FLock: true, OutOfOrder: true}
			w, err := whisper.CreateWithOptions(path, whisper.MustParseRetentionDefs("1s:2h,10s:1d"), whisper.Average, 0.5, opts)
			if err != nil {
				b.Fatal(err)
			}
			now := int(time.Now().Unix())
			if err := w.UpdateMany([]*whisper.TimeSeriesPoint{{Time: now - 10800, Value: 1}, {Time: now - 3600, Value: 1}, {Time: now - 30, Value: 1}}); err != nil {
				b.Fatal(err)
			}
			if err := w.Close(); err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				w, err := whisper.OpenWithOptions(path, opts)
				if err != nil {
					b.Fatal(err)
				}
				input := []*whisper.TimeSeriesPoint{{Time: now - 10800, Value: float64(i)}}
				if withSidecar {
					input = append(input, &whisper.TimeSeriesPoint{Time: now - 1800, Value: float64(i)})
				}
				if err := w.UpdateMany(input); err != nil {
					b.Fatal(err)
				}
				if err := w.Close(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func TestSidecarRetentionBoundarySurvivesRewrite(t *testing.T) {
	const now = 1700000000
	previousNow := whisper.Now
	whisper.Now = func() time.Time { return time.Unix(now, 0) }
	defer func() { whisper.Now = previousNow }()
	for _, correction := range []bool{false, true} {
		name := "compaction"
		if correction {
			name = "coarse-correction"
		}
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "metric.wsp")
			opts := &whisper.Options{Compressed: true, FLock: true, OutOfOrder: true}
			w, err := whisper.CreateWithOptions(path, whisper.MustParseRetentionDefs("1s:40s,10s:5m"), whisper.Average, 0.5, opts)
			if err != nil {
				t.Fatal(err)
			}
			defer w.Close()
			var input []*whisper.TimeSeriesPoint
			for timestamp := now - 39; timestamp < now; timestamp++ {
				input = append(input, &whisper.TimeSeriesPoint{Time: timestamp, Value: 1})
			}
			input = append(input, &whisper.TimeSeriesPoint{Time: now - 100, Value: 10}, &whisper.TimeSeriesPoint{Time: now - 60, Value: 20})
			if err := w.UpdateMany(input); err != nil {
				t.Fatal(err)
			}
			if err := w.UpdateMany([]*whisper.TimeSeriesPoint{{Time: now - 40, Value: 42}}); err != nil {
				t.Fatal(err)
			}
			if w.OutOfOrderPath() == "" {
				t.Fatal("boundary point was not diverted to sidecar")
			}
			assertArchiveValue(t, w, 0, now-40, 42)
			if correction {
				if err := w.UpdateMany([]*whisper.TimeSeriesPoint{{Time: now - 100, Value: 99}}); err != nil {
					t.Fatal(err)
				}
			} else if err := w.MergeOutOfOrder(); err != nil {
				t.Fatal(err)
			}
			assertArchiveValue(t, w, 0, now-40, 42)
			if _, err := os.Stat(whisper.OutOfOrderSidecarPath(path)); !os.IsNotExist(err) {
				t.Fatalf("sidecar was not removed: %v", err)
			}
		})
	}
}
