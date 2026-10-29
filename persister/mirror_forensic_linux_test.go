package persister

import (
	"context"
	"encoding/json"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/internal/whisperio"
	whisper "github.com/go-graphite/go-whisper"
)

func TestNativeMirrorFailureDiff(t *testing.T) {
	base := os.Getenv("GRAPHITE_MIRROR_FORENSIC")
	if base == "" {
		t.Skip("requires retained private failure")
	}
	if !strings.HasSuffix(filepath.Clean(base), "/production-replay-lab-v1") {
		t.Fatal("private case required")
	}
	var corpus struct{ Rows []fullMirrorRow }
	raw, err := os.ReadFile(filepath.Join(base, "byte-corpus-v1/corpus.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err = json.Unmarshal(raw, &corpus); err != nil {
		t.Fatal(err)
	}
	selected := map[int]bool{}
	if os.Getenv("GRAPHITE_MIRROR_FORENSIC_ONLY_FAILURES") != "" {
		var prior struct{ Mismatches []struct{ Index int } }
		b, e := os.ReadFile(filepath.Join(base, "full-mirror-v1/FORENSIC.json"))
		if e != nil {
			t.Fatal(e)
		}
		if e = json.Unmarshal(b, &prior); e != nil {
			t.Fatal(e)
		}
		for _, row := range prior.Mismatches {
			selected[row.Index] = true
		}
	}
	const now = 1792532494
	oldNow := whisper.Now
	whisper.Now = func() time.Time { return time.Unix(now, 0) }
	defer func() { whisper.Now = oldNow }()
	db, err := store.Open(filepath.Join(base, "full-mirror-v1/mirror320-exact"), store.Options{ExactBlocks: true, CacheSize: 256 << 20, MemTableSize: 64 << 20, Now: whisper.Now})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx := context.Background()
	jobs := make(chan int)
	results := make(chan map[string]any, len(corpus.Rows))
	var workers sync.WaitGroup
	for range 8 {
		workers.Go(func() {
			for index := range jobs {
				row := &corpus.Rows[index]
				path := filepath.Join(base, "full-mirror-v1/mirror320/original", row.Path)
				want, e := whisperio.ReadSnapshotIn(ctx, row.Metric, path, filepath.Join(base, "full-mirror-v1/mirror320"))
				if e != nil {
					results <- map[string]any{"index": index, "error": e.Error()}
					continue
				}
				got, e := db.Snapshot(ctx, row.Metric)
				if e != nil {
					results <- map[string]any{"index": index, "error": e.Error()}
					continue
				}
				if store.EqualSnapshots(want, got) == nil {
					continue
				}
				for archive := range want.Archives {
					a, b := want.Archives[archive].Points, got.Archives[archive].Points
					var extra, missing, changed []store.Point
					left, right := 0, 0
					for left < len(a) || right < len(b) {
						if left == len(a) || (right < len(b) && b[right].Timestamp < a[left].Timestamp) {
							extra = append(extra, b[right])
							right++
							continue
						}
						if right == len(b) || a[left].Timestamp < b[right].Timestamp {
							missing = append(missing, a[left])
							left++
							continue
						}
						if math.Float64bits(a[left].Value) != math.Float64bits(b[right].Value) {
							changed = append(changed, b[right])
						}
						left++
						right++
					}
					if len(extra)+len(missing)+len(changed) == 0 {
						continue
					}
					var probes []map[string]any
					for _, point := range extra[:min(8, len(extra))] {
						from, until := int(point.Timestamp)-2, int(point.Timestamp)+2
						sourceSeries, se := readFullMirrorSeries(path, from, until)
						sharedSeries, de := db.Fetch(ctx, row.Metric, from, until)
						probe := map[string]any{"timestamp": point.Timestamp, "age_seconds": now - point.Timestamp, "bits": math.Float64bits(point.Value), "public_fetch_equal": fullMirrorSeriesDigest(sourceSeries) == fullMirrorSeriesDigest(sharedSeries), "source_reply": forensicMirrorSeries(sourceSeries), "shared_reply": forensicMirrorSeries(sharedSeries)}
						if se != nil {
							probe["source_error"] = se.Error()
						}
						if de != nil {
							probe["shared_error"] = de.Error()
						}
						probes = append(probes, probe)
					}
					result := map[string]any{"index": index, "archive": archive, "retention": want.Archives[archive].Retention, "source_count": len(a), "shared_count": len(b), "extra_count": len(extra), "missing_count": len(missing), "changed_count": len(changed), "extra_probes": probes}
					if len(extra) > 0 {
						result["extra_minimum_age_seconds"] = now - extra[len(extra)-1].Timestamp
						result["extra_max_timestamp"] = extra[len(extra)-1].Timestamp
						result["all_extra_before_source_head"] = len(a) > 0 && extra[len(extra)-1].Timestamp < a[0].Timestamp
					}
					if len(a) > 0 {
						result["source_first"] = a[0].Timestamp
						result["source_last"] = a[len(a)-1].Timestamp
					}
					if len(b) > 0 {
						result["shared_first"] = b[0].Timestamp
						result["shared_last"] = b[len(b)-1].Timestamp
					}
					results <- result
				}
			}
		})
	}
	for i := range corpus.Rows {
		if len(selected) == 0 || selected[i] {
			jobs <- i
		}
	}
	close(jobs)
	workers.Wait()
	close(results)
	var all []map[string]any
	for row := range results {
		all = append(all, row)
	}
	fullMirrorWriteJSON(t, filepath.Join(base, "full-mirror-v1/FORENSIC.json"), map[string]any{"model_now": now, "mismatches": all, "scope": "Private failure diagnosis; no input values or cases excluded from the failed oracle."})
	t.Logf("mismatches: %d", len(all))
}

func forensicMirrorSeries(s *store.Series) any {
	if s == nil {
		return nil
	}
	bits := make([]uint64, len(s.Values))
	for i, value := range s.Values {
		bits[i] = math.Float64bits(value)
	}
	return map[string]any{"from": s.FromTime, "until": s.UntilTime, "step": s.Step, "bits": bits}
}
