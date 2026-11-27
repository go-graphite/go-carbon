package main

import (
	"context"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/go-graphite/go-carbon/internal/sealrollup"
	"github.com/go-graphite/go-carbon/internal/v2core"
	"github.com/go-graphite/go-carbon/internal/v2part"
	pb "github.com/go-graphite/protocol/carbonapi_v2_pb"
	"google.golang.org/protobuf/proto"
)

func sourceBacklog(dir, path string, offset int64) map[string]any {
	paths, err := filepath.Glob(filepath.Join(dir, "*.log"))
	var bytes int64
	segments := 0
	for _, p := range paths {
		if p < path {
			continue
		}
		st, e := os.Stat(p)
		if e != nil {
			err = e
			continue
		}
		if p == path {
			bytes += max(0, st.Size()-offset)
		} else {
			bytes += st.Size()
			segments++
		}
	}
	result := map[string]any{"preallocated_file_bytes_upper_bound": bytes, "successor_segments": segments}
	if err != nil {
		result["error"] = err.Error()
	}
	return result
}

func fetchPolicy(client *http.Client, base, name string) ([]byte, error) {
	resp, err := client.Get(base + "/info/?" + url.Values{"target": {name}, "format": {"protobuf"}}.Encode())
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("info HTTP %d: %.100s", resp.StatusCode, b)
	}
	var info pb.InfoResponse
	if err = proto.Unmarshal(b, &info); err != nil {
		return nil, err
	}
	methods := map[string]sealrollup.Method{"average": sealrollup.Average, "sum": sealrollup.Sum, "last": sealrollup.Last, "max": sealrollup.Max, "min": sealrollup.Min, "first": sealrollup.First}
	cfg := sealrollup.Config{Method: methods[strings.ToLower(info.AggregationMethod)], XFilesFactor: info.XFilesFactor}
	for _, r := range info.Retentions {
		cfg.Archives = append(cfg.Archives, sealrollup.Archive{Step: int64(r.SecondsPerPoint), Count: int64(r.NumberOfPoints)})
	}
	return v2core.EncodeRollupPolicy(v2core.RollupPolicy{Name: v2part.ID(name), Group: "shadow", Config: cfg})
}

// Fetch selects archives by lookback. Only returned steps above the finest
// archive count as L3. Compare completed intervals strictly inside the run.
func compareL3(job compareJob, base string, sums *totals) bool {
	defer func() {
		for _, s := range job.Snapshots {
			_ = s.Close()
		}
	}()
	start := time.Now()
	ctx := context.Background()
	type metric struct {
		p v2core.RollupPolicy
		r []sealrollup.Record
	}
	selected := map[string]*metric{}
	for _, n := range job.Names {
		selected[n.Name] = nil
	}
	var failures []string
	for _, s := range job.Snapshots {
		snap, e := v2core.NewRollupSnapshot(ctx, s, nil, nil, nil, 0)
		if e != nil {
			failures = append(failures, e.Error())
			continue
		}
		snap.Metadata(func(name string, p v2core.RollupPolicy, _ v2core.RollupMetadata) {
			if _, ok := selected[name]; ok {
				selected[name] = &metric{p: p}
			}
		})
		for name, m := range selected {
			if m == nil || m.r != nil {
				continue
			}
			r, e := snap.Records(ctx, name)
			if e != nil {
				failures = append(failures, e.Error())
			} else {
				m.r = r
			}
		}
	}
	client := &http.Client{Timeout: 10 * time.Second}
	pairs, total, bad, archives := 0, 0, 0, 0
	var examples []difference
	var observations []difference
	var coverage []map[string]any
	for _, n := range job.Names {
		m := selected[n.Name]
		if m == nil {
			failures = append(failures, n.Name+": no rollup metadata")
			continue
		}
		comparedName := false
		first := 1
		if len(m.p.Config.Archives) == 1 {
			first = 0
		}
		for a := first; a < len(m.p.Config.Archives); a++ {
			step := m.p.Config.Archives[a].Step
			lo := (job.From/step + 1) * step
			hi := job.Until / step * step
			if hi <= lo {
				continue
			}
			// Request well beyond the finer retention, preserving the recent until.
			queryFrom := lo - 1
			if a > 0 {
				prev := m.p.Config.Archives[a-1]
				queryFrom = time.Now().Unix() - prev.Step*prev.Count - 2*step
			}
			classic, e := fetch(client, base, n.Name, queryFrom, hi-1)
			if e != nil {
				failures = append(failures, n.Name+": "+e.Error())
				continue
			}
			if int64(classic.StepTime) != step {
				failures = append(failures, fmt.Sprintf("%s: requested archive%d step%d returned%d", n.Name, a, step, classic.StepTime))
				continue
			}
			var rows []v2core.Row
			for _, r := range m.r {
				if r.Archive == a {
					rows = append(rows, v2core.Row{Time: r.Sample.Time, Bits: r.Sample.Bits, Seq: r.Order})
				}
			}

			if job.PostClose && !job.SkipObservations && len(classic.IsAbsent) == len(classic.Values) {
				slots := map[int64]slot{}
				for _, r := range rows {
					if old, ok := slots[r.Time]; !ok || r.Seq > old.Seq {
						slots[r.Time] = slot{r.Bits, r.Seq}
					}
				}
				for i, v := range classic.Values {
					ts := int64(classic.StartTime) + int64(i)*step
					if ts < lo || ts >= hi {
						continue
					}
					r, ok := slots[ts]
					observations = append(observations, difference{n.Name, ts, fmt.Sprintf("%016x", r.Bits), fmt.Sprintf("%016x", math.Float64bits(v)), ok, !classic.IsAbsent[i]})
				}
			}
			count, diff, ex, e := compareRaw(n.Name, rows, classic, lo, hi)
			if e != nil {
				failures = append(failures, e.Error())
				continue
			}
			total += count
			bad += diff
			archives++
			comparedName = true
			examples = append(examples, ex...)
			coverage = append(coverage, map[string]any{"name": n.Name, "archive": a, "step": step, "from": lo, "until": hi, "slots": count, "differences": diff, "method": m.p.Config.Method, "tagged": strings.Contains(n.Name, ";")})
		}
		if comparedName {
			pairs++
		}
	}
	sums.Names += uint64(pairs)
	sums.Points += uint64(total)
	sums.Mismatches += uint64(bad)
	sums.Failures += uint64(len(failures))
	// Initial differences stay in totals; no reconstruction mismatch is waived.
	emit(map[string]any{"stage": "l3_comparison", "context": contextLoad(), "names": job.Names, "from": job.From, "until": job.Until, "compared_names": pairs, "compared_archives": archives, "compared_points": total, "mismatches": bad, "examples": examples, "observations": observations, "failures": failures, "coverage": coverage, "seconds": time.Since(start).Seconds(), "source_lag_seconds": job.SourceLagSeconds, "source_segments_behind": job.SourceSegmentsBehind, "post_close_recheck": job.PostClose, "original_classic_applications_available": false})
	return false
}
