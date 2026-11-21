package carbonserver

import (
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"os"
	"runtime"
	"slices"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/internal/sealrollup"
	"github.com/go-graphite/go-carbon/internal/v2core"
	"github.com/go-graphite/go-carbon/internal/v2daemon"
	"github.com/go-graphite/go-carbon/internal/v2index"
	"github.com/go-graphite/go-carbon/internal/v2part"
	"github.com/go-graphite/go-carbon/internal/v2read"
	"github.com/go-graphite/go-carbon/points"
)

// Opt-in paired daemon/journal/HTTP experiment, bounded by the Go test timeout.
func TestSealedMergeDaemonMeasure(t *testing.T) {
	duration, err := time.ParseDuration(os.Getenv("V2_MERGE_MEASURE_DURATION"))
	if err != nil {
		t.Skip("set V2_MERGE_MEASURE_DURATION=10m")
	}
	const owners = 16
	r, err := v2read.New(owners, 256<<20, 16<<20)
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	s, err := v2daemon.Open(root, v2daemon.Config{Layer: 3, Producers: 1, Shards: owners, MaxCoreRows: 256, GroupInterval: time.Hour, Hooks: v2core.Hooks{PublishPending: r.PublishPending}})
	if err != nil {
		r.Close()
		t.Fatal(err)
	}
	defer func() {
		r.Close()
		if err := s.Close(); err != nil {
			t.Error(err)
		}
	}()
	var apps []v2daemon.Application
	var names []string
	var chosen [owners]bool
	stamp := time.Now().Unix() - 10
	for i := 0; len(apps) < owners; i++ {
		name := fmt.Sprintf("merge.measure.%d", i)
		owner := int(v2part.Route(v2part.ID(name))) % owners
		if chosen[owner] {
			continue
		}
		chosen[owner] = true
		policy, err := v2core.EncodeRollupPolicy(v2core.RollupPolicy{Name: v2part.ID(name), Group: "test", Config: sealrollup.Config{Method: sealrollup.Last, Archives: []sealrollup.Archive{{Step: 1, Count: 86400}}}})
		if err != nil {
			t.Fatal(err)
		}
		apps = append(apps, v2daemon.Application{Name: name, Policy: policy, Points: []points.Point{{Timestamp: stamp, Value: 42}}, Sequence: []uint64{0}})
		names = append(names, name)
	}
	catalog := &v2index.Catalog{}
	if err := catalog.Publish(1, names); err != nil {
		t.Fatal(err)
	}
	listener := NewCarbonserverListener(nil)
	listener.SetTrieIndex(true)
	listener.SetMaxGlobs(100)
	listener.SetMaxMetricsRendered(100)
	listener.SetV2Reader(r)
	listener.initializeCaches()
	if err := listener.SetV2Catalog(catalog); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(listener.newHTTPMux())
	defer server.Close()
	var rounds atomic.Uint64
	var cpuStart syscall.Rusage
	_ = syscall.Getrusage(syscall.RUSAGE_SELF, &cpuStart)
	cpu := func(r syscall.Rusage) float64 {
		return float64(r.Utime.Sec+r.Stime.Sec) + float64(r.Utime.Usec+r.Stime.Usec)/1e6
	}
	start := time.Now()
	done := make(chan error, 1)
	go func() {
		for time.Since(start) < duration {
			n := rounds.Load() + 1
			now := time.Now().Unix()
			for i := range apps {
				apps[i].Order = n*owners + uint64(i)
				apps[i].Clock = now
				apps[i].Sequence[0] = n
			}
			p := s.Producers[0]
			if err := p.AppendApplications(apps); err != nil {
				done <- err
				return
			}
			if err := p.Seal(); err != nil {
				done <- err
				return
			}
			if err := p.Drain(); err != nil {
				done <- err
				return
			}
			// Bound admission to one outstanding round. The journal cursor is
			// the total original point count, independent of physical seals.
			for {
				complete := true
				for _, f := range s.Core.DurableFrontiers() {
					if f[0] < n*owners {
						complete = false
						break
					}
				}
				if complete {
					break
				}
				time.Sleep(time.Millisecond)
			}
			rounds.Store(n)
		}
		done <- nil
	}()
	sample := func() {
		stats := s.Core.PartGCStats()
		live := make([]uint64, len(stats))
		var runs, written uint64
		for i, x := range stats {
			live[i] = x.LiveParts
			runs += x.MergeRuns
			written += x.MergeWrittenBytes
			if x.Error != "" {
				t.Error(x.Error)
			}
		}
		var usage syscall.Rusage
		_ = syscall.Getrusage(syscall.RUSAGE_SELF, &usage)
		var mem runtime.MemStats
		runtime.ReadMemStats(&mem)
		b, _ := json.Marshal(map[string]any{"elapsed": time.Since(start).Seconds(), "rounds": rounds.Load(), "live_parts": live, "merge_runs": runs, "merge_bytes": written, "cpu_seconds": cpu(usage) - cpu(cpuStart), "heap": mem.HeapAlloc, "core": s.Core.BenchmarkStats()})
		fmt.Printf("MERGE_SAMPLE %s\n", b)
	}
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	timer := time.NewTimer(max(time.Millisecond, duration-5*time.Second))
	defer timer.Stop()
	measuring := false
	var latencies []float64
	client := &http.Client{Timeout: 5 * time.Second}
	render := func() {
		begin := time.Now()
		resp, err := client.Get(fmt.Sprintf("%s/render/?format=json&target=%s&from=%d&until=%d", server.URL, names[0], stamp-1, stamp))
		if err != nil {
			t.Error(err)
			return
		}
		body, err := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		var result struct {
			Metrics []struct {
				Values []float64 `json:"values"`
				Absent []bool    `json:"isAbsent"`
			} `json:"metrics"`
		}
		if err != nil || resp.StatusCode != 200 || json.Unmarshal(body, &result) != nil || len(result.Metrics) != 1 || len(result.Metrics[0].Values) != 1 || len(result.Metrics[0].Absent) != 1 || result.Metrics[0].Absent[0] || math.Float64bits(result.Metrics[0].Values[0]) != math.Float64bits(42) {
			t.Errorf("render mismatch status=%d body=%s err=%v", resp.StatusCode, body, err)
			return
		}
		latencies = append(latencies, float64(time.Since(begin).Nanoseconds())/1e6)
	}
	renderTicker := time.NewTicker(50 * time.Millisecond)
	defer renderTicker.Stop()
	for {
		select {
		case err := <-done:
			if err != nil {
				t.Fatal(err)
			}
			sample()
			if len(latencies) == 0 {
				render()
			}
			slices.Sort(latencies)
			var usage syscall.Rusage
			_ = syscall.Getrusage(syscall.RUSAGE_SELF, &usage)
			var mergeBytes uint64
			for _, x := range s.Core.PartGCStats() {
				mergeBytes += x.MergeWrittenBytes
			}
			stats := s.Core.BenchmarkStats()
			points := float64(rounds.Load() * owners)
			fmt.Printf("MERGE_FINAL workers=%s points=%.0f elapsed=%.3f cpu_seconds=%.6f cpu_ns_per_point=%.3f part_and_frontier_bytes_per_point=%.3f merge_bytes_per_point=%.3f render_samples=%d render_p99_ms=%.6f\n", os.Getenv("CORE_MERGE_WORKERS"), points, time.Since(start).Seconds(), cpu(usage)-cpu(cpuStart), (cpu(usage)-cpu(cpuStart))*1e9/points, (float64(stats["sst_bytes"].(uint64))+float64(mergeBytes))/points, float64(mergeBytes)/points, len(latencies), latencies[(len(latencies)-1)*99/100])
			return
		case <-ticker.C:
			sample()
		case <-timer.C:
			measuring = true
		case <-renderTicker.C:
			if measuring {
				render()
			}
		}
	}
}
