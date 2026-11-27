package v2read

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"os/exec"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/internal/partsformat"
	"github.com/go-graphite/go-carbon/internal/sealrollup"
	"github.com/go-graphite/go-carbon/internal/v2core"
)

// TestHostConcurrent is opt-in: equal-duration, unrestricted producer loops,
// paired with zero/four read workers against the same actual L3 engine. It
// includes final core Close in complete throughput, and emits every arm.
func TestHostConcurrent(t *testing.T) {
	if os.Getenv("V2READ_HOST") != "1" {
		t.Skip("explicit host qualification only")
	}
	duration := 4 * time.Second
	if value := os.Getenv("V2READ_SECONDS"); value != "" {
		seconds, err := strconv.Atoi(value)
		if err != nil || seconds < 1 || seconds > 60 {
			t.Fatal("V2READ_SECONDS must be between 1 and 60")
		}
		duration = time.Duration(seconds) * time.Second
	}
	for arm, readers := range []int{0, 4, 0, 4, 0, 4} {
		beforeLoad, _ := os.ReadFile("/proc/loadavg")
		beforeProcesses, _ := exec.Command("ps", "-eo", "pid,pcpu,comm,args", "--sort=-pcpu").Output()
		reader, err := New(4, 1<<30, 1<<20)
		if err != nil {
			t.Fatal(err)
		}
		cfg := testConfig()
		cfg.Producers = 4
		cfg.Shards = 4
		cfg.Owners = 4
		cfg.MaxRows = 32768
		engine, err := v2core.Open(t.TempDir(), cfg, v2core.Hooks{PublishPending: reader.PublishPending})
		if err != nil {
			t.Fatal(err)
		}
		policies := make([][]byte, 4)
		names := make([]string, 4)
		for i := range policies {
			names[i] = fmt.Sprintf("bench.metric%d", i)
			policies[i], err = v2core.EncodeRollupPolicy(v2core.RollupPolicy{Name: partsformat.ID(names[i]), Group: "test", Config: sealrollup.Config{Method: sealrollup.Average, XFilesFactor: .5, Archives: []sealrollup.Archive{{Step: 1, Count: 3600}, {Step: 60, Count: 1440}}}})
			if err != nil {
				t.Fatal(err)
			}
		}
		// Each producer has its own exact application stream and metric identity.
		seed := make([]v2core.Row, 1024)
		for p := range 4 {
			for j := range seed {
				seed[j] = v2core.Row{ID: uint64(p + 1), Time: 1794700000 + int64(j), Bits: math.Float64bits(float64(j)), Seq: uint64(j + 1)}
			}
			if err = engine.AppendBatches(p, []v2core.Batch{{ID: uint64(p + 1), Order: 1, Clock: 1794701100, Policy: policies[p], Points: seed}}, 1, true); err != nil {
				t.Fatal(err)
			}
		}
		var written, queries, readErrors atomic.Uint64
		var writers, readersWG sync.WaitGroup
		start := time.Now()
		deadline := start.Add(duration)
		fmt.Printf("V2READ_ARM arm=%d readers=%d duration=%s\n", arm, readers, duration)
		samples := make([][]int64, readers)
		readEnds := make([]time.Duration, readers)
		for id := range readers {
			readersWG.Add(1)
			go func(id int) {
				defer readersWG.Done()
				local := make([]int64, 0, 65536)
				for time.Now().Before(deadline) {
					q := time.Now()
					from := int64(1794700980)
					if len(local)%2 == 1 {
						from = 1794697440
					}
					e := reader.FetchMany(context.Background(), names, from, 1794701100, 1794701100, func(_ string, s *Series) error { return nil })
					if e != nil {
						readErrors.Add(1)
					}
					queries.Add(1)
					local = append(local, time.Since(q).Nanoseconds())
				}
				samples[id] = local
				readEnds[id] = time.Since(start)
			}(id)
		}
		errs := make([]error, 4)
		for p := range 4 {
			writers.Add(1)
			go func(p int) {
				defer writers.Done()
				rows := make([]v2core.Row, 4096)
				for i := range rows {
					rows[i] = v2core.Row{ID: uint64(p + 1), Time: 1794700000 + int64(i%1024), Bits: math.Float64bits(float64(i % 17)), Seq: uint64(i + 1)}
				}
				order := uint64(2)
				for time.Now().Before(deadline) {
					e := engine.AppendBatchesPipelined(p, []v2core.Batch{{ID: uint64(p + 1), Order: order, Clock: 1794701100, Policy: policies[p], Points: rows}}, order, false)
					if e != nil {
						errs[p] = e
						return
					}
					written.Add(uint64(len(rows)))
					order++
				}
				errs[p] = engine.AppendBatches(p, nil, order, true)
			}(p)
		}
		writers.Wait()
		intake := time.Since(start)
		readersWG.Wait()
		readDuration := duration
		if readers > 0 {
			readDuration = slices.Max(readEnds)
		}
		reader.Close()
		if err = engine.Close(); err != nil {
			t.Fatal(err)
		}
		complete := time.Since(start)
		for _, e := range errs {
			if e != nil {
				t.Fatal(e)
			}
		}
		if readErrors.Load() != 0 {
			t.Fatalf("read errors %d", readErrors.Load())
		}
		var latency []int64
		for _, s := range samples {
			latency = append(latency, s...)
		}
		slices.Sort(latency)
		var p50, p99 int64
		if len(latency) > 0 {
			p50 = latency[len(latency)/2]
			p99 = latency[(len(latency)-1)*99/100]
		}
		afterLoad, _ := os.ReadFile("/proc/loadavg")
		afterProcesses, _ := exec.Command("ps", "-eo", "pid,pcpu,comm,args", "--sort=-pcpu").Output()
		output := map[string]any{"load_before": strings.TrimSpace(string(beforeLoad)), "load_after": strings.TrimSpace(string(afterLoad)), "processes_before": string(beforeProcesses[:min(len(beforeProcesses), 8192)]), "processes_after": string(afterProcesses[:min(len(afterProcesses), 8192)]), "arm": arm, "readers": readers, "when_cet": time.Now().In(time.FixedZone("CET", 3600)).Format(time.RFC3339), "points": written.Load(), "intake_seconds": intake.Seconds(), "complete_seconds": complete.Seconds(), "complete_points_s": float64(written.Load()) / complete.Seconds(), "queries": queries.Load(), "queries_s": float64(queries.Load()) / readDuration.Seconds(), "read_seconds": readDuration.Seconds(), "requested_seconds": duration.Seconds(), "latency_samples": len(latency), "p50_ns": p50, "p99_ns": p99, "read_errors": readErrors.Load(), "gomaxprocs": runtime.GOMAXPROCS(0), "metrics": len(names), "producers": 4, "rows_per_batch": 4096}
		b, _ := json.Marshal(output)
		fmt.Printf("V2READ_RESULT %s\n", b)
	}
}
