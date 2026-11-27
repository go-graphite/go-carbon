package v2core

import (
	"fmt"
	"os"
	"reflect"
	"runtime"
	"strconv"
	"syscall"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/internal/sealrollup"
)

// Opt-in end-to-end laptop fixture. It writes real immutable parts; the
// separate million-name holder fixture attributes resident reducer bytes.
func TestRollupHistoryDurableCost(t *testing.T) {
	if os.Getenv("ROLLUP_HISTORY_CORE_NAMES") == "" {
		t.Skip("opt-in durable populated fixture")
	}
	names, err := strconv.Atoi(os.Getenv("ROLLUP_HISTORY_CORE_NAMES"))
	if err != nil {
		t.Fatal(err)
	}
	epochs := 36
	if raw := os.Getenv("ROLLUP_HISTORY_CORE_EPOCHS"); raw != "" {
		epochs, _ = strconv.Atoi(raw)
	}
	tick := int64(600)
	if raw := os.Getenv("ROLLUP_HISTORY_CORE_TICK"); raw != "" {
		tick, _ = strconv.ParseInt(raw, 10, 64)
	}
	lateEvery := 5
	if raw := os.Getenv("ROLLUP_HISTORY_CORE_LATE_EVERY"); raw != "" {
		lateEvery, _ = strconv.Atoi(raw)
	}
	const pointsPerName = 6
	cfg := coreTestConfig()
	cfg.Shards = 4
	cfg.Producers = 1
	cfg.Rollups = true
	cfg.MaxRows = names * 7
	e, err := Open(t.TempDir(), cfg, Hooks{})
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	policies := make([]*ApplicationPolicy, names)
	for i := range policies {
		name := fmt.Sprintf("history.cost.%09d", i)
		_, definition := rollupTestPolicy(t, name, "10s:6h,5m:7d,1h:1y", sealrollup.Average, .1)
		policies[i], err = e.RegisterApplicationPolicy(uint64(i+1), name, definition)
		if err != nil {
			t.Fatal(err)
		}
	}
	batches := make([]Batch, names)
	rows := make([]Row, names*(pointsPerName+1))
	now := int64(1794700800)
	var points uint64
	cpuStart := historyCPUTime()
	start := time.Now()
	for epoch := 1; epoch <= epochs; epoch++ {
		now += tick
		for i := range batches {
			id := uint64(i + 1)
			ps := rows[i*7 : i*7+6 : i*7+7]
			for j := range ps {
				ps[j] = Row{ID: id, Time: now - 60 + int64(j)*10, Bits: uint64(i + j), Seq: uint64(j + 1)}
			}
			if lateEvery > 0 && i%lateEvery == 0 {
				ps = append(ps, Row{ID: id, Time: now - 1800, Bits: uint64(epoch), Seq: 100})
			}
			batches[i] = Batch{ID: id, Registered: policies[i], Order: uint64(epoch), Clock: now, Points: ps}
			points += uint64(len(ps))
		}
		if err = e.AppendBatches(0, batches, uint64(epoch), true); err != nil {
			t.Fatal(err)
		}
		if epoch == 6 || epoch == 18 || epoch == 36 || epoch == epochs {
			runtime.GC()
			var stats runtime.MemStats
			runtime.ReadMemStats(&stats)
			var loads, hits uint64
			for _, owner := range e.owners {
				for _, lane := range owner.rollups.lanes {
					cache := reflect.ValueOf(lane).FieldByName("windows")
					if cache.IsValid() {
						loads += cache.FieldByName("loads").Uint()
						hits += cache.FieldByName("hits").Uint()
					}
				}
			}
			t.Logf("durable names=%d hours=%.1f points=%d CPU_ns_point=%.2f wall_ns_point=%.2f heap_B_name=%.2f loads=%d hits=%d", names, float64(epoch)*float64(tick)/3600, points, float64(historyCPUTime()-cpuStart)/float64(points), float64(time.Since(start).Nanoseconds())/float64(points), float64(stats.HeapAlloc)/float64(names), loads, hits)
		}
	}
}

func historyCPUTime() int64 {
	var u syscall.Rusage
	_ = syscall.Getrusage(syscall.RUSAGE_SELF, &u)
	return u.Utime.Nano() + u.Stime.Nano()
}
