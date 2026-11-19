package v2core

import (
	"os"
	"runtime"
	"syscall"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/internal/sealrollup"
)

// Opt-in holder experiment, also runnable with the unchanged-runtime overlay.
// The full core differential separately exercises durable replacement and reads.
func TestRollupHistoryMillionNameHolder(t *testing.T) {
	if os.Getenv("ROLLUP_HISTORY_MEASURE") != "1" {
		t.Skip("opt-in million-name populated holder fixture")
	}
	const names = 1000000
	prepared, err := sealrollup.Prepare(sealrollup.Config{Archives: []sealrollup.Archive{{Step: 10, Count: 2160}, {Step: 300, Count: 2016}, {Step: 3600, Count: 8760}}, Method: sealrollup.Average, XFilesFactor: .1})
	if err != nil {
		t.Fatal(err)
	}
	runtime.GC()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)
	states := make([]rollupMetric, names)
	for i := range states {
		states[i].slots = prepared.NewSlots()
	}
	type recent interface {
		KeepRecent(int64)
		InstallRecent(sealrollup.Delta)
	}
	_, bounded := any(states[0].slots).(recent)
	now := int64(1794700800)
	var points uint64
	start := time.Now()
	cpuStart := historyCPUTime()
	for epoch := int64(1); epoch <= 36; epoch++ {
		now += 600
		var updates []sealrollup.Delta
		// Six populated ten-second slots plus actual minute/hour archive values
		// per ten-minute tick. One fifth of names (3.23% of input points) also receive an old-window sample.
		for j := int64(0); j < 6; j++ {
			stamp := now - 60 + j*10
			updates = append(updates, sealrollup.Delta{Archive: 0, Slot: (stamp / 10) % 2160, Order: uint64(epoch), Sample: sealrollup.Sample{Time: stamp, Bits: uint64(j), Sequence: uint64(epoch)}})
		}
		for a, step := range []int64{300, 3600} {
			stamp := (now - 10) - (now-10)%step
			count := int64(2016)
			if a == 1 {
				count = 8760
			}
			updates = append(updates, sealrollup.Delta{Archive: a + 1, Slot: (stamp / step) % count, Order: uint64(epoch), Sample: sealrollup.Sample{Time: stamp, Bits: uint64(epoch), Sequence: uint64(epoch)}})
		}
		for i := range states {
			s := states[i].slots
			if bounded {
				any(s).(recent).KeepRecent(now - 10)
			}
			if bounded {
				r := any(s).(recent)
				for _, d := range updates {
					r.InstallRecent(d)
				}
				r.KeepRecent(now - 10)
			} else {
				for _, d := range updates {
					s.Install(d)
				}
			}
			if i%5 == 0 {
				stamp := now - 1800
				d := sealrollup.Delta{Archive: 0, Slot: (stamp / 10) % 2160, Order: uint64(epoch), Sample: sealrollup.Sample{Time: stamp, Bits: 1, Sequence: uint64(epoch)}}
				if bounded {
					r := any(s).(recent)
					r.InstallRecent(d)
					r.KeepRecent(now - 10)
				} else {
					s.Install(d)
				}
			}
		}
		points += names*6 + names/5
		if epoch == 6 || epoch == 18 || epoch == 36 {
			elapsed := time.Since(start)
			runtime.GC()
			var stats runtime.MemStats
			runtime.ReadMemStats(&stats)
			t.Logf("holder bounded=%v hours=%d names=%d heap_B_name=%.2f wall_ns_point=%.2f points=%d CPU_ns_point=%.2f", bounded, epoch/6, names, float64(stats.HeapAlloc-before.HeapAlloc)/names, float64(elapsed.Nanoseconds())/float64(points), points, float64(historyCPUTime()-cpuStart)/float64(points))
		}
	}
	runtime.KeepAlive(states)

}

func historyCPUTime() int64 {
	var u syscall.Rusage
	_ = syscall.Getrusage(syscall.RUSAGE_SELF, &u)
	return u.Utime.Nano() + u.Stime.Nano()
}
