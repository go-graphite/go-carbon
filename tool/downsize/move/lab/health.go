package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-graphite/go-carbon/carbon"
)

// Three Apps share one process and CPU budget in this lab. CPU is conservative
// process-wide usage; cache, committed write errors and read windows are per App.
// Disk/NIC capacity and memory headroom are fixture assumptions, not host probes.
func health(app *carbon.App, latency *[64]atomic.Uint64) *httptest.Server {
	var mu sync.Mutex
	lastTime, lastCPU := time.Now(), cpu()
	var previous [64]uint64
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/sample" {
			http.NotFound(w, r)
			return
		}
		mu.Lock()
		defer mu.Unlock()
		now, currentCPU := time.Now(), cpu()
		cores := (currentCPU - lastCPU) / now.Sub(lastTime).Seconds()
		lastTime, lastCPU = now, currentCPU
		var counts [64]uint64
		var total, cumulative, p99 uint64
		for i := range counts {
			current := latency[i].Load()
			counts[i], previous[i] = current-previous[i], current
			total += counts[i]
		}
		for i, count := range counts {
			cumulative += count
			if total > 0 && cumulative*100 >= total*99 {
				p99 = uint64(1) << i
				break
			}
		}
		writes, _ := app.Persister.WriteStats()
		_ = json.NewEncoder(w).Encode(map[string]any{
			"Time": float64(now.UnixNano()) / 1e9, "Generation": "measured-lab", "StoragePhase": "shared",
			"CPUCores": cores, "PhysicalCores": runtime.GOMAXPROCS(0), "CachePoints": app.Cache.Size(),
			"WriteErrors": writes.Failures, "ReadSamples": total, "ReadP99MS": float64(p99) / 1000,
			"DiskBytes": 1 << 40, "DiskFreeBytes": 500 << 30, "DiskWriteCapacity": 2e9,
			"MemoryBytes": 16 << 30, "AvailableBytes": 8 << 30, "NICCapacityBytes": 1e9,
		})
	}))
}
