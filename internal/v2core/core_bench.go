package v2core

import (
	"os"
	"runtime"
	"runtime/metrics"
	"strconv"
	"strings"
	"time"
)

func coreSample(engine *coreEngine, stage string, start time.Time, offered uint64) map[string]any {
	// Completed credit waits are separate from edge-queue waits. The live
	// gauge also exposes a producer whose current wait has not completed yet.
	var leaseWaitNS, leaseWaitCount uint64
	var leaseWaiting int
	for i := range engine.routers {
		router := &engine.routers[i]
		leaseWaitNS += router.leaseWaitNS.Load()
		leaseWaitCount += router.leaseWaitCount.Load()
		if router.leaseWaiting.Load() {
			leaseWaiting++
		}
	}
	runtimeSamples := []metrics.Sample{{Name: "/cpu/classes/gc/total:cpu-seconds"}, {Name: "/cpu/classes/gc/mark/assist:cpu-seconds"}, {Name: "/gc/scan/heap:bytes"}}
	metrics.Read(runtimeSamples)
	runtimeCounters := make(map[string]any, len(runtimeSamples))
	for _, sample := range runtimeSamples {
		switch sample.Value.Kind() {
		case metrics.KindFloat64:
			runtimeCounters[sample.Name] = sample.Value.Float64()
		case metrics.KindUint64:
			runtimeCounters[sample.Name] = sample.Value.Uint64()
		}
	}
	var syncNS, syncMaxNS, syncCalls uint64
	var memory runtime.MemStats
	runtime.ReadMemStats(&memory)
	var points, parts, bytes, debt, compacted uint64
	var l0, l0Sublevels, memtables, flushes int64
	var readAmp int
	var memtableBytes uint64
	var phases []map[string]any
	var liveBytes uint64
	for owner, o := range engine.owners {
		points += o.committed.Load()
		parts += o.parts.Load()
		bytes += o.bytes.Load()
		syncNS += o.syncNS.Load()
		syncMaxNS = max(syncMaxNS, o.syncMaxNS.Load())
		syncCalls += o.syncCalls.Load()
		m := o.db.Metrics()
		debt += m.Compact.EstimatedDebt
		for _, level := range m.Levels {
			compacted += level.BytesCompacted
		}
		l0 += m.Levels[0].NumFiles
		l0Sublevels += int64(m.Levels[0].Sublevels)
		readAmp += m.ReadAmp()
		memtables += m.MemTable.Count
		memtableBytes += m.MemTable.Size
		flushes += m.Flush.Count
		for _, level := range m.Levels {
			liveBytes += uint64(level.Size)
		}
		var blocked uint64
		for _, q := range o.queues {
			blocked += q.blockedNS.Load()
		}
		phases = append(phases, map[string]any{"owner": owner, "seal_rows": o.sealRows, "sealing": o.sealing.Load(), "seal_calls": o.sealCalls.Load(), "seal_seconds": float64(o.sealNS.Load()) / 1e9, "compaction_count": m.Compact.Count, "compaction_in_progress": m.Compact.NumInProgress, "compaction_in_progress_bytes": m.Compact.InProgressBytes, "compaction_seconds": m.Compact.Duration.Seconds(), "compaction_debt_bytes": m.Compact.EstimatedDebt, "sst_sync_max_seconds": float64(o.syncMaxNS.Load()) / 1e9, "sst_sync_seconds": float64(o.syncNS.Load()) / 1e9, "sst_sync_calls": o.syncCalls.Load(), "committed": o.committed.Load(), "sort_seconds": float64(o.sortNS.Load()) / 1e9, "encode_seconds": float64(o.encodeNS.Load()) / 1e9, "ingest_seconds": float64(o.ingestNS.Load()) / 1e9, "producer_queue_wait_seconds": float64(blocked) / 1e9})
	}
	return map[string]any{"producer_lease_wait_seconds": float64(leaseWaitNS) / 1e9, "producer_lease_wait_count": leaseWaitCount, "producer_lease_waiting": leaseWaiting, "runtime_counters": runtimeCounters, "sst_sync_seconds": float64(syncNS) / 1e9, "sst_sync_max_seconds": float64(syncMaxNS) / 1e9, "sst_sync_calls": syncCalls, "l0_sublevels": l0Sublevels, "read_amp": readAmp, "memtables": memtables, "memtable_bytes": memtableBytes, "flushes": flushes, "heap_alloc": memory.HeapAlloc, "heap_inuse": memory.HeapInuse, "total_alloc": memory.TotalAlloc, "num_gc": memory.NumGC, "gc_pause_ns": memory.PauseTotalNs, "gc_cpu_fraction": memory.GCCPUFraction, "owner_phases": phases, "live_sst_bytes": liveBytes, "stage": stage, "when_cet": time.Now().In(time.FixedZone("CET", 3600)).Format(time.RFC3339Nano), "seconds": time.Since(start).Seconds(), "offered": offered, "committed": points, "parts": parts, "sink_bytes": bytes, "estimated_debt_bytes": debt, "compacted_bytes": compacted, "l0_files": l0, "write_stalls": engine.stalls.Load(), "proc": procStats()}
}

// Linux /proc counters are process-wide, including Pebble background threads.
func procStats() map[string]float64 {
	out := map[string]float64{}
	b, _ := os.ReadFile("/proc/self/io")
	for _, line := range strings.Split(string(b), "\n") {
		f := strings.Fields(line)
		if len(f) == 2 {
			v, _ := strconv.ParseFloat(f[1], 64)
			out[strings.TrimSuffix(f[0], ":")] = v
		}
	}
	b, _ = os.ReadFile("/proc/self/status")
	for _, line := range strings.Split(string(b), "\n") {
		f := strings.Fields(line)
		if len(f) > 1 && (f[0] == "VmHWM:" || f[0] == "VmRSS:") {
			v, _ := strconv.ParseFloat(f[1], 64)
			out[strings.TrimSuffix(f[0], ":")+"_bytes"] = v * 1024
		}
	}
	b, _ = os.ReadFile("/proc/self/stat")
	i := strings.LastIndex(string(b), ")")
	if i >= 0 {
		f := strings.Fields(string(b[i+1:]))
		if len(f) > 12 {
			u, _ := strconv.ParseFloat(f[11], 64)
			st, _ := strconv.ParseFloat(f[12], 64)
			out["cpu_seconds_CLK_TCK100"] = (u + st) / 100
		}
	}
	return out
}
