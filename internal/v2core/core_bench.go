package v2core

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"runtime"
	"runtime/metrics"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/go-graphite/go-carbon/internal/inputlog"
	"github.com/go-graphite/go-carbon/tags"
)

// Replay a single immutable journal. The dictionary prefix is decoded again on
// restart to reconstruct the collision-free canonical numeric ID catalogue.
// Cursors count source points (not sorted points); a segment identity prevents
// accidentally reusing a frontier with a different journal. Multi-segment/live
// journal retirement is intentionally not implemented by this offline command.
func coreReplay(journal, out string, cfg coreConfig) error {
	return coreReplayWithHook(journal, out, cfg, nil)
}
func coreReplayWithHook(journal, out string, cfg coreConfig, hook func(int, *pebble.Snapshot, []row) error) error {
	f, err := os.Open(journal)
	if err != nil {
		return err
	}
	defer f.Close()
	var header [64]byte
	if _, err = f.ReadAt(header[:], 0); err != nil {
		return err
	}
	cfg.JournalIdentity = fmt.Sprintf("%x", sha256.Sum256(header[:]))
	cfg.Producers = 1
	engine, err := openCoreWithHook(out, cfg, hook)
	if err != nil {
		return err
	}
	var result error
	defer func() {
		if engine != nil {
			_ = engine.close()
		}
	}()
	from := engine.globalFrontier()[0]
	st, err := f.Stat()
	if err != nil {
		return err
	}
	frontier, err := sealedFrontier(f, st.Size())
	if err != nil {
		return err
	}
	replay, err := inputlog.NewSegmentReplay(f)
	if err != nil {
		return err
	}
	defer replay.Close()
	ids := map[string]uint64{}
	var seq uint64
	batch := make([]row, 0, min(cfg.MaxRows, 32768))
	flush := func(force bool) error {
		if len(batch) == 0 {
			return nil
		}
		err := engine.submit(0, batch, seq, force)
		batch = batch[:0]
		return err
	}
	for {
		records, _, _, e := replay.Next(frontier)
		if errors.Is(e, io.EOF) {
			break
		}
		if e != nil {
			return e
		}
		for _, group := range records {
			name, e := tags.Normalize(group.Metric)
			if e != nil {
				return e
			}
			id, ok := ids[name]
			if !ok {
				id = uint64(len(ids))
				ids[name] = id
			}
			for _, point := range group.Data {
				seq++
				if seq <= from {
					continue
				}
				batch = append(batch, row{id, point.Timestamp, math.Float64bits(point.Value), seq})
				if len(batch) == cap(batch) {
					if e = flush(false); e != nil {
						return e
					}
				}
			}
		}
	}
	if seq < from {
		return errors.New("journal ends before committed frontier")
	}
	if err = flush(true); err != nil {
		return err
	}
	// Empty final barrier also seals previously queued full batches.
	if seq > 0 {
		if err = engine.submit(0, nil, seq, true); err != nil {
			return err
		}
	}
	cursors := engine.globalFrontier()
	result = engine.close()
	engine = nil
	if result != nil {
		return result
	}
	return json.NewEncoder(os.Stdout).Encode(map[string]any{"stage": "core_replay", "source_points": seq, "replayed_points": seq - from, "resume_from": from, "cursors": cursors, "metrics": len(ids), "journal_identity": cfg.JournalIdentity})
}

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
