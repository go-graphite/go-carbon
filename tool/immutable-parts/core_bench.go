package main

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"runtime"
	"sync/atomic"
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
	var points, parts, bytes, debt, compacted uint64
	var l0 int64
	for _, o := range engine.owners {
		points += o.committed.Load()
		parts += o.parts.Load()
		bytes += o.bytes.Load()
		m := o.db.Metrics()
		debt += m.Compact.EstimatedDebt
		for _, level := range m.Levels {
			compacted += level.BytesCompacted
		}
		l0 += m.Levels[0].NumFiles
	}
	return map[string]any{"stage": stage, "when_cet": time.Now().In(time.FixedZone("CET", 3600)).Format(time.RFC3339), "seconds": time.Since(start).Seconds(), "offered": offered, "committed": points, "parts": parts, "sink_bytes": bytes, "estimated_debt_bytes": debt, "compacted_bytes": compacted, "l0_files": l0, "write_stalls": engine.stalls.Load(), "proc": procStats()}
}

// A bounded synthetic durability/LSM workload, explicitly not captured traffic.
// Time advances with offered input. Producer backpressure is visible as a
// shortfall against rate*elapsed; submitted batches are never silently dropped.
func coreBenchmark(out string, cfg coreConfig, duration time.Duration, rate uint64, metrics uint64, settle time.Duration) error {
	return coreBenchmarkWithHook(out, cfg, duration, rate, metrics, settle, nil)
}

// A hook(-1,nil,nil) call ends publication and must join readers/release pins.
func coreBenchmarkWithHook(out string, cfg coreConfig, duration time.Duration, rate uint64, metrics uint64, settle time.Duration, hook func(int, *pebble.Snapshot, []row) error) error {
	if duration <= 0 || rate == 0 || metrics == 0 {
		return os.ErrInvalid
	}
	engine, err := openCoreWithHook(out, cfg, hook)
	if err != nil {
		return err
	}
	start := time.Now()
	baseline := procStats()
	_ = json.NewEncoder(os.Stdout).Encode(map[string]any{"stage": "core_start", "when_cet": start.In(time.FixedZone("CET", 3600)).Format(time.RFC3339), "config": cfg, "rate": rate, "duration_seconds": duration.Seconds(), "metrics": metrics, "gomaxprocs": runtime.GOMAXPROCS(0), "proc": baseline, "workload": "synthetic_preidentified_rows"})
	done := make(chan error, cfg.Producers)
	var offered atomic.Uint64
	cursors := make([]atomic.Uint64, cfg.Producers)
	target := uint64(duration.Seconds() * float64(rate))
	for p := 0; p < cfg.Producers; p++ {
		go func(p int) {
			const batchSize = 32768
			var sent uint64
			for {
				global := sent*uint64(cfg.Producers) + uint64(p)
				if global >= target || time.Since(start) >= duration {
					done <- nil
					return
				}
				n := min(uint64(batchSize), (target-global+uint64(cfg.Producers)-1)/uint64(cfg.Producers))
				due := start.Add(time.Duration(float64(global) / float64(rate) * float64(time.Second)))
				if delay := time.Until(due); delay > 0 {
					time.Sleep(delay)
				}
				rows := make([]row, n)
				for i := range rows {
					seq := (sent+uint64(i))*uint64(cfg.Producers) + uint64(p)
					rows[i] = row{seq % metrics, 1800000000 + int64(seq/rate), math.Float64bits(float64(seq % 100003)), seq}
				}
				sent += n
				offered.Add(n)
				if e := engine.submit(p, rows, sent, false); e != nil {
					done <- e
					return
				}
				cursors[p].Store(sent)
			}
		}(p)
	}
	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	left := cfg.Producers
	for left > 0 {
		select {
		case e := <-done:
			left--
			err = errors.Join(err, e)
		case <-tick.C:
			_ = json.NewEncoder(os.Stdout).Encode(coreSample(engine, "core_sample", start, offered.Load()))
		}
	}
	// Producers have stopped, so the caller may issue final barriers on their queues.
	for p := 0; p < cfg.Producers; p++ {
		cursor := cursors[p].Load()
		if cursor == 0 {
			continue
		}
		err = errors.Join(err, engine.submit(p, nil, cursor, true))
	}
	_ = json.NewEncoder(os.Stdout).Encode(coreSample(engine, "core_ingest_complete", start, offered.Load()))
	until := time.Now().Add(settle)
	for time.Now().Before(until) {
		time.Sleep(time.Second)
		_ = json.NewEncoder(os.Stdout).Encode(coreSample(engine, "core_settle", start, offered.Load()))
	}
	final := coreSample(engine, "core_complete", start, offered.Load())
	final["baseline_proc"] = baseline
	final["scheduled_points"] = target
	final["target_met"] = offered.Load() >= target
	if final["committed"].(uint64) != offered.Load() {
		err = errors.Join(err, errors.New("final seal did not commit all submitted points"))
	}
	if hook != nil {
		err = errors.Join(err, hook(-1, nil, nil))
	}
	err = errors.Join(err, engine.close())
	if err != nil {
		return err
	}
	return json.NewEncoder(os.Stdout).Encode(final)
}
