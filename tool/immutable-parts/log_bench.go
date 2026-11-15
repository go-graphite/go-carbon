package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/go-graphite/go-carbon/internal/partlog"
)

// logBacklogBenchmark uses a fixed synthetic name-ID catalogue and timestamps
// representing a ten-minute, six-million-point/s backlog. It does not stand in
// for captured traffic, ordered rollups, or a steady-state compaction proof.
func logBacklogBenchmark(root, phase string, total, metrics uint64, producers, owners int) error {
	if total == 0 || metrics == 0 || producers < 1 || owners < 1 {
		return os.ErrInvalid
	}
	start := time.Now()
	baseline := procStats()
	cfg := coreConfig{AppendParts: true, Shards: owners, Producers: producers, MaxRows: 1 << 20, PartitionSeconds: 3600}
	emit := func(stage string, points uint64, extra map[string]any) {
		out := map[string]any{"stage": stage, "when_cet": time.Now().In(time.FixedZone("CET", 3600)).Format(time.RFC3339), "points": points, "seconds": time.Since(start).Seconds(), "phase": phase, "gomaxprocs": runtime.GOMAXPROCS(0), "producers": producers, "owners": owners, "metrics": metrics, "proc": procStats(), "proc_start": baseline}
		for k, v := range extra {
			out[k] = v
		}
		out["stage"] = stage
		_ = json.NewEncoder(os.Stdout).Encode(out)
	}
	opts := partlog.Options{Streams: producers, SegmentBytes: 1 << 30, SyncInterval: time.Second}
	if phase == "scan" {
		log, e := partlog.Open(filepath.Join(root, "log"), opts)
		if e != nil {
			return e
		}
		e = log.Close()
		emit("log_recovery_scan_complete", 0, map[string]any{"error": fmt.Sprint(e)})
		return e
	}
	if phase == "write" {
		offset := uint64(0)
		if raw := os.Getenv("PARTLOG_BENCH_OFFSET"); raw != "" {
			var err error
			offset, err = strconv.ParseUint(raw, 10, 64)
			if err != nil {
				return err
			}
		}
		log, e := partlog.Open(filepath.Join(root, "log"), opts)
		if e != nil {
			return e
		}
		done := make(chan error, producers)
		var count atomic.Uint64
		for p := 0; p < producers; p++ {
			go func(p int) {
				const batch = 32768
				for sent := uint64(0); ; {
					first := sent*uint64(producers) + uint64(p)
					if first >= total {
						done <- nil
						return
					}
					n := min(uint64(batch), (total-first+uint64(producers)-1)/uint64(producers))
					rows := make([]row, n)
					for i := range rows {
						seq := offset + (sent+uint64(i))*uint64(producers) + uint64(p)
						rows[i] = row{seq % metrics, 1800000000 + int64(seq/6000000), math.Float64bits(float64(seq % 100003)), seq}
					}
					if _, e := log.Append(p, 1800000000+int64(first/6000000), encodeLogRows(rows)); e != nil {
						done <- e
						return
					}
					sent += n
					count.Add(n)
				}
			}(p)
		}
		tick := time.NewTicker(5 * time.Second)
		defer tick.Stop()
		left := producers
		var result error
		for left > 0 {
			select {
			case e := <-done:
				left--
				result = errors.Join(result, e)
			case <-tick.C:
				emit("log_write_sample", count.Load(), nil)
			}
		}
		result = errors.Join(result, log.Close())
		emit("log_write_complete", count.Load(), map[string]any{"error": fmt.Sprint(result)})
		return result
	}
	if phase == "replay-live" {
		return logReplayLive(root, cfg, opts, total, metrics, emit, start)
	}
	if phase != "replay" {
		return os.ErrInvalid
	}
	l, e := openLoggedCore(filepath.Join(root, "log"), filepath.Join(root, "parts"), cfg, opts)
	if e != nil {
		return e
	}
	emit("log_recovery_scan_complete", 0, nil)
	done := make(chan error, producers)
	var count atomic.Uint64
	from := l.frontier()
	for p := 0; p < producers; p++ {
		go func(p int) {
			var last uint64
			e := l.journal.Replay(p, from[p], func(r partlog.Record) error {
				last = r.Cursor
				if e := l.apply(p, r, false); e != nil {
					return e
				}
				count.Add(uint64((len(r.Payload) - logEnvelopeBytes) / recordBytes))
				return nil
			})
			if e == nil && last > 0 {
				e = l.engine.submit(p, nil, last, true)
			}
			done <- e
		}(p)
	}
	tick := time.NewTicker(5 * time.Second)
	defer tick.Stop()
	left := producers
	var result error
	for left > 0 {
		select {
		case e := <-done:
			left--
			result = errors.Join(result, e)
		case <-tick.C:
			extra := coreSample(l.engine, "replay", start, count.Load())
			queues := make([]map[string]any, producers)
			for producer := range producers {
				depth := 0
				var wait uint64
				for _, owner := range l.engine.owners {
					q := owner.queues[producer]
					for i := range q.slots {
						if q.slots[i].Load() != nil {
							depth++
						}
					}
					wait += q.blockedNS.Load()
				}
				queues[producer] = map[string]any{"stream": producer, "queue_depth": depth, "queue_wait_seconds": float64(wait) / 1e9}
			}
			extra["streams"] = queues
			emit("log_replay_sample", count.Load(), extra)
			if path := os.Getenv("PARTLOG_BENCH_GOROUTINES"); path != "" {
				if f, err := os.Create(path); err == nil {
					_ = pprof.Lookup("goroutine").WriteTo(f, 2)
					_ = f.Close()
				}
			}
		}
	}
	emit("log_replay_ingest_complete", count.Load(), coreSample(l.engine, "seal", start, count.Load()))
	result = errors.Join(result, l.close())
	emit("log_replay_close_complete", count.Load(), map[string]any{"error": fmt.Sprint(result)})
	return result
}

// This exercises acceptance into the durable journal concurrently with ordered
// replay. Live rows have disjoint sequence numbers but the same metric catalogue.
func logReplayLive(root string, cfg coreConfig, opts partlog.Options, total, metrics uint64, emit func(string, uint64, map[string]any), start time.Time) error {
	l, err := openLoggedCore(filepath.Join(root, "log"), filepath.Join(root, "parts"), cfg, opts)
	if err != nil {
		return err
	}
	emit("log_recovery_scan_complete", 0, nil)
	replayStart := time.Now()
	ready := l.startReplay(context.Background())
	stop := make(chan struct{})
	done := make(chan error, cfg.Producers)
	var live atomic.Uint64
	liveStart := time.Now()
	const rate = 20000000
	for p := range cfg.Producers {
		go func() {
			const batch = 32768
			sent := uint64(0)
			for {
				select {
				case <-stop:
					done <- nil
					return
				default:
				}
				deadline := liveStart.Add(time.Duration(float64(sent) * float64(cfg.Producers) / rate * float64(time.Second)))
				if wait := time.Until(deadline); wait > 0 {
					timer := time.NewTimer(wait)
					select {
					case <-stop:
						timer.Stop()
						done <- nil
						return
					case <-timer.C:
					}
				}
				rows := make([]row, batch)
				for i := range rows {
					seq := total + (sent+uint64(i))*uint64(cfg.Producers) + uint64(p)
					rows[i] = row{seq % metrics, 1800000000 + int64(seq/rate), math.Float64bits(float64(seq % 100003)), seq}
				}
				if err := l.append(p, time.Now().Unix(), rows, false); err != nil {
					done <- err
					return
				}
				sent += batch
				live.Add(batch)
			}
		}()
	}
	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	running := true
	for running {
		select {
		case err = <-ready:
			running = false
		case <-tick.C:
			emit("concurrent_live_replay", live.Load(), coreSample(l.engine, "replay_live", start, live.Load()))
		}
	}
	backlogSeconds := time.Since(replayStart).Seconds()
	close(stop)
	for range cfg.Producers {
		err = errors.Join(err, <-done)
	}
	liveSeconds := time.Since(liveStart).Seconds()
	emit("backlog_snapshot_consumed", total, map[string]any{"backlog_seconds": backlogSeconds, "backlog_mpoints_per_second": float64(total) / backlogSeconds / 1e6, "live_accepted": live.Load(), "live_seconds": liveSeconds, "live_mpoints_per_second": float64(live.Load()) / liveSeconds / 1e6})
	err = errors.Join(err, l.stopReplay())
	deadline := time.Now().Add(5 * time.Second)
	settled := false
	quiet := 0
	for time.Now().Before(deadline) {
		busy := false
		for _, owner := range l.engine.owners {
			m := owner.db.Metrics()
			if m.Compact.EstimatedDebt > 0 || m.Compact.NumInProgress > 0 {
				busy = true
			}
		}
		if busy {
			quiet = 0
		} else {
			quiet++
		}
		if quiet >= 5 {
			settled = true
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	sample := coreSample(l.engine, "settled", start, total+live.Load())
	sample["settled"] = settled
	sample["live_accepted"] = live.Load()
	emit("replay_live_settle", total+live.Load(), sample)
	if !settled {
		err = errors.Join(err, errors.New("compaction did not settle"))
	}
	err = errors.Join(err, l.close())
	// All accepted live rows and backlog rows must pass the same core path.
	var committed uint64
	for _, owner := range l.engine.owners {
		committed += owner.committed.Load()
	}
	if committed != total+live.Load() {
		err = errors.Join(err, fmt.Errorf("committed %d != backlog+live %d", committed, total+live.Load()))
	}
	emit("replay_live_close_complete", committed, map[string]any{"error": fmt.Sprint(err), "backlog": total, "live_accepted": live.Load(), "whole_mpoints_per_second": float64(total+live.Load()) / time.Since(start).Seconds() / 1e6})
	return err
}
