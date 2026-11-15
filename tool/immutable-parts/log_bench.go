package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"runtime"
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
						seq := (sent+uint64(i))*uint64(producers) + uint64(p)
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
	from := l.engine.globalFrontier()
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
			emit("log_replay_sample", count.Load(), nil)
		}
	}
	emit("log_replay_ingest_complete", count.Load(), coreSample(l.engine, "seal", start, count.Load()))
	result = errors.Join(result, l.close())
	emit("log_replay_close_complete", count.Load(), map[string]any{"error": fmt.Sprint(result)})
	return result
}
