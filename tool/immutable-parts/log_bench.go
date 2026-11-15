package main

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
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

	"github.com/cockroachdb/pebble"
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
	opts := partlog.Options{Streams: producers, SegmentBytes: 64 << 20, SyncInterval: time.Second}
	if raw := os.Getenv("PARTLOG_BENCH_READAHEAD"); raw != "" {
		var err error
		opts.ReadAheadBytes, err = strconv.Atoi(raw)
		if err != nil {
			return err
		}
	}
	if phase == "verify" {
		return verifyLogBenchmark(root, total, metrics, producers, owners, emit)
	}
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
				storage := make([]row, batch)
				var payload []byte
				for sent := uint64(0); ; {
					first := sent*uint64(producers) + uint64(p)
					if first >= total {
						done <- nil
						return
					}
					n := min(uint64(batch), (total-first+uint64(producers)-1)/uint64(producers))
					rows := storage[:n]
					for i := range rows {
						seq := offset + (sent+uint64(i))*uint64(producers) + uint64(p)
						rows[i] = row{seq % metrics, 1800000000 + int64(seq/6000000), math.Float64bits(float64(seq % 100003)), seq}
					}
					payload = encodeLogRowsInto(payload, rows)
					if _, e := log.Append(p, 1800000000+int64(first/6000000), payload); e != nil {
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
	liveCounts := make([]atomic.Uint64, cfg.Producers)
	liveStart := time.Now()
	const rate = 20000000
	for p := range cfg.Producers {
		go func() {
			const batch = 32768
			rows := make([]row, batch)
			var payload []byte
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
				for i := range rows {
					seq := total + (sent+uint64(i))*uint64(cfg.Producers) + uint64(p)
					rows[i] = row{seq % metrics, 1800000000 + int64(seq/rate), math.Float64bits(float64(seq % 100003)), seq}
				}
				payload = encodeLogRowsInto(payload, rows)
				if _, err := l.journal.Append(p, time.Now().Unix(), payload); err != nil {
					done <- err
					return
				}
				sent += batch
				liveCounts[p].Store(sent)
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
	counts := make([]uint64, len(liveCounts))
	for p := range counts {
		counts[p] = liveCounts[p].Load()
	}
	if data, e := json.Marshal(counts); e != nil {
		err = errors.Join(err, e)
	} else {
		err = errors.Join(err, os.WriteFile(filepath.Join(root, "live-counts.json"), data, 0600))
	}
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
	sample["settled_replay_seconds"] = time.Since(replayStart).Seconds()
	sample["settled_replay_mpoints_per_second"] = float64(total) / time.Since(replayStart).Seconds() / 1e6
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

// Validate every persisted row against the generator and prove no duplicates
// with compact per-owner bitmaps. Membership + uniqueness + exact count proves
// complete coverage; SHA256 receipts cover all name IDs, timestamps and bits.
// This read-only phase is outside the replay throughput timing window.
func verifyLogBenchmark(root string, total, metrics uint64, producers, owners int, emit func(string, uint64, map[string]any)) error {
	raw, err := os.ReadFile(filepath.Join(root, "live-counts.json"))
	if err != nil {
		return err
	}
	var live []uint64
	if err = json.Unmarshal(raw, &live); err != nil {
		return err
	}
	if len(live) != producers {
		return os.ErrInvalid
	}
	want, maxSeq := total, total
	for p, n := range live {
		want += n
		if n > 0 {
			maxSeq = max(maxSeq, total+(n-1)*uint64(producers)+uint64(p))
		}
	}
	ranks := make([]uint32, metrics)
	counts := make([]uint64, owners)
	for id := range metrics {
		owner := coreShard(id, owners)
		ranks[id] = uint32(counts[owner])
		counts[owner]++
	}
	type receipt struct {
		Count          uint64
		Actual, Oracle string
		Err            error
	}
	done := make(chan receipt, owners)
	for owner := range owners {
		go func() {
			r := receipt{}
			defer func() { done <- r }()
			db, e := pebble.Open(filepath.Join(root, "parts", fmt.Sprintf("owner-%03d", owner)), &pebble.Options{ReadOnly: true})
			if e != nil {
				r.Err = e
				return
			}
			defer db.Close()
			it, e := db.NewIter(&pebble.IterOptions{LowerBound: []byte{1}, UpperBound: []byte{3}})
			if e != nil {
				r.Err = e
				return
			}
			defer it.Close()
			seen := make([]uint64, ((maxSeq/metrics+1)*counts[owner]+63)/64)
			actual, oracle := sha256.New(), sha256.New()
			for it.First(); it.Valid(); it.Next() {
				key, v := it.Key(), it.Value()
				if len(key) != 41 || (key[0] != 1 && key[0] != 2) || len(v)%24 != 0 {
					r.Err = errors.New("invalid persisted block")
					return
				}
				offset := 9
				if key[0] == 2 {
					offset = 17
				}
				id := binary.BigEndian.Uint64(key[offset:])
				if id >= metrics || coreShard(id, owners) != owner {
					r.Err = errors.New("invalid persisted ID or route")
					return
				}
				for len(v) > 0 {
					ts, bits, seq := int64(binary.LittleEndian.Uint64(v)), binary.LittleEndian.Uint64(v[8:]), binary.LittleEndian.Uint64(v[16:])
					v = v[24:]
					if seq >= total {
						index := seq - total
						if index/uint64(producers) >= live[index%uint64(producers)] {
							r.Err = errors.New("unexpected live sequence")
							return
						}
					}
					rate := uint64(6000000)
					if seq >= total {
						rate = 20000000
					}
					expected := row{seq % metrics, 1800000000 + int64(seq/rate), math.Float64bits(float64(seq % 100003)), seq}
					if id != expected.ID || ts != expected.Time || bits != expected.Bits {
						r.Err = fmt.Errorf("row bits differ seq%d", seq)
						return
					}
					index := seq/metrics*counts[owner] + uint64(ranks[id])
					mask := uint64(1) << (index % 64)
					if seen[index/64]&mask != 0 {
						r.Err = fmt.Errorf("duplicate seq%d", seq)
						return
					}
					seen[index/64] |= mask
					var a, b [32]byte
					putRow(a[:], row{id, ts, bits, seq})
					putRow(b[:], expected)
					actual.Write(a[:])
					oracle.Write(b[:])
					r.Count++
				}
			}
			r.Err = it.Error()
			r.Actual = fmt.Sprintf("%x", actual.Sum(nil))
			r.Oracle = fmt.Sprintf("%x", oracle.Sum(nil))
		}()
	}
	var count uint64
	digests := make([]receipt, 0, owners)
	for range owners {
		r := <-done
		err = errors.Join(err, r.Err)
		count += r.Count
		digests = append(digests, r)
		if r.Actual != r.Oracle {
			err = errors.Join(err, errors.New("digest mismatch"))
		}
	}
	if count != want {
		err = errors.Join(err, fmt.Errorf("count%d != expected%d", count, want))
	}
	emit("full_persisted_oracle", count, map[string]any{"expected": want, "digests": digests, "error": fmt.Sprint(err)})
	return err
}
