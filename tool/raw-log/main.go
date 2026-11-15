// raw-log is a deliberately separate, opt-in experiment. It never opens an
// existing directory for writing and has no daemon/release integration.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/internal/rawlog"
	"github.com/go-graphite/go-carbon/internal/whisperio"
	"github.com/go-graphite/go-carbon/points"
	"github.com/go-graphite/go-carbon/receiver/stream/codec"
	"github.com/go-graphite/go-carbon/tags"
)

type reporter struct {
	mu      sync.Mutex
	encoder *json.Encoder
}

func (r *reporter) Encode(v any) error { r.mu.Lock(); defer r.mu.Unlock(); return r.encoder.Encode(v) }
func cpuSeconds() float64 {
	var r syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &r); err != nil {
		return math.NaN()
	}
	return float64(r.Utime.Sec+r.Stime.Sec) + float64(r.Utime.Usec+r.Stime.Usec)/1e6
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
func run() error {
	enabled := flag.Bool("experimental-raw-log", false, "required experiment opt-in")
	mode := flag.String("mode", "bench", "capture, bench or replay")
	root := flag.String("dir", "", "new private experiment directory (replay reads it)")
	rate := flag.Int64("rate", 1_000_000, "offered live points per second")
	seconds := flag.Int("seconds", 3, "capture duration")
	connections := flag.Int("connections", 280, "independent stream connections")
	workers := flag.Int("workers", runtime.GOMAXPROCS(0), "replay decode workers")
	pebble := flag.Bool("pebble", false, "apply replay to a private exact-block Pebble store")
	live := flag.Bool("live-replay", false, "run another capture epoch during replay")
	flag.Parse()
	if !*enabled {
		return errors.New("requires -experimental-raw-log")
	}
	if *root == "" || *seconds < 1 || *connections < 1 || *connections > 280 || *rate < int64(*connections)*10 || *rate > 100_000_000 {
		return errors.New("invalid private directory/rate/duration/connections")
	}
	emit := &reporter{encoder: json.NewEncoder(os.Stdout)}
	if *mode == "bench" || *mode == "capture" {
		if err := os.Mkdir(*root, 0700); err != nil {
			return err
		}
		w, err := rawlog.Create(*root)
		if err != nil {
			return err
		}
		defer w.Close()
		wires, err := gateWires(*connections, *rate)
		if err != nil {
			return err
		}
		if err = capture(w, wires, *rate, *seconds, emit, 0); err != nil {
			return err
		}
		if *mode == "capture" {
			return nil
		}
		if err = w.Rotate(); err != nil {
			return err
		}
		if *live {
			// The sealed first segment is replayed while a new segment accepts traffic.
			done := make(chan error, 1)
			var liveWindow atomic.Bool
			liveWindow.Store(true)
			go func() {
				e := capture(w, wires, *rate, *seconds, emit, uint64(*connections))
				liveWindow.Store(false)
				done <- e
			}()
			err = replay(*root, *workers, *pebble, emit, true, &liveWindow)
			return errors.Join(err, <-done)
		}
		return replay(*root, *workers, *pebble, emit, false, nil)
	}
	if *mode == "replay" {
		return replay(*root, *workers, *pebble, emit, false, nil)
	}
	return errors.New("mode must be capture, bench or replay")
}

// gateWires matches the strict gate's live input: 280 independent dictionaries,
// 256 tagged names/connection, eight IEEE bit patterns, reset every 100ms tick.
// This does not supply the gate's retained 268,936,651-point historical journal.
func gateWires(connections int, rate int64) ([][]byte, error) {
	wires := make([][]byte, connections)
	const stamp int64 = 1_800_000_000
	bits := []uint64{0, 1 << 63, 1, 0x3ff0000000000001, 0x7ff8000000000042, 0x7ff0000000000000, 0xfff0000000000000, 0x4000000000000001}
	for id := range wires {
		n := int(rate / 10 / int64(connections))
		if int64(id) < rate/10%int64(connections) {
			n++
		}
		e, err := codec.NewEncoder(codec.Options{})
		if err != nil {
			return nil, err
		}
		if err = e.Reset(); err != nil {
			e.Close()
			return nil, err
		}
		wire, err := codec.AppendPreamble(nil, 0)
		if err != nil {
			e.Close()
			return nil, err
		}
		for i := range n {
			name := fmt.Sprintf("load.c%03d.m%04d;a=first;z=last", id, i%256)
			if err = e.Add([]byte(name), math.Float64frombits(bits[i%len(bits)]), stamp+int64(i/256%8)); err != nil {
				e.Close()
				return nil, err
			}
		}
		wires[id], err = e.Flush(wire)
		e.Close()
		if err != nil {
			return nil, err
		}
	}
	return wires, nil
}

func capture(w *rawlog.Writer, wires [][]byte, rate int64, seconds int, emit *reporter, base uint64) error {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return err
	}
	defer listener.Close()
	var receivers, syncer sync.WaitGroup
	errs := make(chan error, len(wires)*2+2)
	var sockets []net.Conn
	for id := range wires {
		c, err := net.DialTimeout("tcp", listener.Addr().String(), 5*time.Second)
		if err != nil {
			return err
		}
		incoming, err := listener.Accept()
		if err != nil {
			c.Close()
			return err
		}
		sockets = append(sockets, c)
		receivers.Add(1)
		go func(id uint64, c net.Conn) {
			defer receivers.Done()
			defer c.Close()
			var pre [codec.PreambleSize]byte
			if _, err := io.ReadFull(c, pre[:]); err != nil {
				errs <- err
				return
			}
			flags, err := codec.ParsePreamble(pre[:])
			if err != nil {
				errs <- err
				return
			}
			if _, err = w.Begin(id, pre[:]); err != nil {
				errs <- err
				return
			}
			r, err := codec.NewWireReader(codec.Options{Flags: flags})
			if err != nil {
				errs <- err
				return
			}
			for {
				frame, _, err := r.Read(c)
				if err == io.EOF {
					return
				}
				if err != nil {
					errs <- err
					return
				}
				if _, err = w.Stage(id, frame, time.Now().UnixNano()); err != nil {
					errs <- err
					return
				}
			}
		}(base+uint64(id), incoming)
	}
	stop := make(chan struct{})
	syncer.Add(1)
	go func() {
		defer syncer.Done()
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
				if _, err := w.Sync(); err != nil {
					errs <- err
					return
				}
			}
		}
	}()
	start := time.Now().Add(100 * time.Millisecond)
	cpuStart := cpuSeconds()
	before := w.Stats()
	var senders sync.WaitGroup
	var sent atomic.Int64
	for id, c := range sockets {
		senders.Add(1)
		go func(id int, c net.Conn) {
			defer senders.Done()
			defer c.Close()
			for tick := range seconds * 10 {
				due := start.Add(time.Duration(tick) * 100 * time.Millisecond)
				if delay := time.Until(due); delay > 0 {
					time.Sleep(delay)
				}
				wire := wires[id]
				if tick > 0 {
					wire = wire[codec.PreambleSize:]
				}
				if err := c.SetWriteDeadline(due.Add(time.Second)); err != nil {
					errs <- err
					return
				}
				n, err := c.Write(wire)
				if err != nil {
					errs <- err
					return
				}
				if n != len(wire) {
					errs <- io.ErrShortWrite
					return
				}
				count := rate / 10 / int64(len(wires))
				if int64(id) < rate/10%int64(len(wires)) {
					count++
				}
				sent.Add(count)
			}
		}(id, c)
	}
	for second := 1; second <= seconds; second++ {
		if delay := time.Until(start.Add(time.Duration(second) * time.Second)); delay > 0 {
			time.Sleep(delay)
		}
		now := w.Stats()
		if err := emit.Encode(map[string]any{"phase": "capture_sample", "second": second, "points": now.Points - before.Points, "durable": now.DurablePoints - before.DurablePoints, "syncs": now.Syncs - before.Syncs}); err != nil {
			for _, c := range sockets {
				_ = c.Close()
			}
			return err
		}
	}
	senders.Wait()
	receivers.Wait()
	close(stop)
	syncer.Wait()
	_, syncErr := w.Sync()
	elapsed := time.Since(start)
	after := w.Stats()
	close(errs)
	for err := range errs {
		syncErr = errors.Join(syncErr, err)
	}
	row := map[string]any{"phase": "capture", "cpu_s": cpuSeconds() - cpuStart, "cpu_ns_point": (cpuSeconds() - cpuStart) * 1e9 / float64(after.Points-before.Points), "connections": len(wires), "seconds": elapsed.Seconds(), "offered": rate * int64(seconds), "sent": sent.Load(), "durable_points": after.DurablePoints - before.DurablePoints, "mpoints_s": float64(after.DurablePoints-before.DurablePoints) / elapsed.Seconds() / 1e6, "bytes_per_point": float64(after.Bytes-before.Bytes) / float64(after.Points-before.Points), "syncs": after.Syncs - before.Syncs, "sync_ms": float64(after.SyncTime-before.SyncTime) / float64(time.Millisecond), "synthetic": true}
	if err := emit.Encode(row); err != nil {
		return err
	}
	if after.DurablePoints-before.DurablePoints != uint64(rate*int64(seconds)) {
		return errors.Join(syncErr, errors.New("capture count mismatch"))
	}
	return syncErr
}

func replay(root string, workers int, usePebble bool, emit *reporter, sealedOnly bool, liveWindow *atomic.Bool) error {
	files, err := filepath.Glob(filepath.Join(root, "*.raw"))
	if err != nil {
		return err
	}
	sort.Strings(files)
	if sealedOnly && len(files) > 1 {
		files = files[:1]
	}
	var store *chunkstore.Store
	if usePebble {
		store, err = chunkstore.Open(filepath.Join(root, "store"), chunkstore.Options{ExactBlocks: true, Continuation: &whisperio.ContinuationCodec{}, TimeBuckets: true, CacheSize: 32 << 20, MemTableSize: 16 << 20, SyncInterval: time.Second, Now: func() time.Time { return time.Unix(1_800_000_100, 0) }})
		if err != nil {
			return err
		}
		defer store.Close()
	}
	names := make(map[string]bool)
	var updates []chunkstore.MetricUpdate
	var pending int
	var committed, committedWhileLive, decoded uint64
	start := time.Now()
	lastReport := start
	cpuStart := cpuSeconds()
	flush := func() error {
		if store != nil && len(updates) > 0 {
			if err := store.UpdateBatch(context.Background(), updates, nil); err != nil {
				return err
			}
		}
		committed += uint64(pending)
		if liveWindow != nil && liveWindow.Load() {
			committedWhileLive += uint64(pending)
		}
		updates = nil
		pending = 0
		if time.Since(lastReport) >= time.Second {
			live := liveWindow != nil && liveWindow.Load()
			if err := emit.Encode(map[string]any{"phase": "replay_sample", "seconds": time.Since(start).Seconds(), "committed": committed, "committed_while_live": committedWhileLive, "live": live}); err != nil {
				return err
			}
			lastReport = time.Now()
		}
		return nil
	}
	for _, path := range files {
		f, err := os.Open(path)
		if err != nil {
			return err
		}
		st, err := rawlog.Replay(f, rawlog.ReplayOptions{Workers: workers}, func(_ uint64, p *points.Points) error {
			decoded += uint64(len(p.Data))
			if store == nil {
				return nil
			}
			name, err := tags.Normalize(p.Metric)
			if err != nil {
				return err
			}
			if !names[name] {
				_, err = store.Create(context.Background(), chunkstore.MetricConfig{Name: name, Retentions: []chunkstore.Retention{{Step: 1, Count: 1000}}, AggregationMethod: chunkstore.Last})
				if err != nil {
					return err
				}
				names[name] = true
			}
			updates = append(updates, chunkstore.MetricUpdate{Name: name, Points: p.Data})
			pending += len(p.Data)
			if pending >= 65536 {
				return flush()
			}
			return nil
		})
		_ = f.Close()
		if err != nil {
			return err
		}
		_ = st
	}
	if err = flush(); err != nil {
		return err
	}
	if store != nil {
		if err = store.Sync(); err != nil {
			return err
		}
	}
	elapsed := time.Since(start)
	return emit.Encode(map[string]any{"phase": "replay", "cpu_s": cpuSeconds() - cpuStart, "cpu_ns_point": (cpuSeconds() - cpuStart) * 1e9 / float64(decoded), "seconds": elapsed.Seconds(), "decoded": decoded, "decode_mpoints_s": float64(decoded) / elapsed.Seconds() / 1e6, "pebble": store != nil, "committed": committed, "committed_while_live": committedWhileLive, "commit_mpoints_s": float64(committed) / elapsed.Seconds() / 1e6, "synthetic": true})
}
