package carbon

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/go-graphite/go-carbon/cache"
	"github.com/go-graphite/go-carbon/helper"
	"github.com/go-graphite/go-carbon/internal/handoff"
	"github.com/go-graphite/go-carbon/internal/recovery"
	"github.com/go-graphite/go-carbon/points"
	"github.com/go-graphite/go-carbon/receiver/tcp"
	"github.com/lomik/zapwriter"
)

// inputHandoffTimeout bounds how long a stopping instance keeps appending
// input to its tail log for a successor that has not asked for input yet, for
// example one restoring without a checkpoint. Input then stops as without
// handoff, and the successor replays the tail before opening its own input.
const inputHandoffTimeout = 20 * time.Second

// switchWriter lets diverted input move from one log to another. When Switch
// returns, no write to the previous log is in flight, so it can be closed.
type switchWriter struct {
	mu    sync.RWMutex
	write func(*points.Points) error
}

func (s *switchWriter) Write(p *points.Points) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.write(p)
}

func (s *switchWriter) Switch(write func(*points.Points) error) {
	s.mu.Lock()
	s.write = write
	s.mu.Unlock()
}

// handoffReceiver is a receiver whose connections can move to the successor.
type handoffReceiver interface {
	Handoffable() bool
	Detach() (*tcp.Handoff, error)
}

// inputHandoffPossible reports whether every receiver can hand its input over.
// Assumes we are holding app.Lock().
func (app *App) inputHandoffPossible() bool {
	// Only the built-in plaintext receiver has an inheritance path. A mixed
	// or custom configuration must use the ordinary stop/rebind path.
	if len(app.Receivers) != 1 || app.Receivers[0].Name != "tcp" || !app.Config.Tcp.Enabled {
		return false
	}
	for _, r := range app.Receivers {
		if h, ok := r.Receiver.(handoffReceiver); !ok || !h.Handoffable() {
			return false
		}
	}
	return true
}

// inputHandover keeps a stopping instance's input open after its checkpoint,
// appending to a tail log, until the successor takes the input sockets or
// input stops. It implements handoff.InputSource.
type inputHandover struct {
	receivers   []*NamedReceiver
	flushTraces func()
	logger      *zap.Logger
	started     time.Time

	mu     sync.Mutex
	done   bool
	tail   *recovery.Writer
	path   string
	unlock func() error
	timer  *time.Timer
}

// startInputHandover moves diverted input from the checkpoint's WAL to a new
// tail log at path. The tail stays exclusively locked until it is complete.
// Assumes we are holding app.Lock(); the handover owns the receivers.
func (app *App) startInputHandover(path string, divert *switchWriter, logger *zap.Logger) (*inputHandover, error) {
	tail, err := recovery.NewWriter(path, 1, 4096, nil)
	if err != nil {
		return nil, err
	}
	unlock, err := handoff.LockExclusive(path)
	if err != nil {
		_, _ = tail.Close()
		_ = os.Remove(path)
		return nil, err
	}
	h := &inputHandover{receivers: app.Receivers, flushTraces: app.FlushTraces, logger: logger, started: time.Now(), tail: tail, path: path, unlock: unlock}
	app.Receivers = nil
	divert.Switch(tail.WritePoints)
	h.timer = time.AfterFunc(inputHandoffTimeout, func() { h.stop("successor did not take input in time") })
	return h, nil
}

// Detach stops reading input, completes the tail, and returns the sockets. It
// returns nil once input stopped.
func (h *inputHandover) Detach() (*handoff.Input, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.done {
		return nil, nil
	}
	h.done = true
	h.timer.Stop()
	in := &handoff.Input{}
	conns := 0
	for _, r := range h.receivers {
		detached, err := r.Receiver.(handoffReceiver).Detach()
		if err != nil {
			h.logger.Warn("receiver input not handed over", zap.String("receiver", r.Name), zap.Error(err))
			r.Stop()
			continue
		}
		ir := handoff.InputReceiver{Name: r.Name, Listener: detached.Listener}
		for _, c := range detached.Conns {
			ir.Conns = append(ir.Conns, handoff.InputConn{File: c.File, Leftover: c.Leftover})
		}
		conns += len(ir.Conns)
		in.Receivers = append(in.Receivers, ir)
	}
	// The successor replays the tail before reading any handed-over socket.
	if err := h.completeTail(); err != nil {
		in.Close()
		return nil, err
	}
	h.logger.Info("input handed over", zap.Int("connections", conns), zap.Duration("tail_for", time.Since(h.started)))
	if len(in.Receivers) == 0 {
		return nil, nil
	}
	return in, nil
}

// stop closes input as a stop without handoff would, if it is still open.
func (h *inputHandover) stop(reason string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.done {
		return
	}
	h.done = true
	h.timer.Stop()
	for _, r := range h.receivers {
		r.Stop()
	}
	if err := h.completeTail(); err != nil {
		h.logger.Error("input tail incomplete", zap.Error(err))
	}
	h.logger.Info("input stopped", zap.String("reason", reason), zap.Duration("tail_for", time.Since(h.started)))
}

// completeTail runs once no receiver writes anymore.
func (h *inputHandover) completeTail() error {
	if h.flushTraces != nil {
		h.flushTraces()
	}
	_, err := h.tail.Close()
	err = errors.Join(err, syncDir(filepath.Dir(h.path)))
	return errors.Join(err, h.unlock())
}

func syncDir(path string) error {
	dir, err := os.Open(path)
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}

// loadInputTails replays input a stopping instance appended after its
// checkpoint. It runs before input opens, so tail points reach the cache
// before newer points of the same metric. A tail still being appended to is
// waited for: its writer completes it before handing input over or stopping.
func (app *App) loadInputTails(core *cache.Cache) error {
	dir := app.Config.Dump.Path
	if !app.Config.Dump.Enabled || dir == "" {
		return nil
	}
	entries, err := os.ReadDir(dir)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	var names []string
	for _, e := range entries {
		if !e.IsDir() && recovery.IsTail(e.Name()) {
			names = append(names, e.Name())
		}
	}
	slices.Sort(names)
	logger := zapwriter.Logger("restore")
	for _, name := range names {
		path := filepath.Join(dir, name)
		started := time.Now()
		release, err := handoff.LockShared(path)
		if err != nil {
			return fmt.Errorf("lock input tail %s: %w", path, err)
		}
		waited := time.Since(started)
		workers := min(runtime.GOMAXPROCS(0), 32)
		if app.Config.Tags.Enabled {
			// Different raw tag orders may normalize to the same cache key.
			workers = 1
		}
		count, err := replayInputTail(path, workers, func(p *points.Points) {
			// These points were accepted before the checkpoint. Never apply
			// live quotas again or discard them because recovery filled cache.
			// Reserve space before admitting another record; a single metric
			// and its checkpoint history may exceed the configured limit.
			for !core.HasRecoveryCapacity(len(p.Data)) {
				time.Sleep(time.Millisecond)
			}
			core.AddRecovered(p)
		})
		_ = release()
		logger.Info("input tail loaded", zap.String("filename", path), zap.Int("points", count), zap.Int("workers", workers),
			zap.Duration("lock_wait", waited), zap.Duration("runtime", time.Since(started)), zap.Error(err))
		if err != nil {
			return fmt.Errorf("read input tail %s: %w", path, err)
		}
		if app.pendingDone != nil {
			// The checkpoint owns this generation's tail too. Its confirmation
			// tracking retires both only once every recovered point is on disk.
			continue
		}
		for !core.IsEmpty() {
			time.Sleep(time.Millisecond)
		}
		if err = os.Remove(path); err != nil {
			return err
		}
	}
	if len(names) > 0 {
		return syncDir(dir)
	}
	return nil
}

// takeInputReceiver removes the named receiver's input from in.
func takeInputReceiver(in *handoff.Input, name string) *tcp.Handoff {
	if in == nil {
		return nil
	}
	for i, r := range in.Receivers {
		if r.Name != name {
			continue
		}
		in.Receivers = slices.Delete(in.Receivers, i, i+1)
		h := &tcp.Handoff{Listener: r.Listener}
		for _, c := range r.Conns {
			h.Conns = append(h.Conns, tcp.Conn{File: c.File, Leftover: c.Leftover})
		}
		return h
	}
	return nil
}

// replayInputTail overlaps checkpoint claims and cache writes for independent
// metrics. A metric always uses one FIFO worker, so corrections retain their
// on-disk order. Wait for every worker even on a read error: callers must not
// release the tail lock or open live input while recovered writes are in flight.
func replayInputTail(path string, workers int, add func(*points.Points)) (int, error) {
	count := 0
	if workers <= 1 {
		err := points.ReadFromFile(path, func(p *points.Points) {
			count += len(p.Data)
			add(p)
		})
		return count, err
	}
	queues := make([]chan *points.Points, workers)
	var wg sync.WaitGroup
	for i := range queues {
		// Bound read-ahead independently of the tail's size.
		queue := make(chan *points.Points, 32)
		queues[i] = queue
		wg.Go(func() {
			for p := range queue {
				add(p)
			}
		})
	}
	err := points.ReadFromFile(path, func(p *points.Points) {
		count += len(p.Data)
		// Hash the owned string directly. With a power-of-two worker count,
		// this also keeps each cache/confirmation shard on a single worker.
		queues[helper.HashString(p.Metric)%uint64(workers)] <- p
	})
	for _, queue := range queues {
		close(queue)
	}
	wg.Wait()
	return count, err
}
