package carbon

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime/metrics"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-graphite/go-carbon/internal/handoff"
	"github.com/go-graphite/go-carbon/points"
	"github.com/go-graphite/go-carbon/receiver/tcp"
	"github.com/lomik/zapwriter"
	"go.uber.org/zap"
)

// Each metric has one appender, file and decoder. Complete-record frontiers
// are published after write(2), and each file is fsynced periodically and at seal.
// Files use the ordinary legacy recovery format and sort after the predecessor
// tail. Process crashes recover every complete written record. TCP ACK itself
// is not a durability acknowledgement: unread kernel bytes and unsynced data on
// power loss have the same limitations as ordinary TCP input.
type startupShard struct {
	file     *os.File
	path     string
	err      atomic.Pointer[startupWriteError]
	requests chan startupAppend
	written  chan struct{}
	size     atomic.Int64
	offset   atomic.Int64
	owner    *startupInput
	unlock   func() error
}

type startupInput struct {
	file           *os.File // immutable predecessor tail; retained for legacy recovery tests
	path           string
	policy         startupCapturePolicy
	legacy         *startupShard
	shards         []*startupShard
	size           atomic.Int64
	points         atomic.Int64
	replayedPoints atomic.Int64
	receiver       *tcp.TCP
	observe        func(*points.Points) // installed before handlers; tests only
	started        time.Time
	stop           chan struct{}
	done           chan struct{}
	sealed         chan struct{}
	catching       atomic.Bool
	fallback       atomic.Bool
	writeRate      atomic.Int64
	replayRate     atomic.Int64
	idleCPUs       atomic.Int64
	effectiveLimit atomic.Int64
	effectiveBytes atomic.Int64
	pressure       chan struct{}
	inherited      *tcp.Handoff // published by closing sealed
	sealErr        error
	sealedAt       time.Time
	monitored      bool
	syncWG         sync.WaitGroup
	syncing        atomic.Bool
	closeOnce      sync.Once
	closeErr       error
}

func openStartupInput(path string) (*startupInput, error) {
	return openStartupInputWithPolicy(path, startupPolicy(path))
}
func openStartupInputWithPolicy(path string, policy startupCapturePolicy) (_ *startupInput, err error) {
	s := &startupInput{policy: policy, path: path, started: time.Now(), stop: make(chan struct{}), done: make(chan struct{}), sealed: make(chan struct{}), pressure: make(chan struct{}, 1)}
	open := func(path string, flags int) (*startupShard, error) {
		f, err := os.OpenFile(path, flags, 0600)
		if err != nil {
			return nil, err
		}
		unlock, err := handoff.LockExclusive(path)
		if err != nil {
			_ = f.Close()
			return nil, err
		}
		info, err := f.Stat()
		if err != nil {
			_ = f.Close()
			_ = unlock()
			return nil, err
		}
		shard := &startupShard{file: f, path: path, owner: s, unlock: unlock, requests: make(chan startupAppend, 256), written: make(chan struct{})}
		go shard.appendLoop()
		shard.size.Store(info.Size())
		s.size.Add(info.Size())
		return shard, nil
	}
	s.legacy, err = open(path, os.O_RDWR|os.O_APPEND)
	if err != nil {
		return nil, err
	}
	s.file = s.legacy.file
	s.replayRate.Store(s.policy.replayRate)
	defer func() {
		if err != nil {
			_ = s.close()
		}
	}()
	for i := range s.policy.shards {
		shard, e := open(fmt.Sprintf("%s.capture-%02d.tail.bin", path, i), os.O_RDWR|os.O_CREATE|os.O_EXCL|os.O_APPEND)
		if e != nil {
			return nil, e
		}
		s.shards = append(s.shards, shard)
	}
	// Directory entries must survive a crash too, not only the file contents.
	if err = syncDir(filepath.Dir(path)); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *startupInput) CaptureShards() []tcp.Capture {
	out := make([]tcp.Capture, len(s.shards))
	for i, v := range s.shards {
		out[i] = v
	}
	return out
}

// Write is the single-stream compatibility sink used by direct legacy tests.
// The TCP receiver always uses CaptureShards instead.
func (s *startupInput) Write(data []byte, count int) error { return s.legacy.Write(data, count) }

type startupWriteError struct{ err error }
type startupAppend struct {
	data  []byte
	count int
	done  chan error
}

// Each file has exactly one owner. Producers enqueue borrowed batches and wait
// for write completion. No application mutex covers file I/O, and replay uses
// only the atomic complete-record frontier. Opportunistic coalescing amortizes
// writes from many connections without a timer or an unbounded queue.
func (s *startupShard) BeginWrite(data []byte, count int) func() error {
	if failure := s.err.Load(); failure != nil {
		return func() error { return failure.err }
	}
	done := make(chan error, 1)
	s.requests <- startupAppend{data, count, done}
	return func() error { return <-done }
}
func (s *startupShard) Write(data []byte, count int) error { return s.BeginWrite(data, count)() }
func (s *startupShard) appendLoop() {
	defer close(s.written)
	var buf []byte
	pending := make([]startupAppend, 0, 256)
	for first := range s.requests {
		pending = append(pending[:0], first)
		buf = append(buf[:0], first.data...)
		count := first.count
	drain:
		for len(buf) < s.owner.policy.writeBuffer {
			select {
			case next, ok := <-s.requests:
				if !ok {
					break drain
				}
				pending = append(pending, next)
				buf = append(buf, next.data...)
				count += next.count
			default:
				break drain
			}
		}
		var err error
		if failure := s.err.Load(); failure != nil {
			err = failure.err
		} else {
			n, e := s.file.Write(buf)
			err = e
			if err == nil && n != len(buf) {
				err = io.ErrShortWrite
			}
			if err == nil {
				s.size.Add(int64(n))
				s.owner.size.Add(int64(n))
				s.owner.points.Add(int64(count))
				if s.owner.observe != nil {
					err = points.ReadBinary(bytes.NewReader(buf), s.owner.observe)
				}
			}
			if err != nil {
				s.err.CompareAndSwap(nil, &startupWriteError{err})
			}
		}
		for i := range pending {
			pending[i].done <- err
			pending[i] = startupAppend{}
		}
	}
}
func (s *startupShard) sync() error {
	if err := s.file.Sync(); err != nil {
		s.err.CompareAndSwap(nil, &startupWriteError{err})
	}
	if failure := s.err.Load(); failure != nil {
		return failure.err
	}
	return nil
}

func (s *startupInput) requestFallback() {
	select {
	case s.pressure <- struct{}{}:
	default:
	}
}

// This monitor runs even while Start holds the lifecycle lock opening storage.
// Detach stops socket reads and flushes all connection-local buffers. Holding
// inherited descriptors preserves the ordinary ordered drain fallback.
func (s *startupInput) startMonitor(limit time.Duration, maxBytes int64) {
	s.monitored = true
	s.effectiveLimit.Store(int64(limit))
	s.effectiveBytes.Store(maxBytes)
	go func() {
		defer close(s.done)
		tick := time.NewTicker(10 * time.Millisecond)
		defer tick.Stop()
		syncTick := time.NewTicker(time.Second)
		lastSize := s.size.Load()
		lastSample := time.Now()
		cpuSamples := []metrics.Sample{{Name: "/cpu/classes/idle:cpu-seconds"}}
		metrics.Read(cpuSamples)
		lastIdle := cpuSamples[0].Value.Float64()
		defer syncTick.Stop()
		seal := func(reason string) {
			if reason != "" {
				s.fallback.Store(true)
				zapwriter.Logger("app").Error("startup capture limit reached; falling back to ordered input drain", zap.String("reason", reason), zap.Int64("bytes", s.size.Load()), zap.Duration("runtime", time.Since(s.started)))
			}
			s.sealedAt = time.Now()
			s.inherited, s.sealErr = s.receiver.Detach()
			var wg sync.WaitGroup
			errs := make([]error, len(s.shards))
			for i, shard := range s.shards {
				wg.Go(func() { errs[i] = shard.sync() })
			}
			wg.Wait()
			s.sealErr = errors.Join(append(errs, s.sealErr)...)
			close(s.sealed)
		}
		for {
			select {
			case <-s.stop:
				return
			case <-s.pressure:
				seal("recovery_budget_or_reader_error")
				return
			case <-syncTick.C:
				now := time.Now()
				size := s.size.Load()
				rate := int64(float64(size-lastSize) / now.Sub(lastSample).Seconds())
				s.writeRate.Store(rate)
				elapsed := now.Sub(lastSample).Seconds()
				metrics.Read(cpuSamples)
				idle := max(0, (cpuSamples[0].Value.Float64()-lastIdle)/elapsed)
				lastIdle = cpuSamples[0].Value.Float64()
				s.idleCPUs.Store(int64(idle * 1000))
				// Reclaim unused CPU for replay without competing at startup
				// with the receiver's line-rate work. Raise only on observed
				// idle capacity; a full CPU is a reason to keep the reservation.
				if s.catching.Load() {
					current := s.replayRate.Load()
					if idle > float64(s.policy.cpus)/5 {
						s.replayRate.Store(min(int64(s.policy.cpus)*8000000, current*5/4))
					} else if idle < float64(s.policy.cpus)/20 && current > s.policy.replayRate {
						s.replayRate.Store(max(s.policy.replayRate, current*4/5))
					}
				}
				lastSize, lastSample = size, now
				// Test monitors can set stricter bounds; only the production policy adapts.
				if limit == s.policy.limit && maxBytes == s.policy.spoolBytes {
					s.effectiveLimit.Store(int64(captureTimeLimit(maxBytes, rate)))
				}
				// A slow fsync must not stop the independent safety monitor.
				if s.syncing.CompareAndSwap(false, true) {
					s.syncWG.Go(func() {
						defer s.syncing.Store(false)
						var wg sync.WaitGroup
						for _, shard := range s.shards {
							wg.Go(func() {
								if shard.sync() != nil {
									s.requestFallback()
								}
							})
						}
						wg.Wait()
					})
				}
				if limit == s.policy.limit && maxBytes == s.policy.spoolBytes {
					if free, err := migrationDiskFree(filepath.Dir(s.path)); err == nil {
						s.effectiveBytes.Store(min(maxBytes, size+int64(min(free, 1<<60))/8))
					}
				}
			case <-tick.C:
				switch {
				case time.Since(s.started) >= time.Duration(s.effectiveLimit.Load()):
					seal("duration")
					return
				case s.size.Load() >= s.effectiveBytes.Load():
					seal("bytes")
					return
				case s.catching.Load():
					var left int64
					for _, shard := range s.shards {
						left += shard.size.Load() - shard.offset.Load()
					}
					if left <= 1<<20 {
						seal("")
						return
					}
				}
			}
		}
	}()
}

func (s *startupInput) close() error {
	s.closeOnce.Do(func() {
		close(s.stop)
		if s.monitored {
			<-s.done
		}
		s.syncWG.Wait()
		var wg sync.WaitGroup
		shards := append([]*startupShard{s.legacy}, s.shards...)
		errs := make([]error, len(shards))
		for i, shard := range shards {
			wg.Go(func() {
				close(shard.requests)
				<-shard.written
				errs[i] = errors.Join(shard.sync(), shard.file.Close(), shard.unlock())
			})
		}
		wg.Wait()
		s.closeErr = errors.Join(errs...)
	})
	return s.closeErr
}

type startupShardReader struct {
	s     *startupInput
	shard *startupShard
}

func (r *startupShardReader) Read(buf []byte) (int, error) {
	if len(buf) == 0 {
		return 0, nil
	}
	for {
		select {
		case <-r.s.stop:
			return 0, errors.New("startup capture stopped")
		default:
		}
		left := r.shard.size.Load() - r.shard.offset.Load()
		if left > 0 {
			n, err := r.shard.file.ReadAt(buf[:min(int64(len(buf)), left)], r.shard.offset.Load())
			r.shard.offset.Add(int64(n))
			return n, err
		}
		select {
		case <-r.s.sealed:
			// Detach's final flush precedes sealed. Recheck its frontier.
			if r.shard.size.Load() == r.shard.offset.Load() {
				return 0, io.EOF
			}
		case <-r.s.stop:
			return 0, errors.New("startup capture stopped")
		default:
			time.Sleep(time.Millisecond)
		}
	}
}

// One decoder adds each shard directly, without a shared dispatch goroutine.
// A bounded canonical-name memo keeps borrowed names safe if cache retains them.
func replayCaptureShard(reader io.Reader, add func(*points.Points)) (int, error) {
	return replayCaptureShardBuffer(reader, add, 1<<20)
}
func replayCaptureShardBuffer(reader io.Reader, add func(*points.Points), buffer int) (int, error) {
	memo := make(map[string]string)
	retained, count := 0, 0
	var p points.Points
	err := points.ReadBinaryBorrowedSize(reader, buffer, func(name []byte, data []points.Point) {
		canonical, ok := memo[string(name)]
		if !ok {
			if retained+len(name) > 4<<20 || len(memo) >= 4096 {
				clear(memo)
				retained = 0
			}
			canonical = string(name)
			memo[canonical] = canonical
			retained += len(canonical)
		}
		p.Metric, p.Data = canonical, data
		add(&p)
		count += len(data)
	})
	return count, err
}

// Rate-limit only the CPU-heavy replay sink, independently for each shard.
// Sleeping occurs outside cache/file ownership. Available idle CPU raises the
// shared target; capture never waits for a replay permit or a recovery lock.
func (s *startupInput) pacedReplay(add func(*points.Points)) func(*points.Points) {
	start := time.Now()
	var count int64
	nextCheck := int64(4096)
	return func(p *points.Points) {
		add(p)
		count += int64(len(p.Data))
		if count >= nextCheck {
			select {
			case <-s.sealed:
				return
			default:
			}
			nextCheck = count + 4096
			target := s.replayRate.Load() / int64(len(s.shards))
			if target > 0 {
				delay := time.Until(start.Add(time.Duration(float64(count) / float64(target) * float64(time.Second))))
				if delay > 0 {
					time.Sleep(min(delay, 10*time.Millisecond))
				}
			}
			// A bounded window avoids retaining catch-up credit after an I/O stall.
			if time.Since(start) >= 100*time.Millisecond {
				start = time.Now()
				count = 0
				nextCheck = 4096
			}
		}
	}
}
