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

	"github.com/go-graphite/go-carbon/cache"
	"github.com/go-graphite/go-carbon/internal/handoff"
	"github.com/go-graphite/go-carbon/internal/recovery"
	"github.com/go-graphite/go-carbon/points"
	"github.com/go-graphite/go-carbon/receiver/tcp"
	"github.com/klauspost/compress/zstd"
	"github.com/lomik/zapwriter"
	"go.uber.org/zap"
)

// One reader applies each metric-hash FIFO, including after it catches up.
// The append owner only writes frames, so cache admission and application
// cannot hold acknowledgements to the plaintext TCP receiver.
type startupShard struct {
	owner        *startupInput
	file         *os.File // first segment; compatibility with crash fixtures
	path         string
	requests     chan startupAppend
	written      chan struct{}
	pending      atomic.Int64 // queued, blocked submitters and the batch being encoded/written
	err          atomic.Pointer[startupWriteError]
	size, offset atomic.Int64
	first        atomic.Pointer[startupSegment]
	current      atomic.Pointer[startupSegment]
	applied      atomic.Pointer[startupPosition]
	live         atomic.Bool
	promoted     atomic.Uint64
	stream       cache.RecoveryStream
	names        map[string]string
	nameBytes    int
	timings      [captureIOCount]captureTiming
}
type startupSegment struct {
	file                      *os.File
	path                      string
	unlock                    func() error
	source                    *cache.RecoverySource
	next                      atomic.Pointer[startupSegment]
	size                      atomic.Int64
	sealed, replayed, retired atomic.Bool
}
type startupPosition struct {
	segment *startupSegment
	offset  int64
}
type startupWriteError struct{ err error }
type startupAppend struct {
	data     []byte
	count    int
	done     chan error
	promote  *startupPosition
	promoted chan bool
	fenced   chan error
	seal     bool
}

// RecoveryAdmission is deliberately structural: adaptive-caps owns allocation
// policy and blocked-pressure publication. Capture only asks for a batch grant.
type captureAdmission interface {
	HasRecoveryCapacityBatch(int, int64) bool
	RecoveryMetricBytes([]byte) int64
}
type startupSink struct {
	admission   captureAdmission
	add         func(*points.Points, *cache.RecoverySource)
	liveAdd     func(*points.Points, *cache.RecoverySource)
	canRetire   func() bool
	syncHistory func() error
	syncStore   func() error
	directAdd   func(*points.Points)
	startLog    func() (func() error, error)
	drain       func(bool)
}
type startupInput struct {
	file                                                                *os.File
	path                                                                string
	policy                                                              startupCapturePolicy
	legacy                                                              *startupShard
	shards                                                              []*startupShard
	size, rawBytes, points, replayedPoints, replayedBytes, retiredBytes atomic.Int64
	receiver                                                            *tcp.TCP
	observe                                                             func(*points.Points)
	started                                                             time.Time
	stop, done, sealed                                                  chan struct{}
	catching, fallback, paused, continued                               atomic.Bool
	writeRate, replayRate, idleCPUs, effectiveLimit, effectiveBytes     atomic.Int64
	pauseExpected                                                       atomic.Int64
	pressure                                                            chan struct{}
	inherited                                                           *tcp.Handoff
	sealErr                                                             error
	sealedAt                                                            time.Time
	monitored                                                           atomic.Bool
	syncWG                                                              sync.WaitGroup
	syncing                                                             atomic.Bool
	closeOnce                                                           sync.Once
	stopOnce                                                            sync.Once
	closeErr                                                            error
	sink                                                                atomic.Pointer[startupSink]
	replayStop, replayDone, legacyDone                                  chan struct{}
	replayOnce                                                          sync.Once
	replayStarted                                                       atomic.Bool
	historyReady                                                        atomic.Bool
	historyLog                                                          atomic.Pointer[startupSegment]
	replayYields                                                        atomic.Int64
	historyPoints                                                       atomic.Int64
	epoch                                                               int64
	direct                                                              atomic.Bool
	transition                                                          sync.Mutex
	switchStopped                                                       bool
	disabledWarned                                                      bool
	liveSince, refusedAt                                                time.Time
	livePromotions                                                      uint64
	directFence                                                         *startupSegment
	releaseLog                                                          func() error
	preparingDirect                                                     bool                 // transition owns a new journal pin, not an older epoch's pin
	fileSync                                                            func(*os.File) error // nil uses File.Sync; injected by slow-I/O tests before replay
}

func openStartupInput(path string) (*startupInput, error) {
	return openStartupInputWithPolicy(path, startupPolicy(path))
}
func openStartupInputWithPolicy(path string, p startupCapturePolicy) (_ *startupInput, err error) {
	s := &startupInput{path: path, policy: p, started: time.Now(), stop: make(chan struct{}), done: make(chan struct{}), sealed: make(chan struct{}), pressure: make(chan struct{}, 1), replayStop: make(chan struct{}), replayDone: make(chan struct{}), legacyDone: make(chan struct{}), epoch: time.Now().UnixNano()}

	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0600)
	if err != nil {
		return nil, err
	}
	unlock, err := handoff.LockExclusive(path)
	if err != nil {
		f.Close()
		return nil, err
	}
	info, err := f.Stat()
	if err != nil {
		f.Close()
		unlock()
		return nil, err
	}
	s.file = f
	s.legacy = &startupShard{owner: s, file: f, path: path}
	s.legacy.size.Store(info.Size())
	s.size.Store(info.Size())
	legacy := &startupSegment{file: f, path: path, unlock: unlock}
	legacy.size.Store(info.Size())
	s.legacy.first.Store(legacy)
	s.legacy.current.Store(legacy)
	defer func() {
		if err != nil {
			_ = s.close()
		}
	}()
	for i := range p.shards {
		sh := &startupShard{owner: s, requests: make(chan startupAppend, 256), written: make(chan struct{})}
		sh.path = fmt.Sprintf("%s.capture-epoch-%020d.%06d", path, s.epoch, i)
		seg, e := sh.newSegment(0, false)
		if e != nil {
			return nil, e
		}
		sh.first.Store(seg)
		sh.file = seg.file
		sh.current.Store(seg)
		sh.applied.Store(&startupPosition{seg, 0})
		s.shards = append(s.shards, sh)
		go sh.appendLoop()
	}
	if err = syncDir(filepath.Dir(path)); err != nil {
		return nil, err
	}
	return s, nil
}
func (s *startupShard) newSegment(index uint64, syncDirectory bool) (*startupSegment, error) {
	path := fmt.Sprintf("%s.%012d.tail.bin", s.path, index)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	unlock, err := handoff.LockExclusive(path)
	if err != nil {
		f.Close()
		return nil, err
	}
	// Sync the directory before publishing/acknowledging any record in this file.
	if syncDirectory {
		if err = syncDir(filepath.Dir(path)); err != nil {
			f.Close()
			unlock()
			return nil, err
		}
	}
	return &startupSegment{file: f, path: path, unlock: unlock, source: s.stream.Next()}, nil
}
func (s *startupInput) CaptureShards() []tcp.Capture {
	out := make([]tcp.Capture, len(s.shards))
	for i, sh := range s.shards {
		out[i] = sh
	}
	return out
}

// Direct legacy writes are used only by immutable-tail compatibility fixtures.
func (s *startupInput) Write(raw []byte, n int) error {
	written, err := s.file.Write(raw)
	if err == nil && written != len(raw) {
		err = io.ErrShortWrite
	}
	if err == nil {
		s.legacy.size.Add(int64(written))
		s.legacy.first.Load().size.Add(int64(written))
		s.size.Add(int64(written))
		s.rawBytes.Add(int64(written))
		s.points.Add(int64(n))
	}
	return err
}
func (s *startupShard) SubmitCapture(raw []byte, n int, done chan error) {
	if failure := s.err.Load(); failure != nil {
		done <- failure.err
		return
	}
	s.pending.Add(1)
	s.requests <- startupAppend{data: raw, count: n, done: done}
}
func (s *startupShard) BeginWrite(raw []byte, n int) func() error {
	done := make(chan error, 1)
	s.SubmitCapture(raw, n, done)
	return func() error { return <-done }
}

func (s *startupShard) Write(raw []byte, n int) error { return s.BeginWrite(raw, n)() }
func (s *startupShard) fail(err error) {
	if err != nil {
		s.err.CompareAndSwap(nil, &startupWriteError{err})
		s.owner.requestFallback()
	}
}

// A socket flush fans out to the capture queues before it can acknowledge its
// input. Let append owners clear a burst before replay consumes more CPU/cache
// capacity. The per-shard request count includes encoding/writes and blocked
// submitters; an empty channel alone can hide a stalled coalesced batch.
func (s *startupInput) waitCapture() error {
	for {
		// At the disk reserve capture needs replay/persistence to retire sources.
		// Yielding to its blocked writer would create a circular wait.
		if s.paused.Load() {
			return nil
		}
		busy := false
		for _, shard := range s.shards {
			if shard.pending.Load() > 4 {
				busy = true
				break
			}
		}
		if !busy {
			return nil
		}
		s.replayYields.Add(1)
		select {
		case <-s.stop:
			return errStartupReplayStopped
		case <-s.replayStop:
			return errStartupReplayStopped
		case <-time.After(time.Millisecond):
		}
	}
}

func (s *startupShard) appendLoop() {
	defer close(s.written)
	enc, _ := zstd.NewWriter(nil, zstd.WithEncoderConcurrency(1), zstd.WithEncoderLevel(zstd.SpeedFastest), zstd.WithEncoderCRC(false))
	defer enc.Close()
	var buf, frame []byte
	var held startupAppend
	haveHeld := false
	var index uint64
	pending := make([]startupAppend, 0, 256)
	for {
		var first startupAppend
		var ok bool
		if haveHeld {
			first = held
			haveHeld = false
			ok = true
		} else {
			first, ok = <-s.requests
		}
		if !ok {
			break
		}
		if first.fenced != nil {
			cur, applied := s.current.Load(), s.applied.Load()
			var err error
			if failure := s.err.Load(); failure != nil {
				err = failure.err
			} else if !s.live.Load() || applied.segment != cur || applied.offset != cur.size.Load() {
				err = errors.New("partition is behind its append frontier")
			} else if first.seal {
				err = s.syncFile(cur.file)
				if err == nil {
					cur.replayed.Store(true)
					cur.sealed.Store(true)
				}
				s.fail(err)
			}
			first.fenced <- err
			continue
		}
		if first.promoted != nil {
			cur := s.current.Load()
			applied := s.applied.Load()
			promote := s.owner.sink.Load() != nil && applied.segment == cur && applied.offset == cur.size.Load() && first.promote != nil && first.promote.segment == cur && first.promote.offset == applied.offset
			if promote {
				s.live.Store(true)
				s.promoted.Add(1)
			}
			first.promoted <- promote
			continue
		}
		pending = append(pending[:0], first)
		buf = append(buf[:0], first.data...)
		count := first.count

	drain:
		for len(buf) < s.owner.policy.writeBuffer {
			select {
			case next, open := <-s.requests:
				if !open {
					break drain
				}
				if next.promoted != nil || next.fenced != nil {
					held = next
					haveHeld = true
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
		if s.current.Load().sealed.Load() {
			err = errors.New("startup capture partition is sealed")
		} else if failure := s.err.Load(); failure != nil {
			err = failure.err
		} else {
			encodedAt := time.Now()
			frame = recovery.EncodeCaptureFrame(enc, buf, frame, count)
			s.timings[captureEncode].observe(encodedAt)
			cur := s.current.Load()
			if cur.size.Load() > 0 && cur.size.Load()+int64(len(frame)) > s.owner.policy.segmentBytes {
				// Sealing rotates the append file, never the socket or the input stream.
				rotatedAt := time.Now()
				err = s.syncFile(cur.file)
				if err == nil {
					next, e := s.newSegment(index+1, true)
					err = e
					if err == nil {
						index++
						cur.next.Store(next)
						s.current.Store(next)
						cur.sealed.Store(true)
						cur = next
					}
				}

				s.timings[captureRotate].observe(rotatedAt)
			}
			if err == nil {
				for s.owner.diskDanger(int64(len(frame))) {
					s.owner.notePause()
					select {
					case <-s.owner.stop:
						err = errors.New("capture stopped at disk safety reserve")
					case <-time.After(10 * time.Millisecond):
					}
					if err != nil {
						break
					}
				}
			}
			if err == nil {
				writtenAt := time.Now()
				n, e := cur.file.Write(frame)
				s.timings[captureWrite].observe(writtenAt)
				err = e
				if err == nil && n != len(frame) {
					err = io.ErrShortWrite
				}
				if err == nil {
					cur.size.Add(int64(n))
					s.size.Add(int64(n))
					s.owner.size.Add(int64(n))
					s.owner.rawBytes.Add(int64(len(buf)))
					s.owner.points.Add(int64(count))
					if s.owner.observe != nil {
						err = points.ReadBinary(bytes.NewReader(buf), s.owner.observe)
					}
				}
			}
			s.fail(err)
		}
		s.pending.Add(-int64(len(pending)))
		for i := range pending {
			pending[i].done <- err
			pending[i] = startupAppend{}
		}
	}
	cur := s.current.Load()
	cur.sealed.Store(true)
	if !cur.retired.Load() {
		s.fail(s.syncFile(cur.file))
	}
}

// One reservation covers the complete frame, including conservative name
// ownership for distinct metrics; failed admission never enters the cache.
func captureMetricBytes(raw []byte, admission captureAdmission) (int64, error) {
	names := make(map[string]struct{})
	var cost int64
	err := points.ReadBinaryBytesBorrowed(raw, func(name []byte, _ []points.Point) {
		overhead := admission.RecoveryMetricBytes(name)
		if overhead == 0 {
			return
		}
		if _, ok := names[string(name)]; !ok {
			names[string(name)] = struct{}{}
			cost += overhead
		}
	})
	return cost, err
}
func (s *startupShard) apply(raw []byte, source *cache.RecoverySource, sink *startupSink) error {
	if s.names == nil {
		s.names = make(map[string]string)
	}
	var p points.Points
	namer, _ := sink.admission.(interface{ RecoveryMetricName([]byte) string })
	return points.ReadBinaryBytesBorrowed(raw, func(name []byte, data []points.Point) {
		canonical, ok := s.names[string(name)]
		if !ok {
			if len(s.names) >= 4096 || s.nameBytes+len(name) > 4<<20 {
				clear(s.names)
				s.nameBytes = 0
			}
			if namer != nil {
				canonical = namer.RecoveryMetricName(name)
			} else {
				canonical = string(name)
			}
			s.names[canonical] = canonical
			s.nameBytes += len(name)
		}
		p.Metric, p.Data = canonical, data
		sink.add(&p, source)
	})
}

func (s *startupShard) commit(seg *startupSegment, end int64, count int, physical int64) {
	s.applied.Store(&startupPosition{seg, end})
	s.offset.Add(physical)
	s.owner.replayedPoints.Add(int64(count))
	s.owner.replayedBytes.Add(physical)
	if seg.sealed.Load() && end == seg.size.Load() {
		seg.replayed.Store(true)
	}
}
func (s *startupInput) requestFallback() {
	select {
	case s.pressure <- struct{}{}:
	default:
	}
}
func (s *startupInput) diskDanger(n int64) bool {
	if !s.monitored.Load() {
		return false
	}
	limit := s.effectiveBytes.Load()
	return s.size.Load()-s.retiredBytes.Load()+n > max(0, limit)
}
func (s *startupInput) notePause() {
	if s.paused.CompareAndSwap(false, true) {
		s.fallback.Store(true)
		s.pauseExpected.Store(-1)
		zapwriter.Logger("app").Error("startup input reached actual disk safety reserve; capture backpressured", zap.Int64("retained_bytes", s.size.Load()-s.retiredBytes.Load()), zap.Int64("budget_bytes", s.effectiveBytes.Load()), zap.Int64("expected_pause_seconds", -1))
	}
}

// No duration or cache-capacity event stops capture. A measured negative
// convergence margin reports continued capture; only a disk safety reserve or
// an I/O failure can backpressure sockets.
func (s *startupInput) startMonitor(maxBytes int64) {
	s.monitored.Store(true)
	s.effectiveBytes.Store(maxBytes)
	go func() {
		defer close(s.done)
		tick := time.NewTicker(time.Second)
		defer tick.Stop()
		previousBytes, previousPoints, previousReplay := s.rawBytes.Load(), s.points.Load(), s.replayedPoints.Load()
		at := time.Now()
		cpu := []metrics.Sample{{Name: "/cpu/classes/idle:cpu-seconds"}}
		metrics.Read(cpu)
		idleBefore := cpu[0].Value.Float64()
		for {
			select {
			case <-s.stop:
				return
			case <-s.pressure:
				s.fallback.Store(true) // I/O errors are surfaced; capacity denial never sends this.
			case <-tick.C:
				now := time.Now()
				elapsed := now.Sub(at).Seconds()
				raw, points, replay := s.rawBytes.Load(), s.points.Load(), s.replayedPoints.Load()
				s.writeRate.Store(int64(float64(raw-previousBytes) / elapsed))
				s.replayRate.Store(int64(float64(replay-previousReplay) / elapsed))
				margin := float64((replay-previousReplay)-(points-previousPoints)) / elapsed
				backlog := max(0, points-replay)
				eta := captureTimeLimit(backlog, int64(margin))
				if backlog == 0 {
					eta = 0
				} else if eta < 0 {
					eta = -time.Second
				}
				s.effectiveLimit.Store(int64(eta))
				continuing := s.replayStarted.Load() && backlog > 0 && margin <= 0
				if continuing && s.continued.CompareAndSwap(false, true) {
					zapwriter.Logger("app").Error("startup replay has no convergence margin; continuing durable capture without pausing input", zap.Int64("backlog_points", backlog), zap.Int64("spool_budget_bytes", s.effectiveBytes.Load()), zap.Float64("net_replay_points_per_second", margin))
				} else if !continuing {
					s.continued.Store(false)
				}

				previousBytes, previousPoints, previousReplay, at = raw, points, replay, now
				metrics.Read(cpu)
				idle := max(0, (cpu[0].Value.Float64()-idleBefore)/elapsed)
				idleBefore = cpu[0].Value.Float64()
				s.idleCPUs.Store(int64(idle * 1000))

				retained := s.size.Load() - s.retiredBytes.Load()
				if free, err := migrationDiskFree(filepath.Dir(s.path)); err == nil {
					s.effectiveBytes.Store((int64(min(free, 1<<62)) + retained) / 4)
				}
				if s.syncing.CompareAndSwap(false, true) {
					s.syncWG.Go(func() {
						defer s.syncing.Store(false)
						var wg sync.WaitGroup
						for _, sh := range s.shards {
							wg.Go(func() { sh.fail(sh.sync()) })
						}
						wg.Wait()
						s.maybeDirect(time.Now())
						s.reclaim()
					})
				}
			}
		}
	}()
}
func (s *startupShard) sync() error {
	cur := s.current.Load()
	if cur == nil || cur.retired.Load() {
		return nil
	}
	return s.syncFile(cur.file)
}

// Delete only a durably retired contiguous prefix. A directory sync between
// removals prevents an older correction reappearing after a newer segment was
// removed in a crash. The history stream gates all current partition retirement.
func (s *startupInput) reclaim() {
	sink := s.sink.Load()
	if sink == nil || !sink.canRetire() || !s.historyReady.Load() {
		return
	}
	retire := func(sh *startupShard) bool {
		for seg := sh.first.Load(); seg != nil; seg = sh.first.Load() {
			if !seg.sealed.Load() || !seg.replayed.Load() || seg.source.Pending() != 0 {
				return false
			}
			var err error
			if sink.syncStore != nil {
				if err = sink.syncStore(); err != nil {
					sh.fail(err)
					return false
				}
			}
			if seg.file == nil {
				if sink.syncHistory != nil {
					err = sink.syncHistory()
				}
			} else {
				err = os.Remove(seg.path)
				if err == nil {
					err = syncDir(filepath.Dir(seg.path))
				}
				if err == nil {
					err = errors.Join(seg.file.Close(), seg.unlock())
				}
			}
			if err != nil {
				sh.fail(err)
				return false
			}
			seg.retired.Store(true)
			s.retiredBytes.Add(seg.size.Load())
			sh.first.CompareAndSwap(seg, seg.next.Load())
		}
		return true
	}
	if !retire(s.legacy) {
		return
	}
	all := true
	for _, sh := range s.shards {
		if !retire(sh) {
			all = false
		}
	}
	if all && s.directFence != nil && !s.directFence.retired.Load() {
		seg := s.directFence
		if err := os.Remove(seg.path); err != nil {
			s.legacy.fail(err)
			return
		}
		if err := syncDir(filepath.Dir(seg.path)); err != nil {
			s.legacy.fail(err)
			return
		}
		_ = seg.file.Close()
		_ = seg.unlock()
		seg.retired.Store(true)
	}
	if s.releaseLog != nil && (s.directFence == nil || s.directFence.retired.Load()) {
		if err := s.releaseLog(); err != nil {
			s.legacy.fail(err)
			return
		}
		s.releaseLog = nil
	}
}

func (s *startupInput) startReplay(sink *startupSink, legacy func() error) {
	s.replayOnce.Do(func() {
		s.replayStarted.Store(true)
		s.sink.Store(sink)
		go func() {
			defer close(s.replayDone)
			if err := legacy(); err != nil {
				select {
				case <-s.replayStop:
				default:
					s.sealErr = err
					s.requestFallback()
				}
				close(s.legacyDone)
				return
			}
			root := s.legacy.current.Load()
			if root.source == nil {
				root.source = s.legacy.stream.Next()
			}
			root.sealed.Store(true)
			root.replayed.Store(true)
			s.historyReady.Store(true)
			close(s.legacyDone)
			s.catching.Store(true)
			var wg sync.WaitGroup
			for _, sh := range s.shards {
				wg.Go(func() { sh.replay(sink) })
			}
			wg.Wait()
		}()
	})
}
func (s *startupShard) replay(sink *startupSink) {
	dec, err := zstd.NewReader(nil, zstd.WithDecoderConcurrency(1), zstd.WithDecoderMaxMemory(64<<20))
	if err != nil {
		s.fail(err)
		return
	}
	defer dec.Close()
	window := captureReadWindow{target: s.owner.policy.readBuffer}
	raw := make([]byte, 0, s.owner.policy.readBuffer)
	for {
		select {
		case <-s.owner.replayStop:
			return
		case <-s.owner.stop:
			return
		default:
		}
		if s.owner.direct.Load() {
			return
		}
		pos := s.applied.Load()
		seg := pos.segment
		if pos.offset == seg.size.Load() {
			if next := seg.next.Load(); next != nil {
				// Loading next acquires the final segment frontier. An append
				// may have completed after the first size comparison.
				if pos.offset != seg.size.Load() {
					continue
				}
				seg.replayed.Store(true)
				s.applied.Store(&startupPosition{next, 0})
				continue
			}
			if s.live.Load() {
				time.Sleep(time.Millisecond)
				continue
			}
			reply := make(chan bool, 1)
			request := startupAppend{promote: pos, promoted: reply}
			select {
			case s.requests <- request:
			case <-s.owner.replayStop:
				return
			case <-s.owner.stop:
				return
			}
			select {
			case <-reply:
			case <-s.owner.replayStop:
				return
			case <-s.owner.stop:
				return
			}
			time.Sleep(time.Millisecond)
			continue
		}
		frame, e := window.frame(seg, pos.offset, seg.size.Load())
		if e != nil {
			s.fail(e)
			return
		}
		size := len(frame)
		var count int
		raw, count, err = recovery.DecodeCaptureFrame(dec, frame, raw)
		if err != nil {
			s.fail(err)
			return
		}
		if err := s.owner.waitCapture(); err != nil {
			return
		}
		cost, e := captureMetricBytes(raw, sink.admission)
		if e != nil {
			s.fail(e)
			return
		}
		for !sink.admission.HasRecoveryCapacityBatch(count, cost) {
			s.live.Store(false)
			select {
			case <-s.owner.replayStop:
				return
			case <-s.owner.stop:
				return
			case <-time.After(time.Millisecond):
			}
		}
		applySink := *sink
		if s.live.Load() && sink.liveAdd != nil {
			applySink.add = sink.liveAdd
		}
		if err = s.apply(raw, seg.source, &applySink); err != nil {
			s.fail(err)
			return
		}
		s.commit(seg, pos.offset+int64(size), count, int64(size))
	}
}
func (s *startupInput) pauseReplay() {
	s.transition.Lock()
	s.switchStopped = true
	s.transition.Unlock()
	// A disk safety stop can block an append owner behind this control fence.
	// Abort that already-paused intake so shutdown cannot wait on its own quota.
	watcherDone := make(chan struct{})
	defer close(watcherDone)
	go func() {
		tick := time.NewTicker(10 * time.Millisecond)
		defer tick.Stop()
		for {
			select {
			case <-watcherDone:
				return
			case <-tick.C:
				if s.paused.Load() {
					s.stopOnce.Do(func() { close(s.stop) })
					return
				}
			}
		}
	}()
	s.sink.Store(nil)
	select {
	case <-s.replayStop:
	default:
		close(s.replayStop)
	}
	if s.replayStarted.Load() {
		<-s.replayDone
		// Fence queued frontier observations before clearing live state. The
		// reader has stopped; append owners continue writing durable frames.
		for _, sh := range s.shards {
			reply := make(chan bool, 1)
			sh.requests <- startupAppend{promoted: reply}
			<-reply
			sh.live.Store(false)
		}
	}
}
func (s *startupInput) close() error {
	s.closeOnce.Do(func() {
		s.stopOnce.Do(func() { close(s.stop) })
		s.pauseReplay()
		if s.monitored.Load() {
			<-s.done
		}
		s.syncWG.Wait()
		for _, sh := range s.shards {
			close(sh.requests)
		}
		for _, sh := range s.shards {
			<-sh.written
		}
		var errs []error
		for _, sh := range append([]*startupShard{s.legacy}, s.shards...) {
			if sh == nil {
				continue
			}
			for seg := sh.first.Load(); seg != nil; seg = seg.next.Load() {
				if !seg.retired.Load() && seg.file != nil {
					// Append owners have already joined after their final Sync;
					// history files were immutable before this process opened them.
					if seg == s.legacy.current.Load() {
						errs = append(errs, seg.file.Sync())
					}
					errs = append(errs, seg.file.Close(), seg.unlock())
				}
			}
		}
		if seg := s.directFence; seg != nil && !seg.retired.Load() {
			errs = append(errs, seg.file.Close(), seg.unlock())
		}
		s.closeErr = errors.Join(errs...)
		s.sealedAt = time.Now()
		close(s.sealed)
	})
	return s.closeErr
}
