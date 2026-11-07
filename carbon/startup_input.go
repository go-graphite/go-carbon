package carbon

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/go-graphite/go-carbon/cache"
	"github.com/go-graphite/go-carbon/internal/handoff"
	"github.com/go-graphite/go-carbon/internal/recovery"
	"github.com/go-graphite/go-carbon/points"
	"github.com/go-graphite/go-carbon/receiver/tcp"
	"github.com/lomik/zapwriter"
	"go.uber.org/zap"
)

// Take the input-only offer before ANY cache-sized startup work. This uses the
// existing protocol, including when upgrading from a predecessor that does not
// know about startup capture. Mirrored read handoff retains its existing path.
func (app *App) startStartupInput() error {
	fences, err := filepath.Glob(filepath.Join(app.Config.Dump.Path, "input.*.tail.bin.capture-epoch-*"+startupDirectSuffix))
	if err != nil {
		return err
	}
	if len(fences) > 0 && !app.Config.InputLog.Enabled {
		return errors.New("retained direct capture requires its input log; preserve recovery sources and reenable input logging")
	}
	if err := app.tryStartupInput(); err != nil {
		return err
	}
	if len(fences) > 0 && app.startupInput == nil {
		return errors.New("retained direct capture requires ordered startup recovery; preserve sources and restore compatible configuration, manifest and headroom")
	}
	return nil
}

func (app *App) tryStartupInput() error {
	if app.startupInput != nil {
		return nil
	}
	c := app.Config
	if !c.Dump.StoreInputHandoff || !c.Dump.Enabled ||
		c.Whisper.StorageBackend != "pebble-chunk" || c.Whisper.StoreMirrorFiles ||
		c.Tags.Enabled || !c.Tcp.Enabled || c.Tcp.BufferSize != 0 || c.Tcp.Compression != "" ||
		c.Udp.Enabled || c.Pickle.Enabled || len(c.Receiver) != 0 {
		return nil
	}
	// Only the name is needed here; full checksum/root/index validation remains
	// on the ordinary startup path, before a point can enter the cache.
	f, err := os.Open(filepath.Join(c.Dump.Path, recovery.ManifestName))
	if err != nil {
		return nil
	}
	var manifest recovery.Manifest
	err = json.NewDecoder(io.LimitReader(f, 1<<20)).Decode(&manifest)
	_ = f.Close()
	if err != nil || filepath.Base(manifest.WAL.Name) != manifest.WAL.Name || manifest.WAL.Name == "." {
		return nil
	}
	_, err = filepath.Glob(filepath.Join(c.Dump.Path, recovery.TailName(manifest.WAL.Name)+".capture-*.tail.bin"))
	if err != nil {
		return err
	}
	entries, scanErr := os.ReadDir(c.Dump.Path)
	if scanErr != nil {
		return scanErr
	}
	orphanedCapture := false
	for _, entry := range entries {
		name := entry.Name()
		generation := strings.HasPrefix(name, "cache.") || strings.HasPrefix(name, recovery.CompactPrefix) || strings.HasPrefix(name, recovery.BlocksPrefix) || strings.HasPrefix(name, "input.") || strings.HasPrefix(name, recovery.CompactInputPrefix)
		if generation && name != manifest.Cache.Name && name != manifest.WAL.Name && !recovery.IsGenerationTail(name, manifest.WAL.Name) {
			zapwriter.Logger("app").Error("startup capture requires a single ordered recovery generation; using ordinary recovery", zap.String("filename", name))
			return nil
		}
		if !orphanedCapture && app.handoffClaim == nil && !entry.IsDir() && recovery.IsGenerationTail(name, manifest.WAL.Name) && recovery.IsCaptureSegment(name) && !handoff.Locked(filepath.Join(c.Dump.Path, name)) {
			info, err := entry.Info()
			if err != nil {
				return err
			}
			orphanedCapture = info.Size() > 0 || isStartupDirectFence(name)
		}
	}
	if app.handoffClaim == nil && !orphanedCapture {
		return nil
	}
	policy := startupPolicy(filepath.Join(c.Dump.Path, recovery.TailName(manifest.WAL.Name)))
	policy.log()
	if policy.available < 64<<20 || policy.spoolBytes < 64<<20 {
		state := &startupInput{policy: policy, sealed: make(chan struct{})}
		close(state.sealed)
		state.fallback.Store(true)
		app.startupCaptureState.Store(state)
		zapwriter.Logger("app").Error("startup capture lacks safe headroom; using ordered input recovery", zap.Int64("available_memory_bytes", policy.available), zap.Int64("spool_limit_bytes", policy.spoolBytes))
		return nil
	}
	claim := app.handoffClaim
	app.handoffClaim = nil
	var h *tcp.Handoff
	tcpOptions := c.Tcp
	if claim == nil {
		// A crashed capture owner has no sockets to transfer. Start a new
		// capture receiver so its retained epochs use the same parallel,
		// ordered replay path as a normal handoff.
		h, tcpOptions, err = listenStartupCapture(c.Tcp)
		if err != nil {
			return err
		}
	} else {
		ln, _, err := claim.Take(&net.TCPAddr{})
		if ln != nil {
			_ = ln.Close()
			_ = claim.Close()
			return errors.New("expected an input-only store handoff")
		}
		if err != nil {
			_ = claim.Close()
			return fmt.Errorf("take early input offer: %w", err)
		}
		in, err := claim.TakeInput()
		if err != nil {
			return fmt.Errorf("take early store input: %w", err)
		}
		if in == nil {
			return nil
		}
		defer in.Close()
		h = takeInputReceiver(in, "tcp")
		if h == nil {
			return errors.New("store input handoff has no TCP receiver")
		}
	}
	spool, err := openStartupInputWithPolicy(filepath.Join(c.Dump.Path, recovery.TailName(manifest.WAL.Name)), policy)
	if err != nil {
		closeTCPHandoff(h)
		return err
	}
	if receiverStoreHook != nil {
		spool.observe, _ = receiverStoreHook(func(*points.Points) {}, func([]byte, float64, int64) {})
	}
	r, err := tcp.NewInheritedCapture("tcp", tcpOptions, h, spool)
	if err != nil {
		_ = spool.close()
		return err
	}
	spool.receiver = r
	spool.startMonitor(spool.policy.spoolBytes)
	app.startupCaptureState.Store(spool)
	app.startupInput = spool
	app.Receivers = []*NamedReceiver{{Name: "tcp", Receiver: r}}
	app.restart.noteInputOpened()
	zapwriter.Logger("app").Info("input capture active before storage and checkpoint", zap.String("filename", spool.path), zap.Int("connections", len(h.Conns)))
	return nil
}

func listenStartupCapture(options *tcp.Options) (*tcp.Handoff, *tcp.Options, error) {
	addr, err := net.ResolveTCPAddr("tcp", options.Listen)
	if err != nil {
		return nil, nil, err
	}
	listener, err := net.ListenTCP("tcp", addr)
	if err != nil {
		return nil, nil, err
	}
	bound := *options
	bound.Listen = listener.Addr().String()
	file, err := listener.File()
	_ = listener.Close()
	if err != nil {
		return nil, nil, err
	}
	return &tcp.Handoff{Listener: file}, &bound, nil
}

// Attach replay without replacing or detaching any receiver. Startup returns
// while constrained partitions keep accepting durable input in the background.
func (app *App) finishStartupInput(core *cache.Cache) {
	s := app.startupInput
	if s == nil {
		return
	}
	sink := &startupSink{admission: core, add: core.AddRecoveredFrom, liveAdd: core.AddCapturedFrom, canRetire: func() bool { return !core.HasPendingRecovery() }, syncHistory: app.syncInputLog}
	if app.MetricStore != nil {
		sink.syncStore = app.MetricStore.Sync
	}
	add, addPoint := core.Add, core.AddPoint
	if receiverStoreHook != nil {
		add, addPoint = receiverStoreHook(add, addPoint)
	}
	sink.directAdd = add
	sink.drain = core.SetRecoveryDrain
	if s.receiver != nil {
		s.receiver.SetPointOut(addPoint)
		if receiverStoreHook == nil {
			s.receiver.SetPointsOut(core.AddPoints)
			s.receiver.SetPointBatchOut(core.AddPointBatch)
		}
	}
	if log := app.inputLog.Load(); log != nil && app.Config.InputLog.Enabled {
		sink.startLog = func() (func() error, error) {
			log := app.inputLog.Load()
			if log == nil {
				return nil, errors.New("input log unavailable after reset failure")
			}
			// Do not retire files belonging to an active store-copy session.
			if _, err := log.PinCopy(); err != nil {
				return nil, err
			}
			stats := log.Metrics()
			if stats.PointsWritten == 0 && stats.ReplayedPoints == 0 {
				// The usual handoff has a fresh journal. Its first records are
				// the new direct suffix, so checkpoint/tail sources can remain
				// pending and retire behind this retention pin.
				return log.ReleaseCopy, nil
			}
			if s.legacy.first.Load() != nil || !core.InputLogIdle() {
				_ = log.ReleaseCopy()
				// These finite older sources gate a new journal boundary. At
				// low input, metrics/max-delay would only decay toward zero.
				core.SetRecoveryDrain(true)
				return nil, errors.New("earlier journal/history sources have not persisted")
			}
			if sink.syncStore != nil {
				if err := sink.syncStore(); err != nil {
					_ = log.ReleaseCopy()
					return nil, err
				}
			}
			// Input is still fenced into capture. Old journal values have all
			// persisted; discard their replay eligibility before placing a new
			// epoch boundary, including the truncate-lag grace period.
			app.inputLog.Store(nil)
			if err := log.Retire(); err != nil {
				return nil, err
			}
			core.DisableInputLog()
			core.EnableInputLogFromNow()
			if err := app.openInputLog(core); err != nil {
				return nil, err
			}
			log = app.inputLog.Load()
			_, err := log.PinCopy()
			return log.ReleaseCopy, err
		}
	}
	logPath, draining := app.Config.InputLog.Path, app.Persister != nil
	s.startReplay(sink, func() error {
		// Older epochs precede the current stream, including after a crash during
		// partially completed startup. Current files stay exclusively locked.
		entries, err := os.ReadDir(filepath.Dir(s.path))
		if err != nil {
			return err
		}
		paths := []string{s.path}
		for _, entry := range entries {
			path := filepath.Join(filepath.Dir(s.path), entry.Name())
			if path != s.path && recovery.IsTail(entry.Name()) && !handoff.Locked(path) {
				paths = append(paths, path)
			}
		}
		slices.Sort(paths)
		fencedEpoch := ""
		for _, path := range paths {
			if isStartupDirectFence(path) {
				fencedEpoch = strings.TrimSuffix(path, startupDirectSuffix)
			}
		}
		cancelled := func() bool {
			select {
			case <-s.replayStop:
				return true
			case <-s.stop:
				return true
			default:
				return false
			}
		}
		tail := s.legacy.current.Load()
		add := func(source *cache.RecoverySource) func(*points.Points) {
			return func(p *points.Points) {
				for !core.HasRecoveryCapacityFor(p.Metric, len(p.Data)) {
					if cancelled() {
						return
					}
					time.Sleep(time.Millisecond)
				}
				if !cancelled() {
					core.AddRecoveredFrom(p, source)
				}
			}
		}
		replayLog := func() error {
			if s.historyLog.Load() != nil {
				return nil
			}
			seg := &startupSegment{source: s.legacy.stream.NextJournal()}
			tail.next.Store(seg)
			tail = seg
			s.historyLog.Store(seg)
			err := app.replayInputLogWith(core, s.replayStop, logPath, draining, func(p *points.Points) { core.AddRecoveredFrom(p, seg.source) })
			seg.sealed.Store(true)
			seg.replayed.Store(true)
			return err
		}
		openSegment := func(path string) (*startupSegment, error) {
			if path == s.path {
				seg := s.legacy.current.Load()
				seg.source = s.legacy.stream.Next()
				return seg, nil
			}
			f, e := os.OpenFile(path, os.O_RDWR, 0600)
			if e != nil {
				return nil, e
			}
			unlock, e := handoff.LockExclusive(path)
			if e != nil {
				f.Close()
				return nil, e
			}
			info, e := f.Stat()
			if e != nil {
				f.Close()
				unlock()
				return nil, e
			}
			seg := &startupSegment{file: f, path: path, unlock: unlock, source: s.legacy.stream.Next()}
			seg.size.Store(info.Size())
			s.size.Add(info.Size())
			tail.next.Store(seg)
			tail = seg
			return seg, nil
		}
		replaySegment := func(seg *startupSegment, framed bool) error {
			reader := startupStoppedReader{reader: io.NewSectionReader(seg.file, 0, seg.size.Load()), stop: s.replayStop}
			started := time.Now()
			var loaded int
			var e error
			if framed {
				worker := &startupShard{owner: s}
				loaded, e = recovery.ReadCaptureFramesBuffer(reader, s.policy.readBuffer, func(raw []byte, n int) error {
					if e := s.waitCapture(); e != nil {
						return e
					}
					cost, e := captureMetricBytes(raw, core)
					if e != nil {
						return e
					}
					for !core.HasRecoveryCapacityBatch(n, cost) {
						if cancelled() {
							return errStartupReplayStopped
						}
						time.Sleep(time.Millisecond)
					}
					if e := worker.apply(raw, seg.source, sink); e != nil {
						return e
					}
					s.historyPoints.Add(int64(n))
					return nil
				})
			} else {
				loaded, e = replayInputTailReader(reader, min(s.policy.shards, runtime.GOMAXPROCS(0)), add(seg.source))
			}
			if e != nil && !errors.Is(e, io.EOF) && !errors.Is(e, io.ErrUnexpectedEOF) {
				return e
			}
			if seg.path == s.path {
				s.legacy.offset.Store(seg.size.Load())
			}
			seg.sealed.Store(true)
			seg.replayed.Store(true)
			zapwriter.Logger("app").Info("input tail loaded", zap.String("filename", seg.path), zap.Int("points", loaded), zap.Duration("runtime", time.Since(started)), zap.Bool("input_capturing", true))
			return nil
		}
		for i := 0; i < len(paths); {
			if cancelled() {
				return errStartupReplayStopped
			}
			if !recovery.IsCaptureSegment(filepath.Base(paths[i])) {
				seg, e := openSegment(paths[i])
				if e != nil {
					return e
				}
				if e = replaySegment(seg, false); e != nil {
					return e
				}
				i++
				continue
			}
			if fencedEpoch == "" {
				if e := replayLog(); e != nil {
					return e
				}
			}
			// Epoch first, partition second, segment sequence last. Readers
			// overlap partitions only, joining before the next epoch begins.
			epochKey := func(path string) string {
				prefix, suffix, _ := strings.Cut(path, ".capture-epoch-")
				epoch, _, _ := strings.Cut(suffix, ".")
				return prefix + ".capture-epoch-" + epoch
			}
			epoch := epochKey(paths[i])
			groups := make(map[string][]*startupSegment)
			for i < len(paths) && recovery.IsCaptureSegment(filepath.Base(paths[i])) && epochKey(paths[i]) == epoch {
				seg, e := openSegment(paths[i])
				if e != nil {
					return e
				}
				_, suffix, _ := strings.Cut(paths[i], ".capture-epoch-")
				fields := strings.Split(suffix, ".")
				groups[fields[1]] = append(groups[fields[1]], seg)
				i++
			}
			jobs := make(chan []*startupSegment, len(groups))
			for _, segments := range groups {
				jobs <- segments
			}
			close(jobs)
			failures := make(chan error, len(groups))
			var wg sync.WaitGroup
			// Capture appenders and socket readers need runnable capacity while
			// old epochs are backlogged. Queue feedback handles transient bursts.
			for range min(len(groups), max(1, runtime.GOMAXPROCS(0)/2)) {
				wg.Go(func() {
					for segments := range jobs {
						for _, seg := range segments {
							if e := replaySegment(seg, true); e != nil {
								failures <- e
								break
							}
						}
					}
				})
			}
			wg.Wait()
			close(failures)
			for e := range failures {
				return e
			}
			if epoch == fencedEpoch {
				if e := replayLog(); e != nil {
					return e
				}
			}
		}

		if cancelled() {
			return errStartupReplayStopped
		}
		return replayLog()

	})
	zapwriter.Logger("app").Info("startup input replay running without pausing receivers", zap.Int("partitions", len(s.shards)))
}

func closeTCPHandoff(h *tcp.Handoff) {
	_ = h.Listener.Close()
	for _, conn := range h.Conns {
		handoff.CloseInputConn(conn.File, len(conn.Leftover), zapwriter.Logger("app"), "inherited_start_failed")
	}
}

var errStartupReplayStopped = errors.New("startup input replay stopped")

type startupStoppedReader struct {
	reader io.Reader
	stop   <-chan struct{}
}

func (r startupStoppedReader) Read(p []byte) (int, error) {
	select {
	case <-r.stop:
		return 0, errStartupReplayStopped
	default:
		return r.reader.Read(p)
	}
}
