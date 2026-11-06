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
	c := app.Config
	if app.handoffClaim == nil || !c.Dump.StoreInputHandoff || !c.Dump.Enabled ||
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
	existing, err := filepath.Glob(filepath.Join(c.Dump.Path, recovery.TailName(manifest.WAL.Name)+".capture-*.tail.bin"))
	if err != nil {
		return err
	}
	_ = existing
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
	h := takeInputReceiver(in, "tcp")
	if h == nil {
		return errors.New("store input handoff has no TCP receiver")
	}
	spool, err := openStartupInputWithPolicy(filepath.Join(c.Dump.Path, recovery.TailName(manifest.WAL.Name)), policy)
	if err != nil {
		closeTCPHandoff(h)
		return err
	}
	if receiverStoreHook != nil {
		spool.observe, _ = receiverStoreHook(func(*points.Points) {}, func([]byte, float64, int64) {})
	}
	r, err := tcp.NewInheritedCapture("tcp", c.Tcp, h, spool)
	if err != nil {
		_ = spool.close()
		return err
	}
	spool.receiver = r
	spool.startMonitor(spool.policy.limit, spool.policy.spoolBytes)
	app.startupCaptureState.Store(spool)
	app.startupInput = spool
	app.Receivers = []*NamedReceiver{{Name: "tcp", Receiver: r}}
	app.restart.noteInputOpened()
	zapwriter.Logger("app").Info("input capture active before storage and checkpoint", zap.String("filename", spool.path), zap.Int("connections", len(h.Conns)))
	return nil
}

// Attach replay without replacing or detaching any receiver. Startup returns
// while constrained partitions keep accepting durable input in the background.
func (app *App) finishStartupInput(core *cache.Cache) error {
	s := app.startupInput
	if s == nil {
		return nil
	}
	sink := &startupSink{admission: core, add: core.AddRecoveredFrom, canRetire: func() bool { return !core.HasPendingRecovery() }}
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
		admit := func(p *points.Points) {
			for !core.HasRecoveryCapacity(len(p.Data)) {
				if cancelled() {
					return
				}
				time.Sleep(time.Millisecond)
			}
			if !cancelled() {
				core.AddRecovered(p)
			}
		}
		for _, path := range paths {
			if cancelled() {
				return errors.New("startup tail replay stopped")
			}
			if path == s.path {
				_, err = replayInputTailReader(io.NewSectionReader(s.file, 0, s.legacy.size.Load()), min(s.policy.shards, runtime.GOMAXPROCS(0)), admit)
				if err == nil {
					s.legacy.offset.Store(s.legacy.size.Load())
				}
			} else if recovery.IsCaptureSegment(filepath.Base(path)) {
				_, err = recovery.ReadCaptureFile(path, admit)
			} else {
				_, err = replayInputTail(path, min(s.policy.shards, runtime.GOMAXPROCS(0)), admit)
			}
			if err != nil && !errors.Is(err, io.ErrUnexpectedEOF) && !errors.Is(err, io.EOF) {
				return err
			}
		}
		if cancelled() {
			return errors.New("startup tail replay stopped")
		}
		return app.replayInputLogStopped(core, s.replayStop)

	})
	zapwriter.Logger("app").Info("startup input replay running without pausing receivers", zap.Int("partitions", len(s.shards)))
	return nil
}

func closeTCPHandoff(h *tcp.Handoff) {
	_ = h.Listener.Close()
	for _, conn := range h.Conns {
		handoff.CloseInputConn(conn.File, len(conn.Leftover), zapwriter.Logger("app"), "inherited_start_failed")
	}
}
