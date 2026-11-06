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
	if len(existing) != 0 {
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

// Replay the immutable predecessor tail first, then follow independent metric
// shards. All readers finish before live input opens. A capacity/time/size limit
// seals capture immediately and falls back to ordered, finite-source recovery.
func (app *App) finishStartupInput(core *cache.Cache) error {
	s := app.startupInput
	if s == nil {
		return nil
	}
	budget := core.NewStartupRecoveryBudget(s.policy.recoveryPoints, s.policy.recoveryBytes)
	add := func(p *points.Points) {
		for !budget.HasCapacity(len(p.Data)) {
			select {
			case <-s.stop:
				return
			default:
			}
			s.requestFallback()
			time.Sleep(time.Millisecond)
		}
		core.AddRecovered(p)
	}
	// This file belongs to the predecessor and was sealed by TakeInput.
	count, err := replayInputTailReader(io.NewSectionReader(s.file, 0, s.legacy.size.Load()), min(s.policy.shards, runtime.GOMAXPROCS(0)), add)
	if err != nil {
		return fmt.Errorf("replay predecessor input: %w", err)
	}
	s.legacy.offset.Store(s.legacy.size.Load())
	s.replayedPoints.Add(int64(count))
	var wg sync.WaitGroup
	errs := make([]error, len(s.shards))
	for i, shard := range s.shards {
		wg.Go(func() {
			paced := s.pacedReplay(add)
			var unpublished int64
			tracked := func(p *points.Points) {
				paced(p)
				unpublished += int64(len(p.Data))
				if unpublished >= 4096 {
					s.replayedPoints.Add(unpublished)
					unpublished = 0
				}
			}
			_, err := replayCaptureShardBuffer(&startupShardReader{s: s, shard: shard}, tracked, s.policy.readBuffer)
			s.replayedPoints.Add(unpublished)
			errs[i] = err
			if err != nil {
				s.requestFallback()
			}
		})
	}
	s.catching.Store(true)
	wg.Wait()
	select {
	case <-s.stop:
		return errors.New("startup recovery stopped")
	case <-s.sealed:
	}
	if err = errors.Join(append(errs, s.sealErr)...); err != nil {
		return fmt.Errorf("replay startup input: %w", err)
	}
	if err = s.close(); err != nil {
		return err
	}
	app.startupInput = nil
	h := s.inherited
	app.startupInputPauseAt = s.sealedAt
	app.inheritedInput = &handoff.Input{Receivers: []handoff.InputReceiver{{Name: "tcp", Listener: h.Listener}}}
	for _, conn := range h.Conns {
		app.inheritedInput.Receivers[0].Conns = append(app.inheritedInput.Receivers[0].Conns, handoff.InputConn{File: conn.File, Leftover: conn.Leftover})
	}
	// The metrics snapshot outlives startup; leave socket/decoder leftovers
	// owned only by the receiver handoff, rather than retaining them for metrics.
	s.inherited = nil
	app.inputTailOffsets = make(map[string]int64)
	for _, shard := range append([]*startupShard{s.legacy}, s.shards...) {
		app.inputTailOffsets[shard.path] = shard.offset.Load()
	}
	app.startupCapturedPoints = uint64(s.points.Load())
	zapwriter.Logger("app").Info("startup input capture sealed", zap.Int64("points", s.points.Load()), zap.Int64("bytes", s.size.Load()), zap.Int64("replayed_points", s.replayedPoints.Load()), zap.Bool("fallback", s.fallback.Load()), zap.Duration("runtime", time.Since(s.started)))
	return nil
}

func closeTCPHandoff(h *tcp.Handoff) {
	_ = h.Listener.Close()
	for _, conn := range h.Conns {
		handoff.CloseInputConn(conn.File, len(conn.Leftover), zapwriter.Logger("app"), "inherited_start_failed")
	}
}
