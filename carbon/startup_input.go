package carbon

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-graphite/go-carbon/cache"
	"github.com/go-graphite/go-carbon/internal/handoff"
	"github.com/go-graphite/go-carbon/internal/recovery"
	"github.com/go-graphite/go-carbon/points"
	"github.com/go-graphite/go-carbon/receiver/tcp"
	"github.com/lomik/zapwriter"
	"go.uber.org/zap"
)

// startupInput keeps the predecessor's tail as the recovery authority while the
// successor opens storage and validates its checkpoint. Socket handlers append
// complete records in connection-local batches; recovery follows a published
// complete-record frontier. Neither cache size nor the RAM read-ahead limit can
// stop socket draining. Sources remain owned by ordinary pending recovery.
type startupInput struct {
	mu             sync.Mutex
	file           *os.File
	path           string
	err            error
	size           atomic.Int64
	points         atomic.Int64
	offset         int64 // replayed prefix; only the startup goroutine changes it
	replayedPoints int64
	unlock         func() error
	stop           chan struct{}
	done           chan struct{}
	receiver       *tcp.TCP
	started        time.Time
	observe        func(*points.Points) // receiverStoreHook, tests only
}

func openStartupInput(path string) (*startupInput, error) {
	unlock, err := handoff.LockExclusive(path)
	if err != nil {
		return nil, err
	}
	f, err := os.OpenFile(path, os.O_RDWR|os.O_APPEND, 0600)
	if err != nil {
		_ = unlock()
		return nil, err
	}
	info, err := f.Stat()
	if err != nil {
		_ = f.Close()
		_ = unlock()
		return nil, err
	}
	s := &startupInput{file: f, path: path, unlock: unlock, stop: make(chan struct{}), done: make(chan struct{}), started: time.Now()}
	s.size.Store(info.Size())
	go func() {
		defer close(s.done)
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-s.stop:
				return
			case <-ticker.C:
				err := s.file.Sync()
				s.mu.Lock()
				s.err = errors.Join(s.err, err)
				s.mu.Unlock()
			}
		}
	}()
	return s, nil
}

func (s *startupInput) Write(data []byte, count int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err != nil {
		return s.err
	}
	n, err := s.file.Write(data)
	if err == nil && n != len(data) {
		err = io.ErrShortWrite
	}
	s.err = err
	if err == nil {
		s.size.Add(int64(n))
		s.points.Add(int64(count))
		if s.observe != nil {
			s.err = points.ReadBinary(bytes.NewReader(data), s.observe)
		}
	}
	return s.err
}

// close runs after all capture handlers have stopped. A failed startup retains
// the very same tail the ordinary crash/ordered recovery already understands.
func (s *startupInput) close() error {
	close(s.stop)
	<-s.done
	s.mu.Lock()
	defer s.mu.Unlock()
	s.err = errors.Join(s.err, s.file.Sync(), s.file.Close(), s.unlock())
	return s.err
}

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
	spool, err := openStartupInput(filepath.Join(c.Dump.Path, recovery.TailName(manifest.WAL.Name)))
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
	app.startupInput = spool
	app.Receivers = []*NamedReceiver{{Name: "tcp", Receiver: r}}
	app.restart.noteInputOpened()
	zapwriter.Logger("app").Info("input capture active before storage and checkpoint", zap.String("filename", spool.path), zap.Int("connections", len(h.Conns)))
	return nil
}

// startupInputReader follows the append frontier with one persistent decoder
// and worker pool. Recreating recovery for successive snapshots has a fixed
// cost: at high input rates that alone can keep a small backlog from converging.
// When at most 1 MiB remains, detach capture once, then read its final flush.
type startupInputReader struct {
	s         *startupInput
	app       *App
	inherited *tcp.Handoff
	detached  bool
}

func (r *startupInputReader) Read(buf []byte) (int, error) {
	if len(buf) == 0 {
		return 0, nil
	}
	s := r.s
	s.mu.Lock()
	err := s.err
	s.mu.Unlock()
	if err != nil {
		return 0, err
	}
	if !r.detached && s.size.Load()-s.offset <= 1<<20 {
		r.detached = true
		r.app.startupInputPauseAt = time.Now()
		r.inherited, err = s.receiver.Detach()
		if err != nil {
			return 0, err
		}
		r.app.Receivers = nil
	}
	remaining := s.size.Load() - s.offset
	if remaining == 0 {
		return 0, io.EOF
	}
	n, err := s.file.ReadAt(buf[:min(int64(len(buf)), remaining)], s.offset)
	s.offset += int64(n)
	return n, err
}

// Catch up while capture continues. Pending recovery owns both the immutable
// checkpoint and this tail until all recovered cache items have persisted.
func (app *App) finishStartupInput(core *cache.Cache) error {
	s := app.startupInput
	if s == nil {
		return nil
	}
	reader := &startupInputReader{s: s, app: app}
	count, err := replayInputTailReader(reader, min(32, runtime.GOMAXPROCS(0)), func(p *points.Points) {
		for !core.HasRecoveryCapacity(len(p.Data)) {
			time.Sleep(time.Millisecond)
		}
		core.AddRecovered(p)
	})
	if err != nil {
		if reader.inherited != nil {
			closeTCPHandoff(reader.inherited)
		}
		return fmt.Errorf("replay startup input: %w", err)
	}
	s.replayedPoints = int64(count)
	err = s.close()
	app.startupInput = nil
	h := reader.inherited
	if err != nil {
		if h != nil {
			closeTCPHandoff(h)
		}
		return err
	}

	app.inheritedInput = &handoff.Input{Receivers: []handoff.InputReceiver{{Name: "tcp", Listener: h.Listener}}}
	for _, conn := range h.Conns {
		app.inheritedInput.Receivers[0].Conns = append(app.inheritedInput.Receivers[0].Conns, handoff.InputConn{File: conn.File, Leftover: conn.Leftover})
	}
	app.inputTailOffsets = map[string]int64{s.path: s.offset}
	app.startupCapturedPoints = uint64(s.points.Load())
	zapwriter.Logger("app").Info("startup input capture sealed", zap.Int64("points", s.points.Load()), zap.Int64("bytes", s.size.Load()), zap.Int64("replayed_bytes", s.offset), zap.Int64("replayed_points", s.replayedPoints), zap.Duration("runtime", time.Since(s.started)))
	return nil
}

func closeTCPHandoff(h *tcp.Handoff) {
	_ = h.Listener.Close()
	for _, conn := range h.Conns {
		handoff.CloseInputConn(conn.File, len(conn.Leftover), zapwriter.Logger("app"), "inherited_start_failed")
	}
}
