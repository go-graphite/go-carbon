// Package v2recv connects the existing relay codecs to serial, bounded owners.
// It is an experimental receiver for the core-first daemon, not a legacy cache.
package v2recv

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-graphite/go-carbon/points"
	"github.com/go-graphite/go-carbon/receiver/parse"
	"github.com/go-graphite/go-carbon/receiver/stream/codec"
	"github.com/klauspost/compress/zstd"
)

// Owner is called by exactly one goroutine. Append borrows its arguments only
// until return. Seal+Drain must journal-sync before publishing to the core.
type Owner interface {
	Append([]string, []points.Point, []uint64, int64) error
	Seal() error
	Drain() error
}

type Config struct {
	BatchSize, QueueDepth, MaxConnections, MaxLineBytes int
	SyncInterval                                        time.Duration
	Stream                                              codec.Options
	Logger                                              *slog.Logger
}
type batch struct {
	names []string
	data  []points.Point
}
type counters struct{ accepted, rejected, parse, transport, sink, wire, connections atomic.Uint64 }
type Stats struct{ Accepted, Rejected, ParseErrors, TransportErrors, SinkErrors, WireBytes, Connections uint64 }

// Server has no writer-shared data lock. The mutex only protects connection
// lifetime and the first terminal error; every owner has a private input queue.
type Server struct {
	cfg      Config
	listener net.Listener
	queues   []chan batch
	owners   []Owner
	done     chan struct{}
	stop     sync.Once
	accept   sync.WaitGroup
	handlers sync.WaitGroup
	workers  sync.WaitGroup
	mu       sync.Mutex
	conns    map[net.Conn]struct{}
	err      error
	stats    counters
	finish   sync.Once
	finished chan struct{}
}

func Start(l net.Listener, owners []Owner, cfg Config) (*Server, error) {
	if l == nil || len(owners) == 0 {
		return nil, errors.New("receiver requires owners")
	}
	if cfg.BatchSize == 0 {
		cfg.BatchSize = 1024
	}
	if cfg.QueueDepth == 0 {
		cfg.QueueDepth = 2
	}
	if cfg.MaxConnections == 0 {
		cfg.MaxConnections = 128
	}
	if cfg.MaxLineBytes == 0 {
		cfg.MaxLineBytes = 1 << 20
	}
	if cfg.SyncInterval == 0 {
		cfg.SyncInterval = time.Second
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if cfg.BatchSize < 1 || cfg.BatchSize > 32768 || cfg.QueueDepth < 1 || cfg.MaxConnections < 1 || cfg.MaxLineBytes < 1 || cfg.SyncInterval < 0 {
		return nil, errors.New("invalid receiver bounds")
	}
	d, err := codec.NewDecoder(cfg.Stream)
	if err != nil {
		return nil, err
	}
	d.Close()
	s := &Server{cfg: cfg, listener: l, owners: owners, done: make(chan struct{}), finished: make(chan struct{}), conns: make(map[net.Conn]struct{})}
	for _, o := range owners {
		if o == nil {
			return nil, errors.New("nil owner")
		}
	}
	for _, o := range owners {
		q := make(chan batch, cfg.QueueDepth)
		s.queues = append(s.queues, q)
		s.workers.Add(1)
		go s.work(o, q)
	}
	s.accept.Add(1)
	go s.listen()
	return s, nil
}
func (s *Server) Stats() Stats {
	c := &s.stats
	return Stats{c.accepted.Load(), c.rejected.Load(), c.parse.Load(), c.transport.Load(), c.sink.Load(), c.wire.Load(), c.connections.Load()}
}
func (s *Server) fail(err error) {
	s.mu.Lock()
	if s.err == nil {
		s.err = err
	}
	s.mu.Unlock()
	s.cfg.Logger.Error("v2 receiver failed", "error", err)
	s.stop.Do(func() {
		close(s.done)
		_ = s.listener.Close()
		s.mu.Lock()
		for c := range s.conns {
			_ = c.Close()
		}
		s.mu.Unlock()
	})
}
func (s *Server) work(o Owner, q <-chan batch) {
	defer s.workers.Done()
	tick := time.NewTicker(s.cfg.SyncInterval)
	defer tick.Stop()
	var seq uint64
	failed := false
	syncOwner := func() {
		if !failed {
			if err := errors.Join(o.Seal(), o.Drain()); err != nil {
				failed = true
				s.stats.sink.Add(1)
				s.fail(err)
			}
		}
	}
	for {
		select {
		case b, ok := <-q:
			if !ok {
				syncOwner()
				return
			}
			if failed {
				s.stats.rejected.Add(uint64(len(b.data)))
				continue
			}
			sequence := make([]uint64, len(b.data))
			for i := range sequence {
				seq++
				sequence[i] = seq
			}
			if err := o.Append(b.names, b.data, sequence, time.Now().Unix()); err != nil {
				failed = true
				s.stats.rejected.Add(uint64(len(b.data)))
				s.stats.sink.Add(1)
				s.fail(err)
			} else {
				s.stats.accepted.Add(uint64(len(b.data)))
			}
		case <-tick.C:
			syncOwner()
		}
	}
}
func (s *Server) listen() {
	defer s.accept.Done()
	slots := make(chan struct{}, s.cfg.MaxConnections)
	for {
		select {
		case slots <- struct{}{}:
		case <-s.done:
			return
		}
		c, err := s.listener.Accept()
		if err != nil {
			<-slots
			if !errors.Is(err, net.ErrClosed) {
				s.fail(err)
			}
			return
		}
		s.mu.Lock()
		select {
		case <-s.done:
			s.mu.Unlock()
			_ = c.Close()
			<-slots
			return
		default:
		}
		s.conns[c] = struct{}{}
		s.mu.Unlock()
		s.stats.connections.Add(1)
		s.handlers.Add(1)
		go func() {
			defer s.handlers.Done()
			defer func() { _ = c.Close(); s.mu.Lock(); delete(s.conns, c); s.mu.Unlock(); <-slots }()
			if err := s.read(c); err != nil {
				s.stats.transport.Add(1)
				s.cfg.Logger.Error("v2 connection rejected", "peer", c.RemoteAddr(), "error", err)
			}
		}()
	}
}

// Wait stops admission, consumes existing connections through EOF, then drains
// and syncs every owner. Callers must first finish/half-close their senders.
// Cancellation closes sockets; already decoded points are still drained.
func (s *Server) Wait(ctx context.Context) error {
	s.finish.Do(func() {
		_ = s.listener.Close()
		go func() {
			s.accept.Wait()
			s.handlers.Wait()
			for _, q := range s.queues {
				close(q)
			}
			s.workers.Wait()
			close(s.finished)
		}()
	})
	select {
	case <-s.finished:
	case <-ctx.Done():
		s.fail(ctx.Err())
		<-s.finished
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.err
}

// Abort terminates connections and drains the decoded prefix with a loud error.
func (s *Server) Abort(err error) { s.fail(err) }

type countedReader struct {
	io.Reader
	n *atomic.Uint64
}

func (r countedReader) Read(b []byte) (int, error) {
	n, e := r.Reader.Read(b)
	r.n.Add(uint64(n))
	return n, e
}

type reference struct {
	name  string
	owner int
}

func ownerFor(name string, n int) int {
	h := uint64(14695981039346656037)
	for i := 0; i < len(name); i++ {
		h ^= uint64(name[i])
		h *= 1099511628211
	}
	return int(h % uint64(n))
}
func (s *Server) read(c net.Conn) error {
	r := bufio.NewReaderSize(countedReader{c, &s.stats.wire}, 64<<10)
	// Both protocols are selected on the same endpoint, as in receiver/tcp.
	head, err := r.Peek(1)
	if err == io.EOF {
		return nil
	}
	if err != nil {
		return err
	}
	buffers := make([]batch, len(s.queues))
	flush := func(i int) error {
		b := buffers[i]
		if len(b.data) == 0 {
			return nil
		}
		select {
		case s.queues[i] <- b:
			buffers[i] = batch{}
			return nil
		case <-s.done:
			s.stats.rejected.Add(uint64(len(b.data)))
			buffers[i] = batch{}
			return errors.New("receiver stopped")
		}
	}
	flushAll := func() error {
		var err error
		for i := range buffers {
			err = errors.Join(err, flush(i))
		}
		return err
	}
	defer func() { _ = flushAll() }()
	var appendErr error
	add := func(ref reference, value float64, ts int64) {
		if appendErr != nil {
			s.stats.rejected.Add(1)
			return
		}
		b := &buffers[ref.owner]
		b.names = append(b.names, ref.name)
		b.data = append(b.data, points.Point{Value: value, Timestamp: ts})
		if len(b.data) >= s.cfg.BatchSize {
			appendErr = flush(ref.owner)
		}
	}
	if head[0] == 0 {
		pre := make([]byte, codec.PreambleSize)
		if _, err = io.ReadFull(r, pre); err != nil {
			return err
		}
		flags, err := codec.ParsePreamble(pre)
		if err != nil {
			return err
		}
		opts := s.cfg.Stream
		opts.Flags = flags
		d, err := codec.NewDecoder(opts)
		if err != nil {
			return err
		}
		defer d.Close()
		for {
			_, err = d.ReadFrame(r, func(_ uint32, name []byte, value float64, ts int64, slot *any) {
				if *slot == nil {
					n := string(name)
					*slot = reference{n, ownerFor(n, len(s.queues))}
				}
				add((*slot).(reference), value, ts)
			})
			if err == io.EOF {
				return flushAll()
			}
			if err != nil {
				return err
			}
			if appendErr != nil {
				return appendErr
			}
			if err = flushAll(); err != nil {
				return err
			}
		}
	}
	var source io.Reader = r
	if head[0] == 0x28 {
		z, err := zstd.NewReader(r, zstd.WithDecoderConcurrency(1), zstd.WithDecoderMaxMemory(64<<20))
		if err != nil {
			return err
		}
		defer z.Close()
		source = z
	}
	plain := bufio.NewReaderSize(source, 64<<10)
	for {
		line, err := plain.ReadSlice('\n')
		if err == bufio.ErrBufferFull {
			joined := append([]byte(nil), line...)
			for err == bufio.ErrBufferFull {
				if len(joined) > s.cfg.MaxLineBytes {
					return fmt.Errorf("plaintext line limit: %d", s.cfg.MaxLineBytes)
				}
				line, err = plain.ReadSlice('\n')
				joined = append(joined, line...)
			}
			line = joined
		}
		if err == io.EOF && len(line) == 0 {
			return flushAll()
		}
		if err != nil {
			return err
		}
		if len(line) > s.cfg.MaxLineBytes {
			return errors.New("plaintext line limit")
		}
		if len(line) > 1 {
			name, value, ts, e := parse.PlainLine(line)
			if e != nil {
				s.stats.parse.Add(1)
				s.stats.rejected.Add(1)
				s.cfg.Logger.Error("v2 invalid plaintext point", "error", e)
			} else {
				n := string(name)
				add(reference{n, ownerFor(n, len(s.queues))}, value, ts)
			}
		}
		if appendErr != nil {
			return appendErr
		}
		if plain.Buffered() == 0 {
			if err = flushAll(); err != nil {
				return err
			}
		}
	}
}
