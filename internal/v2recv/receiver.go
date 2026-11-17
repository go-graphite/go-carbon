// Package v2recv connects the existing relay codecs to serial, bounded owners.
// It is an experimental receiver for the core-first daemon, not a legacy cache.
package v2recv

import (
	"bufio"
	"bytes"
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
	OnStopped func() // called once after Wait drains handlers and owners

	PlacementKeys                                       map[string][]byte
	Placement                                           PlacedSink
	PlacementKey                                        []byte
	PlacementProducer                                   string
	BatchSize, QueueDepth, MaxConnections, MaxLineBytes int
	SyncInterval                                        time.Duration
	Stream                                              codec.Options
	Logger                                              *slog.Logger
}
type batch struct {
	names []string
	data  []points.Point
}
type ownerCall struct {
	run   func() error
	ack   chan error
	drain bool
}
type counters struct{ accepted, rejected, parse, transport, sink, wire, connections atomic.Uint64 }
type Stats struct{ Accepted, Rejected, ParseErrors, TransportErrors, SinkErrors, WireBytes, Connections uint64 }

// Server shares a buffer-level maintenance fence. The mutex protects connection
// lifetime and the first terminal error; every owner has a private input queue.
type Server struct {
	cfg      Config
	listener net.Listener
	queues   []chan *batch
	batches  sync.Pool
	controls []chan ownerCall
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
	intake   sync.RWMutex                   // held per input buffer, never per name or point
	decoded  map[*fencedReader]func() error // protected by the exclusive intake fence
	finish   sync.Once
	finished chan struct{}
}

func Start(l net.Listener, owners []Owner, cfg Config) (*Server, error) {
	if l == nil || (len(owners) == 0 && cfg.Placement == nil) {
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
	if cfg.Placement != nil {
		if len(owners) != 0 {
			return nil, errors.New("placed endpoint requires exclusive placed sink")
		}
		keys := make(map[string][]byte)
		for producer, key := range cfg.PlacementKeys {
			if producer == "" || len(key) < 32 || len(key) > 4096 {
				return nil, errors.New("invalid placement producer key")
			}
			keys[producer] = append([]byte(nil), key...)
		}
		if len(cfg.PlacementKey) != 0 || cfg.PlacementProducer != "" {
			if len(keys) != 0 || len(cfg.PlacementKey) < 32 || len(cfg.PlacementKey) > 4096 || cfg.PlacementProducer == "" {
				return nil, errors.New("placed endpoint requires producer and exclusive credential configuration")
			}
			keys[cfg.PlacementProducer] = append([]byte(nil), cfg.PlacementKey...)
		}
		if len(keys) == 0 {
			return nil, errors.New("placed endpoint requires producer keys")
		}
		cfg.PlacementKeys = keys
		cfg.PlacementKey = nil
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
		q := make(chan *batch, cfg.QueueDepth)
		control := make(chan ownerCall)
		s.controls = append(s.controls, control)
		s.queues = append(s.queues, q)
		s.workers.Add(1)
		go s.work(o, q, control)
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
func (s *Server) work(o Owner, q <-chan *batch, control <-chan ownerCall) {
	defer s.workers.Done()
	tick := time.NewTicker(s.cfg.SyncInterval)
	defer tick.Stop()
	var seq uint64
	failed := false
	// Socket flushes can contain one point per owner. Accumulate on the
	// existing serial worker, bounded by BatchSize and the existing sync tick.
	// Per-name order survives this coalescing.
	var pending batch
	sequence := make([]uint64, 0, s.cfg.BatchSize)
	appendBatch := func(names []string, data []points.Point) {
		sequence = sequence[:0]
		for range data {
			seq++
			sequence = append(sequence, seq)
		}
		if err := o.Append(names, data, sequence, time.Now().Unix()); err != nil {
			failed = true
			s.stats.rejected.Add(uint64(len(data)))
			s.stats.sink.Add(1)
			s.fail(err)
		} else {
			s.stats.accepted.Add(uint64(len(data)))
		}
	}
	appendPending := func() {
		if failed || len(pending.data) == 0 {
			return
		}
		appendBatch(pending.names, pending.data)
		clear(pending.names)
		pending.names = pending.names[:0]
		pending.data = pending.data[:0]
	}
	syncOwner := func() {
		appendPending()
		if !failed {
			if err := errors.Join(o.Seal(), o.Drain()); err != nil {
				failed = true
				s.stats.sink.Add(1)
				s.fail(err)
			}
		}
	}
	consume := func(b *batch) {
		originalNames, originalData := b.names, b.data
		recycle := func() {
			clear(originalNames)
			b.names, b.data = originalNames[:0], originalData[:0]
			s.batches.Put(b)
		}
		if failed {
			s.stats.rejected.Add(uint64(len(b.data)))
			recycle()
			return
		}
		if len(pending.data) == 0 && len(b.data) == s.cfg.BatchSize {
			appendBatch(b.names, b.data)
			b.names, b.data = b.names[:0], b.data[:0]
		}
		for len(b.data) > 0 {
			n := min(len(b.data), s.cfg.BatchSize-len(pending.data))
			pending.names = append(pending.names, b.names[:n]...)
			pending.data = append(pending.data, b.data[:n]...)
			b.names, b.data = b.names[n:], b.data[n:]
			if len(pending.data) == s.cfg.BatchSize {
				appendPending()
			}
			if failed {
				s.stats.rejected.Add(uint64(len(b.data)))
				break
			}
		}
		recycle()
	}
	for {
		select {
		case call := <-control:
			// A fenced control holds intake exclusively, so this finite queue
			// prefix cannot grow while it is drained.
			if call.drain {
				for len(q) > 0 {
					consume(<-q)
				}
			}
			appendPending()
			if failed {
				call.ack <- errors.New("receiver owner failed")
			} else {
				call.ack <- call.run()
			}
		case b, ok := <-q:
			if !ok {
				syncOwner()
				return
			}
			consume(b)
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
			if s.cfg.OnStopped != nil {
				s.cfg.OnStopped()
			}
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

// OwnerFor returns the stable producer used by unplaced TCP intake.
func OwnerFor(name string, n int) int {
	h := uint64(14695981039346656037)
	for i := 0; i < len(name); i++ {
		h ^= uint64(name[i])
		h *= 1099511628211
	}
	return int(h % uint64(n))
}
func (s *Server) read(c net.Conn) error {
	if s.cfg.Placement != nil {
		return s.readPlaced(c)
	}
	buffers := make([]*batch, len(s.queues))
	flush := func(i int) error {
		b := buffers[i]
		if b == nil || len(b.data) == 0 {
			return nil
		}
		select {
		case s.queues[i] <- b:
			buffers[i] = nil
			return nil
		case <-s.done:
			s.stats.rejected.Add(uint64(len(b.data)))
			buffers[i] = nil
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
	input := &fencedReader{Reader: countedReader{c, &s.stats.wire}, gate: &s.intake}
	s.intake.Lock()
	if s.decoded == nil {
		s.decoded = make(map[*fencedReader]func() error)
	}
	s.decoded[input] = flushAll
	s.intake.Unlock()
	defer func() {
		_ = flushAll()
		input.release()
		s.intake.Lock()
		delete(s.decoded, input)
		s.intake.Unlock()
	}()
	r := bufio.NewReaderSize(input, 64<<10)
	// Both protocols are selected on the same endpoint, as in receiver/tcp.
	head, err := r.Peek(1)
	if err == io.EOF {
		return nil
	}
	if err != nil {
		return err
	}

	var appendErr error
	add := func(ref *reference, value float64, ts int64) {
		if appendErr != nil {
			s.stats.rejected.Add(1)
			return
		}
		b := buffers[ref.owner]
		if b == nil {
			if pooled := s.batches.Get(); pooled != nil {
				b = pooled.(*batch)
			} else {
				b = &batch{names: make([]string, 0, s.cfg.BatchSize), data: make([]points.Point, 0, s.cfg.BatchSize)}
			}
			buffers[ref.owner] = b
		}
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
					*slot = &reference{name: n, owner: OwnerFor(n, len(s.queues))}
				}
				add((*slot).(*reference), value, ts)
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
	compressedPlain := false
	if head[0] == 0x28 {
		magic, peekErr := r.Peek(4)
		compressedPlain = peekErr == nil && string(magic) == "\x28\xb5\x2f\xfd"
	}
	if compressedPlain {
		z, err := zstd.NewReader(r, zstd.WithDecoderConcurrency(1), zstd.WithDecoderMaxMemory(64<<20))
		if err != nil {
			return err
		}
		defer z.Close()
		source = z
	}
	plain := bufio.NewReaderSize(source, 64<<10)
	var previous reference
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
				// Relay groups often contain several points for one name. Reuse
				// its immutable name and stable owner until the name changes.
				if !bytes.Equal(name, []byte(previous.name)) {
					n := string(name)
					previous = reference{name: n, owner: OwnerFor(n, len(s.queues))}
				}
				add(&previous, value, ts)
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

// Addr reports the bound listener address, including an allocated ephemeral port.
func (s *Server) Addr() net.Addr { return s.listener.Addr() }

// Done closes after Wait has stopped handlers and drained owners.
func (s *Server) Done() <-chan struct{} { return s.finished }

// fencedReader holds a shared intake fence while a decoder consumes one network
// buffer. Before waiting for more bytes it releases the fence. Maintenance can
// then flush the registered decoded prefix. Partial wire records have not been
// admitted; their callback runs
// only after the next buffer reacquires the fence.
type fencedReader struct {
	io.Reader
	gate *sync.RWMutex
	held bool
}

func (r *fencedReader) release() {
	if r.held {
		r.held = false
		r.gate.RUnlock()
	}
}
func (r *fencedReader) Read(b []byte) (int, error) {
	r.release()
	n, err := r.Reader.Read(b)
	r.gate.RLock()
	r.held = true
	return n, err
}
