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

	"github.com/go-graphite/go-carbon/internal/namefp"
	"github.com/go-graphite/go-carbon/points"
	"github.com/go-graphite/go-carbon/receiver/parse"
	"github.com/go-graphite/go-carbon/receiver/stream/codec"
	"github.com/klauspost/compress/zstd"
)

// Owner is called by exactly one goroutine. Append borrows its arguments only
// until return; plaintext names share one string per batch, so an owner
// that keeps a name must clone it. hashes[i] is namefp.Route(names[i]), the hash
// that routed the name to this owner. Seal+Drain must journal-sync before
// publishing to the core.
type Owner interface {
	Append(names []string, hashes []uint64, data []points.Point, sequence []uint64, clock int64) error
	Seal() error
	Drain() error
}

// ByteOwner accepts plaintext names without making a batch string. Each name
// spans text[ends[i-1]:ends[i]] (the first starts at zero). Like Append, it
// borrows all arguments until return; retained names must be copied.
type ByteOwner interface {
	AppendBytes(text []byte, ends []int, hashes []uint64, data []points.Point, sequence []uint64, clock int64) error
}

type Config struct {
	OnStopped func() // called once after Wait drains handlers and owners

	PlacementKeys                           map[string][]byte
	Placement                               PlacedSink
	PlacementKey                            []byte
	PlacementProducer                       string
	BatchSize, MaxConnections, MaxLineBytes int
	// BufferBytes bounds what one connection holds before handing it to the
	// owners: name text plus rowBytes per point, summed over all owners.
	BufferBytes  int
	SyncInterval time.Duration
	Stream       codec.Options
	Logger       *slog.Logger
}

// rowBytes is the per-point cost of a batch: name header, hash, point and end.
const rowBytes = 16 + 8 + 16 + 8

// batch holds each name either as text bytes (plaintext; ends set, names
// empty) or as a string (codec frames; names set).
type batch struct {
	names  []string
	hashes []uint64 // namefp.Route of each name, computed once for routing
	data   []points.Point
	text   []byte // plaintext name bytes, borrowed by ByteOwner until return
	ends   []int  // end of each plaintext name in text; empty for codec batches
}

// named converts plaintext names with one allocation per batch instead of one
// per line. The substrings share that immutable string, never the reused text.
func (b *batch) named() {
	if len(b.ends) == 0 {
		return
	}
	text, start := string(b.text), 0
	for _, end := range b.ends {
		b.names = append(b.names, text[start:end])
		start = end
	}
	b.text, b.ends = b.text[:0], b.ends[:0]
}

// add copies p to the end of b in one bulk copy per column. A text batch
// keeps every name as bytes; any other batch keeps strings, so p must be
// named first.
func (b *batch) add(p *batch, text bool) {
	switch {
	case text && len(p.ends) != 0:
		offset := len(b.text)
		b.text = append(b.text, p.text...)
		for _, end := range p.ends {
			b.ends = append(b.ends, offset+end)
		}
	case text:
		for _, name := range p.names {
			b.text = append(b.text, name...)
			b.ends = append(b.ends, len(b.text))
		}
	default:
		b.names = append(b.names, p.names...)
	}
	b.hashes = append(b.hashes, p.hashes...)
	b.data = append(b.data, p.data...)
}

func (b *batch) reset() {
	clear(b.names)
	b.names, b.hashes, b.data, b.text, b.ends = b.names[:0], b.hashes[:0], b.data[:0], b.text[:0], b.ends[:0]
}

// inbox is one owner's input, shared by all connections, like a VictoriaMetrics
// raw-rows shard. A connection copies its whole batch (at most BatchSize
// points) in under mu and wakes the owner's worker once the inbox holds
// BatchSize points; until the worker takes it, further appends wait. The
// worker swaps the inbox (fewer than 2*BatchSize points) for its empty spare
// and calls the owner once, without mu.
type inbox struct {
	mu    sync.Mutex
	space sync.Cond // broadcast when the worker takes the inbox or the server stops
	b     batch
	text  bool          // the owner is a ByteOwner: names stay bytes
	full  chan struct{} // capacity 1: the inbox reached BatchSize
}

func newInbox(o Owner) *inbox {
	_, text := o.(ByteOwner)
	in := &inbox{text: text, full: make(chan struct{}, 1)}
	in.space.L = &in.mu
	return in
}

type ownerCall struct {
	run func() error
	ack chan error
}
type counters struct{ accepted, rejected, parse, transport, sink, wire, connections atomic.Uint64 }
type Stats struct{ Accepted, Rejected, ParseErrors, TransportErrors, SinkErrors, WireBytes, Connections uint64 }

// Server shares a buffer-level maintenance fence. The mutex protects connection
// lifetime and the first terminal error; every owner has one inbox.
type Server struct {
	cfg      Config
	listener net.Listener
	inboxes  []*inbox
	closing  chan struct{} // closed once every connection handler has returned
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
	if cfg.MaxConnections == 0 {
		cfg.MaxConnections = 128
	}
	if cfg.MaxLineBytes == 0 {
		cfg.MaxLineBytes = 1 << 20
	}
	if cfg.BufferBytes == 0 {
		cfg.BufferBytes = 1 << 20
	}
	if cfg.SyncInterval == 0 {
		cfg.SyncInterval = time.Second
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	// An inbox holds fewer than 2*BatchSize points: within an owner call's
	// 32768-point limit.
	if cfg.BatchSize < 1 || cfg.BatchSize > 16384 || cfg.MaxConnections < 1 || cfg.MaxLineBytes < 1 || cfg.BufferBytes < 1 || cfg.SyncInterval < 0 {
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
	s := &Server{cfg: cfg, listener: l, owners: owners, done: make(chan struct{}), closing: make(chan struct{}), finished: make(chan struct{}), conns: make(map[net.Conn]struct{})}
	for _, o := range owners {
		if o == nil {
			return nil, errors.New("nil owner")
		}
	}
	for _, o := range owners {
		s.inboxes = append(s.inboxes, newInbox(o))
		s.controls = append(s.controls, make(chan ownerCall))
	}
	for i, o := range owners {
		s.workers.Add(1)
		go s.work(i, o)
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
		for _, in := range s.inboxes {
			in.mu.Lock()
			in.space.Broadcast()
			in.mu.Unlock()
		}
	})
}

func (s *Server) stopped() bool {
	select {
	case <-s.done:
		return true
	default:
		return false
	}
}

// handOff copies a connection's batch for owner i into its inbox. A full inbox
// blocks the connection until the worker takes it; input is dropped (and
// counted) only once the server has stopped.
func (s *Server) handOff(i int, b *batch) error {
	in := s.inboxes[i]
	if !in.text {
		b.named()
	}
	in.mu.Lock()
	defer in.mu.Unlock()
	for len(in.b.data) >= s.cfg.BatchSize && !s.stopped() {
		in.space.Wait()
	}
	if s.stopped() {
		s.stats.rejected.Add(uint64(len(b.data)))
		return errors.New("receiver stopped")
	}
	in.b.add(b, in.text)
	if len(in.b.data) >= s.cfg.BatchSize {
		select {
		case in.full <- struct{}{}:
		default:
		}
	}
	return nil
}
func (s *Server) work(i int, o Owner) {
	defer s.workers.Done()
	tick := time.NewTicker(s.cfg.SyncInterval)
	defer tick.Stop()
	in, control := s.inboxes[i], s.controls[i]
	var seq uint64
	failed := false
	byteOwner, _ := o.(ByteOwner)
	var spare batch // the inbox taken last, emptied for the next swap
	sequence := make([]uint64, 0, s.cfg.BatchSize)
	// take swaps the inbox (fewer than 2*BatchSize points) for the empty spare
	// and appends it outside the lock in one owner call. Per-name order survives:
	// appends keep arrival order within the inbox.
	take := func() {
		in.mu.Lock()
		in.b, spare = spare, in.b
		in.space.Broadcast()
		in.mu.Unlock()
		b := &spare
		defer b.reset()
		if len(b.data) == 0 {
			return
		}
		if failed {
			s.stats.rejected.Add(uint64(len(b.data)))
			return
		}
		sequence = sequence[:0]
		for range b.data {
			seq++
			sequence = append(sequence, seq)
		}
		var err error
		if byteOwner != nil {
			err = byteOwner.AppendBytes(b.text, b.ends, b.hashes, b.data, sequence, time.Now().Unix())
		} else {
			err = o.Append(b.names, b.hashes, b.data, sequence, time.Now().Unix())
		}
		if err != nil {
			failed = true
			s.stats.rejected.Add(uint64(len(b.data)))
			s.stats.sink.Add(1)
			s.fail(err)
		} else {
			s.stats.accepted.Add(uint64(len(b.data)))
		}
	}
	syncOwner := func() {
		take()
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
		case call := <-control:
			// A fenced control holds intake exclusively and has flushed every
			// connection, so the inbox it takes holds all decoded input.
			take()
			if failed {
				call.ack <- errors.New("receiver owner failed")
			} else {
				call.ack <- call.run()
			}
		case <-in.full:
			take()
		case <-tick.C:
			syncOwner()
		case <-s.closing:
			syncOwner()
			return
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
			close(s.closing)
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
	hash  uint64
	owner int
}

// connBuffers is one connection's per-owner batches and the bytes they hold.
// held changes on every point, so the struct fills whole cache lines and never
// shares one with another connection (as VictoriaMetrics pads raw-rows shards).
type connBuffers struct {
	s       *Server
	buffers []batch
	held    int
	_       [128 - 8 - 24 - 8]byte
}

func (c *connBuffers) flush(i int) error {
	b := &c.buffers[i]
	if len(b.data) == 0 {
		return nil
	}
	c.held -= len(b.text) + len(b.data)*rowBytes
	err := c.s.handOff(i, b)
	b.reset()
	return err
}

func (c *connBuffers) flushAll() error {
	var err error
	for i := range c.buffers {
		err = errors.Join(err, c.flush(i))
	}
	return err
}

// OwnerFor returns the stable producer used by unplaced TCP intake.
func OwnerFor(name string, n int) int { return int(namefp.Route(name) % uint64(n)) }
func (s *Server) read(c net.Conn) error {
	if s.cfg.Placement != nil {
		return s.readPlaced(c)
	}
	// One byte budget per connection, shared by its per-owner batches: a busy
	// stream rarely ends a read on a line boundary, so without it every owner
	// batch of every connection would fill to BatchSize names. A full owner
	// inbox blocks this connection; input is never dropped.
	conn := &connBuffers{s: s, buffers: make([]batch, len(s.inboxes))}
	flush, flushAll := conn.flush, conn.flushAll
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
	buffer := func(owner int) *batch { return &conn.buffers[owner] } // grows to what the byte budget lets in
	added := func(owner int, b *batch, text int, value float64, ts int64) {
		b.data = append(b.data, points.Point{Value: value, Timestamp: ts})
		if conn.held += text + rowBytes; len(b.data) >= s.cfg.BatchSize {
			appendErr = flush(owner)
		} else if conn.held >= s.cfg.BufferBytes {
			appendErr = flushAll()
		}
	}
	add := func(ref *reference, value float64, ts int64) {
		if appendErr != nil {
			s.stats.rejected.Add(1)
			return
		}
		b := buffer(ref.owner)
		b.names = append(b.names, ref.name)
		b.hashes = append(b.hashes, ref.hash)
		added(ref.owner, b, 0, value, ts)
	}
	// addText copies a borrowed plaintext name into its owner's batch text.
	addText := func(owner int, hash uint64, name []byte, value float64, ts int64) {
		if appendErr != nil {
			s.stats.rejected.Add(1)
			return
		}
		b := buffer(owner)
		b.text = append(b.text, name...)
		b.ends = append(b.ends, len(b.text))
		b.hashes = append(b.hashes, hash)
		added(owner, b, len(name), value, ts)
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
					h := namefp.Route(n)
					*slot = &reference{name: n, hash: h, owner: int(h % uint64(len(s.inboxes)))}
				}
				add((*slot).(*reference), value, ts)
			})
			if errors.Is(err, io.EOF) {
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
	var previous []byte // last plaintext name, reused while consecutive lines repeat it
	owner, hash := 0, uint64(0)
	for {
		line, err := plain.ReadSlice('\n')
		if errors.Is(err, bufio.ErrBufferFull) {
			joined := append([]byte(nil), line...)
			for errors.Is(err, bufio.ErrBufferFull) {
				if len(joined) > s.cfg.MaxLineBytes {
					return fmt.Errorf("plaintext line limit: %d", s.cfg.MaxLineBytes)
				}
				line, err = plain.ReadSlice('\n')
				joined = append(joined, line...)
			}
			line = joined
		}
		if errors.Is(err, io.EOF) && len(line) == 0 {
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
				// its stable owner until the name changes.
				if previous == nil || !bytes.Equal(name, previous) {
					previous = append(previous[:0], name...)
					hash = namefp.Route(previous)
					owner = int(hash % uint64(len(s.inboxes)))
				}
				addText(owner, hash, name, value, ts)
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
