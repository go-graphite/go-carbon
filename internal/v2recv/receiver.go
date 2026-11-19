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
// until return; plaintext names share one string per socket batch, so an owner
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
	for i, end := range b.ends {
		b.names[i] = text[start:end]
		start = end
	}
	b.text, b.ends = b.text[:0], b.ends[:0]
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
	byteOwner, _ := o.(ByteOwner)
	sequence := make([]uint64, 0, s.cfg.BatchSize)
	appendBatch := func(b *batch) {
		sequence = sequence[:0]
		for range b.data {
			seq++
			sequence = append(sequence, seq)
		}
		var err error
		if byteOwner != nil && len(b.ends) != 0 {
			err = byteOwner.AppendBytes(b.text, b.ends, b.hashes, b.data, sequence, time.Now().Unix())
		} else {
			b.named()
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
	appendPending := func() {
		if failed || len(pending.data) == 0 {
			return
		}
		appendBatch(&pending)
		clear(pending.names)
		pending.names, pending.hashes, pending.data = pending.names[:0], pending.hashes[:0], pending.data[:0]
		pending.text, pending.ends = pending.text[:0], pending.ends[:0]
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
		originalNames, originalHashes, originalData := b.names, b.hashes, b.data
		recycle := func() {
			clear(originalNames)
			b.names, b.hashes, b.data = originalNames[:0], originalHashes[:0], originalData[:0]
			b.text, b.ends = b.text[:0], b.ends[:0]
			s.batches.Put(b)
		}
		if failed {
			s.stats.rejected.Add(uint64(len(b.data)))
			recycle()
			return
		}
		// Codec names already own immutable strings. Keep their batching path
		// separate when connections with different protocols share an owner.
		if len(pending.data) != 0 && (len(pending.ends) == 0) != (len(b.ends) == 0) {
			appendPending()
			if failed {
				s.stats.rejected.Add(uint64(len(b.data)))
				recycle()
				return
			}
		}
		if len(pending.data) == 0 && len(b.data) == s.cfg.BatchSize {
			appendBatch(b)
			b.names, b.hashes, b.data = b.names[:0], b.hashes[:0], b.data[:0]
		}
		textStart, nameIndex := 0, 0
		for len(b.data) > 0 {
			n := min(len(b.data), s.cfg.BatchSize-len(pending.data))
			if len(b.ends) != 0 {
				ends := b.ends[nameIndex : nameIndex+n]
				offset := len(pending.text) - textStart
				pending.text = append(pending.text, b.text[textStart:ends[n-1]]...)
				for _, end := range ends {
					pending.ends = append(pending.ends, offset+end)
				}
				textStart = ends[n-1]
				nameIndex += n
			}
			pending.names = append(pending.names, b.names[:n]...)
			pending.hashes = append(pending.hashes, b.hashes[:n]...)
			pending.data = append(pending.data, b.data[:n]...)
			b.names, b.hashes, b.data = b.names[n:], b.hashes[n:], b.data[n:]
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
	hash  uint64
	owner int
}

// OwnerFor returns the stable producer used by unplaced TCP intake.
func OwnerFor(name string, n int) int { return int(namefp.Route(name) % uint64(n)) }
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
		if _, ok := s.owners[i].(ByteOwner); !ok {
			b.named()
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
	buffer := func(owner int) *batch {
		b := buffers[owner]
		if b == nil {
			if pooled := s.batches.Get(); pooled != nil {
				b = pooled.(*batch)
			} else {
				b = &batch{names: make([]string, 0, s.cfg.BatchSize), hashes: make([]uint64, 0, s.cfg.BatchSize), data: make([]points.Point, 0, s.cfg.BatchSize)}
			}
			buffers[owner] = b
		}
		return b
	}
	added := func(owner int, b *batch, value float64, ts int64) {
		b.data = append(b.data, points.Point{Value: value, Timestamp: ts})
		if len(b.data) >= s.cfg.BatchSize {
			appendErr = flush(owner)
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
		added(ref.owner, b, value, ts)
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
		b.names = append(b.names, "")
		b.hashes = append(b.hashes, hash)
		added(owner, b, value, ts)
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
					*slot = &reference{name: n, hash: h, owner: int(h % uint64(len(s.queues)))}
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
					owner = int(hash % uint64(len(s.queues)))
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
