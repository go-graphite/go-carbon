package tcp

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"

	"github.com/go-graphite/go-carbon/internal/handoff"
	"github.com/go-graphite/go-carbon/points"
	"github.com/lomik/zapwriter"
)

// Handoff is a TCP receiver's plaintext or stream input moved to another process: the
// listening socket and the accepted connections, each with the bytes read
// from it but not yet parsed.
type Handoff struct {
	Listener *os.File
	Conns    []Conn
}

// Conn is one accepted connection of a Handoff.
type Conn struct {
	File     *os.File
	Leftover []byte
	conn     net.Conn
	ahead    *readAheadConn
}

// handoffState tracks plaintext at line boundaries, independent zstd frames,
// and stream input at committed frame boundaries. Legacy compression and
// buffered points cannot move.
type handoffState struct {
	enabled    bool
	mu         sync.Mutex
	detaching  bool
	conns      map[*handoffConn]struct{}
	handlers   sync.WaitGroup
	acceptDone chan struct{}
	detached   []Conn
	replaying  atomic.Bool
}

type handoffConn struct {
	conn  net.Conn
	ahead *readAheadConn
	// mu orders read deadline updates with interrupt, so a refreshed deadline
	// cannot undo it.
	mu     sync.Mutex
	detach atomic.Bool
}

func (h *handoffState) track(conn net.Conn) *handoffConn {
	c := &handoffConn{conn: conn}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.conns == nil {
		h.conns = make(map[*handoffConn]struct{})
	}
	h.conns[c] = struct{}{}
	if h.detaching {
		// Accepted just before Detach closed the listener.
		c.detach.Store(true)
	}
	h.handlers.Add(1)
	return c
}

func (h *handoffState) untrack(c *handoffConn) {
	h.mu.Lock()
	delete(h.conns, c)
	h.mu.Unlock()
	h.handlers.Done()
}

func (c *handoffConn) detaching() bool { return c != nil && c.detach.Load() }

func (c *handoffConn) setReadDeadline(conn net.Conn, t time.Time) {
	if c == nil {
		_ = conn.SetReadDeadline(t)
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.detach.Load() {
		_ = c.conn.SetReadDeadline(t)
	}
}

// interrupt makes a blocked read return so the handler sees the detach.
func (c *handoffConn) interrupt() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.detach.Store(true)
	_ = c.conn.SetReadDeadline(time.Unix(1, 0))
}

// detach hands the connection over instead of closing it: partial is the
// unterminated line a read returned with its error, followed by what reader
// buffered and what rest still holds of inherited input. It returns false if
// the socket cannot be duplicated; the caller then closes it, losing its
// unparsed input as a normal stop would.
func (rcv *TCP) detach(c *handoffConn, reader *bufio.Reader, rest *bytes.Reader, partial []byte) bool {
	return rcv.detachOwned(c, reader, rest, append([]byte(nil), partial...))
}

// Stream snapshots already own their backing storage. Do not copy the entire
// dictionary again just to append the small amount of buffered wire input.
func (rcv *TCP) detachOwned(c *handoffConn, reader *bufio.Reader, rest *bytes.Reader, leftover []byte) bool {
	if n := reader.Buffered(); n > 0 {
		buffered, _ := reader.Peek(n)
		leftover = append(leftover, buffered...)
	}
	if rest != nil && rest.Len() > 0 {
		tail := make([]byte, rest.Len())
		_, _ = rest.Read(tail)
		leftover = append(leftover, tail...)
	}
	return rcv.detachBytes(c, leftover)
}

func (rcv *TCP) detachBytes(c *handoffConn, leftover []byte) bool {
	if c.ahead != nil {
		leftover = c.ahead.pending(leftover)
	}
	tcpConn, ok := c.conn.(*net.TCPConn)
	if !ok {
		handoff.CloseInputConn(c.conn, len(leftover), rcv.logger, "detach_failed")
		return false
	}
	file, err := tcpConn.File()
	if err != nil {
		rcv.logger.Warn("connection not handed over", zap.Error(err))
		handoff.CloseInputConn(c.conn, len(leftover), rcv.logger, "detach_failed")
		return false
	}
	rcv.handoff.mu.Lock()
	rcv.handoff.detached = append(rcv.handoff.detached, Conn{File: file, Leftover: leftover})
	rcv.handoff.mu.Unlock()
	// The duplicate keeps the socket open.
	_ = c.conn.Close()
	return true
}

// Handoffable reports whether Detach can move this receiver's connections:
// plaintext, autodetected zstd or stream input without buffering.
func (rcv *TCP) Handoffable() bool { return rcv.handoff.enabled && rcv.listener != nil }

// Detach stops the receiver but leaves its listening socket and accepted
// connections open, and returns them with the bytes read but not yet parsed.
// Every point parsed earlier has been passed to the store when it returns, so
// another process can continue reading exactly where this one stopped. A
// connection that cannot be duplicated is closed as by Stop.
func (rcv *TCP) Detach() (*Handoff, error) {
	h := &rcv.handoff
	if !rcv.Handoffable() {
		return nil, errors.New("receiver cannot hand over its connections")
	}
	listener, err := rcv.listener.File()
	if err != nil {
		return nil, err
	}
	h.mu.Lock()
	h.detaching = true
	h.mu.Unlock()
	// Connections still in the accept backlog stay with the socket.
	_ = rcv.listener.Close()
	<-h.acceptDone
	h.mu.Lock()
	for c := range h.conns {
		c.interrupt()
	}
	h.mu.Unlock()
	h.handlers.Wait()
	out := &Handoff{Listener: listener, Conns: h.detached}
	h.detached = nil
	rcv.Stop()
	return out, nil
}

// NewInherited starts a plaintext/stream TCP receiver on input another process handed
// over with Detach. Leftover bytes of each connection are parsed before
// anything read from it. It takes ownership of the handoff's files.
// Call FinishInputReplay once ordered recovery is complete to release startup
// read-ahead; handlers finish restoring their dictionaries before switching.
func NewInherited(name string, options *Options, store func(*points.Points), h *Handoff) (*TCP, error) {
	return newInherited(name, options, store, h, nil)
}

// NewInheritedCapture drains inherited input into a durable startup queue before
// storage and its checkpoint are ready. It never calls a cache sink.
func NewInheritedCapture(name string, options *Options, h *Handoff, capture Capture) (*TCP, error) {
	return newInherited(name, options, nil, h, capture)
}

func newInherited(name string, options *Options, store func(*points.Points), h *Handoff, capture Capture) (*TCP, error) {
	closeAll := func() {
		_ = h.Listener.Close()
		for _, c := range h.Conns {
			handoff.CloseInputConn(c.File, len(c.Leftover), zapwriter.Logger(name), "inherited_start_failed")
		}
	}
	if options.BufferSize != 0 || options.Compression != "" {
		closeAll()
		return nil, errors.New("inherited input requires plaintext without buffering")
	}
	if err := validateZstdLimit(options.ZstdMaxFrameSize); err != nil {
		closeAll()
		return nil, err
	}
	readAheadBytes, err := readAheadLimit(options.HandoffReadAheadBytes)
	if err != nil {
		closeAll()
		return nil, err
	}
	want, err := net.ResolveTCPAddr("tcp", options.Listen)
	if err != nil {
		closeAll()
		return nil, err
	}
	r := &TCP{out: store, name: name, logger: zapwriter.Logger(name), decompressor: newDecompressor(""), capture: capture}
	r.cacheLookupBatch = options.CacheLookupBatch
	r.handoffReadAheadBytes = readAheadBytes
	r.zstdMaxFrameSize = options.ZstdMaxFrameSize
	if err := r.configureStream(options); err != nil {
		closeAll()
		return nil, err
	}
	r.handoff.enabled = true
	r.handoff.replaying.Store(capture == nil)
	conns := make([]Conn, 0, len(h.Conns))
	for _, c := range h.Conns {
		conn, err := net.FileConn(c.File)
		if err != nil {
			handoff.CloseInputConn(c.File, len(c.Leftover), r.logger, "inherited_connection_unusable")
			r.logger.Warn("inherited connection unusable", zap.Error(err))
			continue
		}
		_ = c.File.Close() // conn owns the duplicate
		// The pump starts before the tracked handler configures its deadline.
		// Give its initial read an explicitly unexpired deadline.
		_ = conn.SetReadDeadline(time.Time{})
		var ahead *readAheadConn
		if capture == nil {
			ahead = newReadAheadConnLimit(conn, readAheadBytes)
		}
		conns = append(conns, Conn{Leftover: c.Leftover, conn: conn, ahead: ahead})
	}
	err = r.listen(func() (*net.TCPListener, error) {
		ln, err := net.FileListener(h.Listener)
		_ = h.Listener.Close()
		if err != nil {
			return nil, err
		}
		tcpListener, ok := ln.(*net.TCPListener)
		if !ok || !sameAddr(tcpListener.Addr(), want) {
			_ = ln.Close()
			return nil, fmt.Errorf("inherited listener %s does not match %s", ln.Addr(), options.Listen)
		}
		return tcpListener, nil
	}, conns)
	if err != nil {
		for _, c := range conns {
			var pending []byte
			if c.ahead != nil {
				pending = c.ahead.pending(nil)
			}
			handoff.CloseInputConn(c.conn, len(c.Leftover)+len(pending), r.logger, "inherited_start_failed")
		}
		return nil, err
	}
	return r, nil
}

// FinishInputReplay releases startup read-ahead once the input tail is applied.
// Each handler drains its buffered bytes before returning to direct TCP reads,
// and releases its ring. Dictionary replay still precedes all socket parsing.
func (rcv *TCP) FinishInputReplay() {
	rcv.handoff.replaying.Store(false)
	rcv.handoff.mu.Lock()
	defer rcv.handoff.mu.Unlock()
	for c := range rcv.handoff.conns {
		if c.ahead != nil {
			c.ahead.finish()
		}
	}
}

func sameAddr(got net.Addr, want *net.TCPAddr) bool {
	g, ok := got.(*net.TCPAddr)
	if !ok || g.Port != want.Port {
		return false
	}
	if len(want.IP) == 0 || want.IP.IsUnspecified() {
		return g.IP.IsUnspecified()
	}
	return g.IP.Equal(want.IP)
}
