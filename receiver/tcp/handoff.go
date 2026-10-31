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

	"github.com/go-graphite/go-carbon/points"
	"github.com/lomik/zapwriter"
)

// Handoff is a plaintext TCP receiver's input moved to another process: the
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
}

// handoffState tracks plaintext connections so Detach can stop each at a line
// boundary. Autodetected zstd frames can also move, since each frame is
// independent. Legacy compression and buffered points cannot move.
type handoffState struct {
	enabled    bool
	mu         sync.Mutex
	detaching  bool
	conns      map[*handoffConn]struct{}
	handlers   sync.WaitGroup
	acceptDone chan struct{}
	detached   []Conn
}

type handoffConn struct {
	conn net.Conn
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
	leftover := append([]byte(nil), partial...)
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
	tcpConn, ok := c.conn.(*net.TCPConn)
	if !ok {
		return false
	}
	file, err := tcpConn.File()
	if err != nil {
		rcv.logger.Warn("connection not handed over", zap.Error(err))
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
// plaintext or autodetected zstd input without buffering.
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

// NewInherited starts a plaintext TCP receiver on input another process handed
// over with Detach. Leftover bytes of each connection are parsed before
// anything read from it. It takes ownership of the handoff's files.
func NewInherited(name string, options *Options, store func(*points.Points), h *Handoff) (*TCP, error) {
	closeAll := func() {
		_ = h.Listener.Close()
		for _, c := range h.Conns {
			_ = c.File.Close()
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
	want, err := net.ResolveTCPAddr("tcp", options.Listen)
	if err != nil {
		closeAll()
		return nil, err
	}
	r := &TCP{out: store, name: name, logger: zapwriter.Logger(name), decompressor: newDecompressor("")}
	r.zstdMaxFrameSize = options.ZstdMaxFrameSize
	r.handoff.enabled = true
	conns := make([]Conn, 0, len(h.Conns))
	for _, c := range h.Conns {
		conn, err := net.FileConn(c.File)
		_ = c.File.Close()
		if err != nil {
			r.logger.Warn("inherited connection unusable", zap.Error(err))
			continue
		}
		conns = append(conns, Conn{Leftover: c.Leftover, conn: conn})
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
			_ = c.conn.Close()
		}
		return nil, err
	}
	return r, nil
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
