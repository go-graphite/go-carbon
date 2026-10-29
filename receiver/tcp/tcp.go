package tcp

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync/atomic"
	"time"

	"go.uber.org/zap"

	"github.com/go-graphite/go-carbon/helper"
	"github.com/go-graphite/go-carbon/internal/phasecpu"
	"github.com/go-graphite/go-carbon/points"
	"github.com/go-graphite/go-carbon/receiver"
	"github.com/go-graphite/go-carbon/receiver/parse"
	"github.com/klauspost/compress/gzip"
	"github.com/klauspost/compress/snappy"
	"github.com/lomik/graphite-pickle/framing"
	"github.com/lomik/zapwriter"
	"github.com/prometheus/client_golang/prometheus"
)

func Register() {
	receiver.Register(
		"tcp",
		func() interface{} { return NewOptions() },
		func(name string, options interface{}, store func(*points.Points)) (receiver.Receiver, error) {
			return newTCP(name, options.(*Options), store)
		},
	)

	receiver.Register(
		"pickle",
		func() interface{} { return NewFramingOptions() },
		func(name string, options interface{}, store func(*points.Points)) (receiver.Receiver, error) {
			return newFraming("pickle", name, options.(*FramingOptions), store)
		},
	)

	receiver.Register(
		"protobuf",
		func() interface{} { return NewFramingOptions() },
		func(name string, options interface{}, store func(*points.Points)) (receiver.Receiver, error) {
			return newFraming("protobuf", name, options.(*FramingOptions), store)
		},
	)
}

type Options struct {
	Listen      string `toml:"listen"`
	Enabled     bool   `toml:"enabled"`
	BufferSize  int    `toml:"buffer-size"`
	Compression string `toml:"compression"`
}

func NewOptions() *Options {
	return &Options{
		Listen:     ":2003",
		Enabled:    true,
		BufferSize: 0,
	}
}

type FramingOptions struct {
	Listen         string `toml:"listen"`
	MaxMessageSize uint32 `toml:"max-message-size"`
	Enabled        bool   `toml:"enabled"`
	BufferSize     int    `toml:"buffer-size"`
}

func NewFramingOptions() *FramingOptions {
	return &FramingOptions{
		Listen:         ":2004",
		MaxMessageSize: 67108864, // 64 Mb
		Enabled:        true,
		BufferSize:     0,
	}
}

const plainReadBufferSize = 64 << 10

// TCP receive metrics from TCP connections
type TCP struct {
	helper.Stoppable
	out                 func(*points.Points)
	name                string // name for store metrics
	maxMessageSize      uint32
	metricsReceived     uint32
	promMetricsReceived prometheus.Counter
	errors              uint32 // read and framing failures
	parseErrors         uint32 // rejected metric lines or framed messages
	active              int32  // counter
	listener            *net.TCPListener
	isFraming           bool
	frameParser         func(body []byte) ([]*points.Points, error)
	phase               phasecpu.Phase
	buffer              chan *points.Points
	logger              *zap.Logger
	decompressor        decompressor
	handoff             handoffState
	pointOut            atomic.Pointer[func(name []byte, value float64, timestamp int64)]
	pointsOut           atomic.Pointer[func(name []byte, data []points.Point)]
}

// SetPointOut configures the point callback for unbuffered plain-text input.
// Connections already running before the initial callback is configured adopt
// it too. out must not retain name. A buffered receiver keeps using its store.
func (rcv *TCP) SetPointOut(out func(name []byte, value float64, timestamp int64)) {
	if rcv.buffer == nil {
		rcv.pointOut.Store(&out)
	}
}

// SetPointsOut batches consecutive points already in a connection's read
// buffer. out must not retain name or data. Buffered receivers keep their store.
func (rcv *TCP) SetPointsOut(out func(name []byte, data []points.Point)) {
	if rcv.buffer == nil {
		rcv.pointsOut.Store(&out)
	}
}

// Addr returns binded socket address. For bind port 0 in tests
func (rcv *TCP) Addr() net.Addr {
	if rcv.listener == nil {
		return nil
	}
	return rcv.listener.Addr()
}

func newTCP(name string, options *Options, store func(*points.Points)) (*TCP, error) {
	if !options.Enabled {
		return nil, nil
	}

	addr, err := net.ResolveTCPAddr("tcp", options.Listen)
	if err != nil {
		return nil, err
	}

	r := &TCP{
		out:    store,
		name:   name,
		logger: zapwriter.Logger(name),
	}

	if options.BufferSize > 0 {
		r.buffer = make(chan *points.Points, options.BufferSize)
	}

	r.decompressor = newDecompressor(options.Compression)
	r.handoff.enabled = options.BufferSize == 0 && options.Compression == ""

	err = r.Listen(addr)
	if err != nil {
		return nil, err
	}

	return r, err
}

func newFraming(parser string, name string, options *FramingOptions, store func(*points.Points)) (*TCP, error) {
	if !options.Enabled {
		return nil, nil
	}

	addr, err := net.ResolveTCPAddr("tcp", options.Listen)
	if err != nil {
		return nil, err
	}

	r := &TCP{
		out:            store,
		phase:          phasecpu.Receiver(parser),
		name:           name,
		logger:         zapwriter.Logger(name),
		maxMessageSize: options.MaxMessageSize,
		isFraming:      true,
	}

	switch parser {
	case "pickle":
		r.frameParser = parse.Pickle
	case "protobuf":
		r.frameParser = parse.Protobuf
	default:
		return nil, fmt.Errorf("unknown frame parser %#v", parser)
	}

	if options.BufferSize > 0 {
		r.buffer = make(chan *points.Points, options.BufferSize)
	}

	err = r.Listen(addr)
	if err != nil {
		return nil, err
	}

	return r, err
}

func (rcv *TCP) HandleConnection(conn net.Conn) {
	rcv.handlePlain(conn, nil, nil)
}

// handlePlain reads newline-separated points. leftover holds bytes a previous
// owner read from conn but did not parse; they come first. A tracked
// connection can be detached (see Detach) instead of closed.
func (rcv *TCP) handlePlain(conn net.Conn, leftover []byte, tracked *handoffConn) {
	phasecpu.Set(phasecpu.ReceiverTCP)
	defer phasecpu.Set(phasecpu.Other)
	atomic.AddInt32(&rcv.active, 1)
	defer atomic.AddInt32(&rcv.active, -1)

	detached := false
	defer func() {
		if !detached {
			conn.Close()
		}
	}()

	var src io.Reader = conn
	var rest *bytes.Reader
	if len(leftover) > 0 {
		rest = bytes.NewReader(leftover)
		src = io.MultiReader(rest, conn)
	}
	bconn, err := rcv.decompressor(src)
	if err != nil {
		rcv.logger.Error("failed init decompressor", zap.Error(err))
		return
	}
	reader := bufio.NewReaderSize(bconn, plainReadBufferSize)

	finished := make(chan bool)
	defer close(finished)

	rcv.Go(func(exit chan bool) {
		select {
		case <-finished:
			return
		case <-exit:
			conn.Close()
			return
		}
	})

	lastDeadline := time.Now()
	readTimeout := 2 * time.Minute
	tracked.setReadDeadline(conn, lastDeadline.Add(readTimeout))

	// Count locally and publish before any read that may block: every
	// connection incrementing the shared counter per point serializes them.
	var received uint32
	defer func() { atomic.AddUint32(&rcv.metricsReceived, received) }()

	var pointOut func(name []byte, value float64, timestamp int64)
	if out := rcv.pointOut.Load(); out != nil {
		pointOut = *out
	}

	var pointsOut func([]byte, []points.Point)
	if out := rcv.pointsOut.Load(); out != nil {
		pointsOut = *out
	}
	var batchName []byte
	var batch []points.Point
	if pointsOut != nil {
		batch = make([]points.Point, 0, 64)
	}
	flush := func() {
		if len(batch) > 0 {
			pointsOut(batchName, batch)
			batch, batchName = batch[:0], nil
		}
	}
	defer flush()

	for lines := 0; ; lines++ {
		var bufferedLine []byte
		if len(batch) > 0 {
			buffered, _ := reader.Peek(reader.Buffered())
			// ReadSlice may refill even with a partial next line buffered.
			end := bytes.IndexByte(buffered, '\n')
			if tracked.detaching() || received >= 1024 || end < 0 {
				flush()
			} else {
				bufferedLine = buffered[:end+1]
			}
		}
		if received != 0 && (reader.Buffered() == 0 || received >= 1024) {
			atomic.AddUint32(&rcv.metricsReceived, received)
			received = 0
		}
		if tracked.detaching() {
			flush()
			detached = rcv.detach(tracked, reader, rest, nil)
			return
		}

		// Reading the clock per line is measurable at millions of points per
		// second; the deadline only needs refreshing well within readTimeout.
		if lines%64 == 0 || reader.Buffered() == 0 {
			now := time.Now()
			if now.Sub(lastDeadline) > (readTimeout / 4) {
				tracked.setReadDeadline(conn, now.Add(readTimeout))
				lastDeadline = now
			}
		}

		line := bufferedLine
		var err error
		if line != nil {
			_, err = reader.Discard(len(line))
		} else {
			line, err = readPlainLine(reader)
		}

		if err != nil {
			flush()
			if tracked.detaching() {
				detached = rcv.detach(tracked, reader, rest, line)
				return
			}
			if err == io.EOF { //nolint:errorlint // Preserve legacy direct EOF classification; wrapped transport errors are read errors.
				if len(line) > 0 {
					rcv.logger.Warn("unfinished line", zap.String("line", string(line)))
				}
			} else {
				atomic.AddUint32(&rcv.errors, 1)
				rcv.logger.Error("read error", zap.Error(err))
			}
			break
		}
		if len(line) > 0 { // skip empty lines

			name, value, timestamp, err := parse.PlainLine(line)

			if err != nil {
				flush()
				atomic.AddUint32(&rcv.parseErrors, 1)
				rcv.logger.Debug("parse failed",
					zap.Error(err),
					zap.String("peer", conn.RemoteAddr().String()),
				)
			} else {
				received++
				// Inherited handlers start before the application sets pointOut.
				// Adopt it even if it arrived while ReadLine was blocked, then
				// retain the callback without an atomic load per subsequent point.
				if pointOut == nil && pointsOut == nil && rcv.buffer == nil {
					if out := rcv.pointOut.Load(); out != nil {
						pointOut = *out
					}
				}
				switch {
				case pointsOut != nil:
					if len(batch) > 0 && (!bytes.Equal(batchName, name) || len(batch) == cap(batch)) {
						flush()
					}
					batchName = name
					batch = append(batch, points.Point{Value: value, Timestamp: timestamp})
				case pointOut != nil:
					pointOut(name, value, timestamp)
				default:
					rcv.out(points.OnePoint(string(name), value, timestamp))
				}
			}
		}
	}
}

func readPlainLine(reader *bufio.Reader) ([]byte, error) {
	line, err := reader.ReadSlice('\n')
	if err != bufio.ErrBufferFull { //nolint:errorlint // Only the direct ReadSlice sentinel requests another fragment; wrapped errors must be returned.
		return line, err
	}

	// ReadSlice reuses its buffer. Copy only an oversized line before reading
	// the next fragment, so the normal path stays allocation-free here.
	fullLine := append([]byte(nil), line...)
	for err == bufio.ErrBufferFull { //nolint:errorlint // Only the direct ReadSlice sentinel requests another fragment; wrapped errors must be returned.
		line, err = reader.ReadSlice('\n')
		fullLine = append(fullLine, line...)
	}
	return fullLine, err
}

func (rcv *TCP) handleFraming(conn net.Conn) {
	phasecpu.Set(rcv.phase)
	defer phasecpu.Set(phasecpu.Other)

	framedConn, _ := framing.NewConn(conn, byte(4), binary.BigEndian)
	defer func() {
		if r := recover(); r != nil {
			rcv.logger.Error("panic recovered", zap.String("traceback", fmt.Sprint(r)))
		}
	}()

	atomic.AddInt32(&rcv.active, 1)
	defer atomic.AddInt32(&rcv.active, -1)

	defer conn.Close()

	finished := make(chan bool)
	defer close(finished)

	rcv.Go(func(exit chan bool) {
		select {
		case <-finished:
			return
		case <-exit:
			conn.Close()
			return
		}
	})

	framedConn.MaxFrameSize = uint(rcv.maxMessageSize)

	for {
		conn.SetReadDeadline(time.Now().Add(2 * time.Minute))
		data, err := framedConn.ReadFrame()
		switch {
		case errors.Is(err, io.EOF):
			return
		case errors.Is(err, framing.ErrPrefixLength):
			atomic.AddUint32(&rcv.errors, 1)
			rcv.logger.Warn("bad message size")
			return
		case err != nil:
			atomic.AddUint32(&rcv.errors, 1)
			rcv.logger.Warn("can't read message body", zap.Error(err))
			return
		}

		msgs, err := rcv.frameParser(data)

		if err != nil {
			atomic.AddUint32(&rcv.parseErrors, 1)
			rcv.logger.Info("can't parse message",
				zap.String("data", string(data)),
				zap.Error(err),
			)
			return
		}

		for _, msg := range msgs {
			atomic.AddUint32(&rcv.metricsReceived, uint32(len(msg.Data)))
			rcv.out(msg)
		}
	}
}

func (rcv *TCP) Stat(send helper.StatCallback) {
	metricsReceived := atomic.LoadUint32(&rcv.metricsReceived)
	atomic.AddUint32(&rcv.metricsReceived, -metricsReceived)
	send("metricsReceived", float64(metricsReceived))
	if rcv.promMetricsReceived != nil {
		rcv.promMetricsReceived.Add(float64(metricsReceived))
	}

	active := float64(atomic.LoadInt32(&rcv.active))
	send("active", active)

	errors := atomic.LoadUint32(&rcv.errors)
	atomic.AddUint32(&rcv.errors, -errors)
	send("errors", float64(errors))

	parseErrors := atomic.LoadUint32(&rcv.parseErrors)
	atomic.AddUint32(&rcv.parseErrors, -parseErrors)
	send("parseErrors", float64(parseErrors))

	if rcv.buffer != nil {
		send("bufferLen", float64(len(rcv.buffer)))
		send("bufferCap", float64(cap(rcv.buffer)))
	}
}

// Listen bind port. Receive messages and send to out channel
func (rcv *TCP) Listen(addr *net.TCPAddr) error {
	return rcv.listen(func() (*net.TCPListener, error) { return net.ListenTCP("tcp", addr) }, nil)
}

// listen serves the listener open returns, after the inherited connections.
func (rcv *TCP) listen(open func() (*net.TCPListener, error), inherited []Conn) error {
	return rcv.StartFunc(func() error {

		tcpListener, err := open()
		if err != nil {
			return err
		}

		rcv.Go(func(exit chan bool) {
			<-exit
			tcpListener.Close() //skipcq: GSC-G104
		})

		handler := rcv.HandleConnection
		if rcv.isFraming {
			handler = rcv.handleFraming
		}

		if rcv.buffer != nil {
			originalOut := rcv.out

			rcv.Go(func(exit chan bool) {
				phase := phasecpu.ReceiverTCP
				if rcv.isFraming {
					phase = rcv.phase
				}
				phasecpu.Set(phase)
				defer phasecpu.Set(phasecpu.Other)
				for {
					select {
					case <-exit:
						return
					case p := <-rcv.buffer:
						originalOut(p)
					}
				}
			})

			rcv.out = func(p *points.Points) {
				rcv.buffer <- p
			}
		}

		serve := func(conn net.Conn, leftover []byte) {
			if !rcv.handoff.enabled {
				rcv.Go(func(exit chan bool) { handler(conn) })
				return
			}
			// Count the handler before it starts, so Detach waits for it.
			tracked := rcv.handoff.track(conn)
			rcv.Go(func(exit chan bool) {
				defer rcv.handoff.untrack(tracked)
				rcv.handlePlain(conn, leftover, tracked)
			})
		}
		for _, c := range inherited {
			serve(c.conn, c.Leftover)
		}

		acceptDone := make(chan struct{})
		rcv.handoff.acceptDone = acceptDone
		rcv.Go(func(exit chan bool) {
			defer close(acceptDone)
			defer tcpListener.Close()

			for {

				conn, err := tcpListener.Accept()
				if err != nil {
					if strings.Contains(err.Error(), "use of closed network connection") {
						break
					}
					rcv.logger.Warn("failed to accept connection",
						zap.Error(err),
					)
					continue
				}

				serve(conn, nil)
			}

		})

		rcv.listener = tcpListener

		return nil
	})
}

type decompressor func(io.Reader) (io.Reader, error)

func newDecompressor(typ string) decompressor {
	switch typ {
	case "snappy":
		return func(c io.Reader) (io.Reader, error) {
			return snappy.NewReader(c), nil
		}
	case "gzip":
		return func(c io.Reader) (io.Reader, error) {
			return gzip.NewReader(c)
		}
	default:
		return func(c io.Reader) (io.Reader, error) {
			return c, nil
		}
	}
}

func (rcv *TCP) InitPrometheus(reg prometheus.Registerer) {
	rcv.promMetricsReceived = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "metrics_received_tcp_total",
			Help: "Counter of metrics received via the TCP endpoint.",
		},
	)

	reg.MustRegister(rcv.promMetricsReceived)
}
