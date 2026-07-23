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
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"

	"github.com/go-graphite/go-carbon/helper"
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
	Workers     int    `toml:"workers"`
	Compression string `toml:"compression"`
}

type tcpBatch struct {
	data []byte
	peer string
}

func NewOptions() *Options {
	return &Options{
		Listen:     ":2003",
		Enabled:    true,
		BufferSize: 0,
		Workers:    1,
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

// TCP receive metrics from TCP connections
type TCP struct {
	helper.Stoppable
	out                 func(*points.Points)
	name                string // name for store metrics
	maxMessageSize      uint32
	metricsReceived     uint32
	promMetricsReceived prometheus.Counter
	errors              uint32
	active              int32 // counter
	listener            *net.TCPListener
	isFraming           bool
	frameParser         func(body []byte) ([]*points.Points, error)
	buffer              chan *points.Points
	batchBuffer         chan tcpBatch
	workers             int
	producers           sync.WaitGroup
	connectionsExit     chan struct{}
	acceptDone          chan struct{}
	stopOnce            sync.Once
	logger              *zap.Logger
	decompressor        decompressor
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
		out:     store,
		name:    name,
		logger:  zapwriter.Logger(name),
		workers: max(options.Workers, 1),
	}

	if r.workers > 1 {
		r.batchBuffer = make(chan tcpBatch, max(options.BufferSize, 0))
	} else if options.BufferSize > 0 {
		r.buffer = make(chan *points.Points, options.BufferSize)
	}

	r.decompressor = newDecompressor(options.Compression)

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
	atomic.AddInt32(&rcv.active, 1)
	defer atomic.AddInt32(&rcv.active, -1)

	defer conn.Close()

	finished := make(chan bool)
	defer close(finished)

	rcv.Go(func(exit chan bool) {
		select {
		case <-finished:
		case <-exit:
			conn.Close()
		case <-rcv.connectionsExit:
			conn.Close()
		}
	})

	bconn, err := rcv.decompressor(conn)
	if err != nil {
		rcv.logger.Error("failed init decompressor", zap.Error(err))
		return
	}
	reader := bufio.NewReader(bconn)
	peer := conn.RemoteAddr().String()

	lastDeadline := time.Now()
	readTimeout := 2 * time.Minute
	conn.SetReadDeadline(lastDeadline.Add(readTimeout))

	for {
		now := time.Now()
		if now.Sub(lastDeadline) > (readTimeout / 4) {
			conn.SetReadDeadline(now.Add(readTimeout))
			lastDeadline = now
		}

		line, err := reader.ReadBytes('\n')

		if err != nil {
			if err == io.EOF {
				if len(line) > 0 {
					rcv.logger.Warn("unfinished line", zap.String("line", string(line)))
				}
			} else {
				atomic.AddUint32(&rcv.errors, 1)
				rcv.logger.Error("read error", zap.Error(err))
			}
			break
		}
		if rcv.batchBuffer != nil {
			// Amortize channel overhead across every complete line already buffered.
			if buffered := reader.Buffered(); buffered > 0 {
				data, _ := reader.Peek(buffered)
				if end := bytes.LastIndexByte(data, '\n'); end >= 0 {
					line = append(line, data[:end+1]...)
					_, _ = reader.Discard(end + 1)
				}
			}

			rcv.batchBuffer <- tcpBatch{data: line, peer: peer}
			continue
		}

		if len(line) > 0 { // skip empty lines
			rcv.processLine(line, peer)
		}
	}
}

func (rcv *TCP) processBatch(batch tcpBatch) {
	for len(batch.data) > 0 {
		end := bytes.IndexByte(batch.data, '\n')
		if end < 0 {
			return
		}
		rcv.processLine(batch.data[:end+1], batch.peer)
		batch.data = batch.data[end+1:]
	}
}

func (rcv *TCP) processLine(line []byte, peer string) {
	name, value, timestamp, err := parse.PlainLine(line)
	if err != nil {
		atomic.AddUint32(&rcv.errors, 1)
		rcv.logger.Debug("parse failed", zap.Error(err), zap.String("peer", peer))
		return
	}

	atomic.AddUint32(&rcv.metricsReceived, 1)
	rcv.out(points.OnePoint(string(name), value, timestamp))
}

// Stop drains accepted plaintext batches when parallel workers are enabled.
func (rcv *TCP) Stop() {
	rcv.stopOnce.Do(func() {
		if rcv.batchBuffer != nil {
			_ = rcv.listener.Close()
			<-rcv.acceptDone // No more producers can be added after Accept stops.
			close(rcv.connectionsExit)
			rcv.producers.Wait()
			close(rcv.batchBuffer)
		}
		rcv.Stoppable.Stop()
	})
}

func (rcv *TCP) handleFraming(conn net.Conn) {
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
			atomic.AddUint32(&rcv.errors, 1)
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

	if rcv.buffer != nil {
		send("bufferLen", float64(len(rcv.buffer)))
		send("bufferCap", float64(cap(rcv.buffer)))
	} else if rcv.batchBuffer != nil {
		send("bufferLen", float64(len(rcv.batchBuffer)))
		send("bufferCap", float64(cap(rcv.batchBuffer)))
	}
}

// Listen bind port. Receive messages and send to out channel
func (rcv *TCP) Listen(addr *net.TCPAddr) error {
	return rcv.StartFunc(func() error {

		tcpListener, err := net.ListenTCP("tcp", addr)
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

		if rcv.batchBuffer != nil {
			rcv.connectionsExit = make(chan struct{})
			for i := 0; i < rcv.workers; i++ {
				rcv.Go(func(exit chan bool) {
					for batch := range rcv.batchBuffer {
						rcv.processBatch(batch)
					}
				})
			}
		}

		rcv.acceptDone = make(chan struct{})
		rcv.Go(func(exit chan bool) {
			defer tcpListener.Close()
			defer close(rcv.acceptDone)

			for {

				conn, err := tcpListener.Accept()
				if err != nil {
					if errors.Is(err, net.ErrClosed) || strings.Contains(err.Error(), "use of closed network connection") {
						break
					}
					rcv.logger.Warn("failed to accept connection",
						zap.Error(err),
					)
					continue
				}

				if rcv.batchBuffer != nil {
					rcv.producers.Add(1)
					rcv.Go(func(exit chan bool) {
						defer rcv.producers.Done()
						handler(conn)
					})
				} else {
					rcv.Go(func(exit chan bool) {
						handler(conn)
					})
				}
			}

		})

		rcv.listener = tcpListener

		return nil
	})
}

type decompressor func(net.Conn) (io.Reader, error)

func newDecompressor(typ string) decompressor {
	switch typ {
	case "snappy":
		return func(c net.Conn) (io.Reader, error) {
			return snappy.NewReader(c), nil
		}
	case "gzip":
		return func(c net.Conn) (io.Reader, error) {
			return gzip.NewReader(c)
		}
	default:
		return func(c net.Conn) (io.Reader, error) {
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
