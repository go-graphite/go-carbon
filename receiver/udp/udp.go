package udp

import (
	"bytes"
	"net"
	"strings"
	"sync/atomic"

	"go.uber.org/zap"

	"github.com/lomik/go-carbon/helper"
	"github.com/lomik/go-carbon/points"
	"github.com/lomik/go-carbon/receiver"
	"github.com/lomik/go-carbon/receiver/parse"
	"github.com/lomik/zapwriter"
)

func init() {
	receiver.Register(
		"udp",
		func() interface{} { return NewOptions() },
		func(name string, options interface{}, store func(*points.Points)) (receiver.Receiver, error) {
			return newUDP(name, options.(*Options), store)
		},
	)
}

type Options struct {
	Listen     string `toml:"listen"`
	Enabled    bool   `toml:"enabled"`
	BufferSize int    `toml:"buffer-size"`
}

// UDP receive metrics from UDP socket
type UDP struct {
	helper.Stoppable
	out             func(*points.Points)
	name            string
	metricsReceived uint32
	errors          uint32
	logIncomplete   bool
	conn            *net.UDPConn
	buffer          chan *points.Points
	logger          *zap.Logger
}

func NewOptions() *Options {
	return &Options{
		Listen:     ":2003",
		Enabled:    true,
		BufferSize: 0,
	}
}

// Addr returns binded socket address. For bind port 0 in tests
func (rcv *UDP) Addr() net.Addr {
	if rcv.conn == nil {
		return nil
	}
	return rcv.conn.LocalAddr()
}

func newUDP(name string, options *Options, store func(*points.Points)) (*UDP, error) {
	if !options.Enabled {
		return nil, nil
	}

	addr, err := net.ResolveUDPAddr("udp", options.Listen)
	if err != nil {
		return nil, err
	}

	r := &UDP{
		out:    store,
		name:   name,
		logger: zapwriter.Logger(name),
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

func (rcv *UDP) Stat(send helper.StatCallback) {
	metricsReceived := atomic.LoadUint32(&rcv.metricsReceived)
	atomic.AddUint32(&rcv.metricsReceived, -metricsReceived)
	send("metricsReceived", float64(metricsReceived))

	errors := atomic.LoadUint32(&rcv.errors)
	atomic.AddUint32(&rcv.errors, -errors)
	send("errors", float64(errors))

	if rcv.buffer != nil {
		send("bufferLen", float64(len(rcv.buffer)))
		send("bufferCap", float64(cap(rcv.buffer)))
	}
}

func (rcv *UDP) receiveWorker(exit chan bool) {
	defer rcv.conn.Close()

	var buf [65535]byte

	var data *bytes.Buffer

	for {
		rlen, peer, err := rcv.conn.ReadFromUDP(buf[:])
		if err != nil {
			if strings.Contains(err.Error(), "use of closed network connection") {
				break
			}
			atomic.AddUint32(&rcv.errors, 1)
			rcv.logger.Error("read error", zap.Error(err))
			continue
		}

		data = bytes.NewBuffer(buf[:rlen])

		for {
			line, err := data.ReadBytes('\n')

			if len(line) > 0 {
				name, value, timestamp, err := parse.PlainLine(line)
				if err != nil {
					atomic.AddUint32(&rcv.errors, 1)
					rcv.logger.Info("parse failed",
						zap.Error(err),
						zap.String("peer", peer.String()),
					)
				} else {
					atomic.AddUint32(&rcv.metricsReceived, 1)
					rcv.out(points.OnePoint(string(name), value, timestamp))
				}
			}

			if err != nil {
				break
			}
		}
	}
}

// Listen bind port. Receive messages and send to out channel
func (rcv *UDP) Listen(addr *net.UDPAddr) error {
	return rcv.StartFunc(func() error {
		var err error
		rcv.conn, err = net.ListenUDP("udp", addr)
		if err != nil {
			return err
		}

		rcv.Go(func(exit chan bool) {
			<-exit
			rcv.conn.Close()
		})

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

		rcv.Go(rcv.receiveWorker)

		return nil
	})
}
