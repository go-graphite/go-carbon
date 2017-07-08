package udp

import (
	"bytes"
	"fmt"
	"io"
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
	Listen        string `toml:"listen"`
	Enabled       bool   `toml:"enabled"`
	LogIncomplete bool   `toml:"log-incomplete"`
	BufferSize    int    `toml:"buffer-size"`
}

// UDP receive metrics from UDP socket
type UDP struct {
	helper.Stoppable
	out                func(*points.Points)
	name               string
	metricsReceived    uint32
	incompleteReceived uint32
	errors             uint32
	logIncomplete      bool
	conn               *net.UDPConn
	buffer             chan *points.Points
	logger             *zap.Logger
}

func NewOptions() *Options {
	return &Options{
		Listen:        ":2003",
		Enabled:       true,
		LogIncomplete: false,
		BufferSize:    0,
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
		out:           store,
		name:          name,
		logIncomplete: options.LogIncomplete,
		logger:        zapwriter.Logger(name),
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

func logIncomplete(logger *zap.Logger, peer *net.UDPAddr, message []byte, lastLine []byte) {
	p1 := bytes.IndexByte(message, 0xa) // find first "\n"

	var m string
	if p1 != -1 && p1+len(lastLine) < len(message)-10 { // print short version
		m = fmt.Sprintf("%s\\n...(%d bytes)...\\n%s",
			string(message[:p1]),
			len(message)-p1-len(lastLine)-2,
			string(lastLine),
		)
	} else { // print full
		m = string(message)
	}

	logger.Warn("incomplete message",
		zap.String("peer", peer.String()),
		zap.String("message", m),
	)
}

func (rcv *UDP) Stat(send helper.StatCallback) {
	metricsReceived := atomic.LoadUint32(&rcv.metricsReceived)
	atomic.AddUint32(&rcv.metricsReceived, -metricsReceived)
	send("metricsReceived", float64(metricsReceived))

	incompleteReceived := atomic.LoadUint32(&rcv.incompleteReceived)
	atomic.AddUint32(&rcv.incompleteReceived, -incompleteReceived)
	send("incompleteReceived", float64(incompleteReceived))

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

	lines := newIncompleteStorage()

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

		prev := lines.pop(peer.String())

		if prev != nil {
			data = bytes.NewBuffer(prev)
			data.Write(buf[:rlen])
		} else {
			data = bytes.NewBuffer(buf[:rlen])
		}

		for {
			line, err := data.ReadBytes('\n')

			if err != nil {
				if err == io.EOF {
					if len(line) > 0 { // incomplete line received

						if rcv.logIncomplete {
							logIncomplete(rcv.logger, peer, buf[:rlen], line)
						}

						lines.store(peer.String(), line)
						atomic.AddUint32(&rcv.incompleteReceived, 1)
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
