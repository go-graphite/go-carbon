package receiver

import (
	"bufio"
	"encoding/binary"
	"io"
	"net"
	"strings"
	"sync/atomic"
	"time"

	"github.com/lomik/go-carbon/helper"
	"github.com/lomik/go-carbon/helper/framing"
	"github.com/lomik/go-carbon/points"

	"github.com/Sirupsen/logrus"
)

// TCP receive metrics from TCP connections
type TCP struct {
	helper.Stoppable
	out                  func(*points.Points)
	name                 string // name for store metrics
	maxPickleMessageSize uint32
	metricsReceived      uint32
	errors               uint32
	active               int32 // counter
	listener             *net.TCPListener
	isPickle             bool
	buffer               chan *points.Points
}

// Name returns receiver name (for store internal metrics)
func (rcv *TCP) Name() string {
	return rcv.name
}

// Addr returns binded socket address. For bind port 0 in tests
func (rcv *TCP) Addr() net.Addr {
	if rcv.listener == nil {
		return nil
	}
	return rcv.listener.Addr()
}

func (rcv *TCP) HandleConnection(conn net.Conn) {
	atomic.AddInt32(&rcv.active, 1)
	defer atomic.AddInt32(&rcv.active, -1)

	defer conn.Close()
	reader := bufio.NewReader(conn)

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

	for {
		conn.SetReadDeadline(time.Now().Add(2 * time.Minute))

		line, err := reader.ReadBytes('\n')

		if err != nil {
			if err == io.EOF {
				if len(line) > 0 {
					logrus.Warningf("[tcp] Unfinished line: %#v", line)
				}
			} else {
				atomic.AddUint32(&rcv.errors, 1)
				logrus.Error(err)
			}
			break
		}
		if len(line) > 0 { // skip empty lines
			if msg, err := points.ParseText(string(line)); err != nil {
				atomic.AddUint32(&rcv.errors, 1)
				logrus.Info(err)
			} else {
				atomic.AddUint32(&rcv.metricsReceived, 1)
				rcv.out(msg)
			}
		}
	}
}

func (rcv *TCP) handlePickle(conn net.Conn) {
	framedConn, _ := framing.NewConn(conn, byte(4), binary.BigEndian)
	defer func() {
		if r := recover(); r != nil {
			logrus.Errorf("[pickle] Unknown error recovered: %s", r)
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

	framedConn.MaxFrameSize = uint(rcv.maxPickleMessageSize)

	for {
		conn.SetReadDeadline(time.Now().Add(2 * time.Minute))
		data, err := framedConn.ReadFrame()
		if err == framing.ErrPrefixLength {
			atomic.AddUint32(&rcv.errors, 1)
			logrus.Warningf("[pickle] Bad message size")
			return
		} else if err != nil {
			atomic.AddUint32(&rcv.errors, 1)
			logrus.Warningf("[pickle] Can't read message body: %s", err.Error())
			return
		}

		msgs, err := points.ParsePickle(data)

		if err != nil {
			atomic.AddUint32(&rcv.errors, 1)
			logrus.Infof("[pickle] Can't unpickle message: %s", err.Error())
			logrus.Debugf("[pickle] Bad message: %#v", string(data))
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

	active := float64(atomic.LoadInt32(&rcv.active))
	send("active", active)

	errors := atomic.LoadUint32(&rcv.errors)
	atomic.AddUint32(&rcv.errors, -errors)
	send("errors", float64(errors))

	if rcv.buffer != nil {
		send("bufferLen", float64(len(rcv.buffer)))
		send("bufferCap", float64(cap(rcv.buffer)))
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
			tcpListener.Close()
		})

		handler := rcv.HandleConnection
		if rcv.isPickle {
			handler = rcv.handlePickle
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

		rcv.Go(func(exit chan bool) {
			defer tcpListener.Close()

			for {

				conn, err := tcpListener.Accept()
				if err != nil {
					if strings.Contains(err.Error(), "use of closed network connection") {
						break
					}
					logrus.Warningf("[tcp] Failed to accept connection: %s", err)
					continue
				}

				rcv.Go(func(exit chan bool) {
					handler(conn)
				})
			}

		})

		rcv.listener = tcpListener

		return nil
	})
}
