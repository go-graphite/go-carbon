package receiver

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"strings"
	"sync/atomic"
	"time"

	"github.com/lomik/go-carbon/helper"
	"github.com/lomik/go-carbon/points"

	"github.com/Sirupsen/logrus"
)

// TCP receive metrics from TCP connections
type TCP struct {
	helper.Stoppable
	out                  chan *points.Points
	graphPrefix          string
	maxPickleMessageSize uint32
	metricsReceived      uint32
	errors               uint32
	active               int32 // counter
	listener             *net.TCPListener
	isPickle             bool
	metricInterval       time.Duration
}

// NewTCP create new instance of TCP
func NewTCP(out chan *points.Points) *TCP {
	return &TCP{
		out:            out,
		isPickle:       false,
		metricInterval: time.Minute,
	}
}

// NewPickle create new instance of TCP with pickle listener enabled
func NewPickle(out chan *points.Points) *TCP {
	return &TCP{
		out:                  out,
		isPickle:             true,
		metricInterval:       time.Minute,
		maxPickleMessageSize: 67108864, // 64 Mb
	}
}

// SetGraphPrefix for internal cache metrics
func (rcv *TCP) SetGraphPrefix(prefix string) {
	rcv.graphPrefix = prefix
}

// SetMetricInterval sets doChekpoint interval
func (rcv *TCP) SetMetricInterval(interval time.Duration) {
	rcv.metricInterval = interval
}

// SetMaxPickleMessageSize sets maxPickleMessageSize (in bytes)
func (rcv *TCP) SetMaxPickleMessageSize(newSize uint32) {
	rcv.maxPickleMessageSize = newSize
}

// Stat sends internal statistics to cache
func (rcv *TCP) Stat(metric string, value float64) {
	rcv.out <- points.OnePoint(
		fmt.Sprintf("%s%s", rcv.graphPrefix, metric),
		value,
		time.Now().Unix(),
	)
}

// Addr returns binded socket address. For bind port 0 in tests
func (rcv *TCP) Addr() net.Addr {
	if rcv.listener == nil {
		return nil
	}
	return rcv.listener.Addr()
}

func (rcv *TCP) handleConnection(conn net.Conn) {
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
				rcv.out <- msg
			}
		}
	}
}

func (rcv *TCP) handlePickle(conn net.Conn) {
	defer func() {
		if r := recover(); r != nil {
			logrus.Errorf("[pickle] Unknown error recovered: %s", r)
		}
	}()

	atomic.AddInt32(&rcv.active, 1)
	defer atomic.AddInt32(&rcv.active, -1)

	defer conn.Close()
	reader := bufio.NewReader(conn)

	var msgLen uint32
	var err error

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

	maxMessageSize := rcv.maxPickleMessageSize

	for {
		conn.SetReadDeadline(time.Now().Add(2 * time.Minute))

		// Read prepended length
		err = binary.Read(reader, binary.BigEndian, &msgLen)
		if err != nil {
			if err == io.EOF {
				return
			}

			atomic.AddUint32(&rcv.errors, 1)
			logrus.Warningf("[pickle] Can't read message length: %s", err.Error())
			return
		}

		if msgLen > maxMessageSize {
			atomic.AddUint32(&rcv.errors, 1)
			logrus.Warningf("[pickle] Bad message size: %d", msgLen)
			return
		}

		// Allocate a byte array of the expected length
		data := make([]byte, msgLen)

		// Read remainder of pickle packet into byte array
		if err = binary.Read(reader, binary.BigEndian, data); err != nil {
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
			rcv.out <- msg
		}
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
			rcvName := "tcp"
			if rcv.isPickle {
				rcvName = "pickle"
			}

			ticker := time.NewTicker(rcv.metricInterval)
			defer ticker.Stop()

			for {
				select {
				case <-ticker.C:
					metricsReceived := atomic.LoadUint32(&rcv.metricsReceived)
					atomic.AddUint32(&rcv.metricsReceived, -metricsReceived)
					rcv.Stat("metricsReceived", float64(metricsReceived))

					active := float64(atomic.LoadInt32(&rcv.active))
					rcv.Stat("active", active)

					errors := atomic.LoadUint32(&rcv.errors)
					atomic.AddUint32(&rcv.errors, -errors)
					rcv.Stat("errors", float64(errors))

					logrus.WithFields(logrus.Fields{
						"metricsReceived": int(metricsReceived),
						"active":          int(active),
						"errors":          int(errors),
					}).Infof("[%s] doCheckpoint()", rcvName)
				case <-exit:
					tcpListener.Close()
					return
				}
			}
		})

		handler := rcv.handleConnection
		if rcv.isPickle {
			handler = rcv.handlePickle
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
