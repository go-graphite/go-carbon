package receiver

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"strings"
	"sync/atomic"
	"time"

	"devroom.ru/lomik/carbon/points"

	"github.com/Sirupsen/logrus"
)

// TCP receive metrics from TCP connections
type TCP struct {
	out             chan *points.Points
	exit            chan bool
	graphPrefix     string
	metricsReceived uint32
	active          int32 // counter
	listener        *net.TCPListener
}

// NewTCP create new instance of TCP
func NewTCP(out chan *points.Points) *TCP {
	return &TCP{
		out:  out,
		exit: make(chan bool),
	}
}

// SetGraphPrefix for internal cache metrics
func (rcv *TCP) SetGraphPrefix(prefix string) {
	rcv.graphPrefix = prefix
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

	for {
		conn.SetReadDeadline(time.Now().Add(2 * time.Minute))

		line, err := reader.ReadBytes('\n')

		if err != nil {
			if err == io.EOF {
				if len(line) > 0 {
					logrus.Warningf("Unfinished line: %#v", line)
				}
			} else {
				logrus.Error(err)
			}
			break
		}
		if len(line) > 0 { // skip empty lines
			if msg, err := points.ParseText(string(line)); err != nil {
				logrus.Info(err)
			} else {
				atomic.AddUint32(&rcv.metricsReceived, 1)
				rcv.out <- msg
			}
		}
	}
}

// Listen bind port. Receive messages and send to out channel
func (rcv *TCP) Listen(addr *net.TCPAddr) error {
	var err error
	rcv.listener, err = net.ListenTCP("tcp", addr)
	if err != nil {
		return err
	}

	go func() {
		ticker := time.NewTicker(time.Minute)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				cnt := atomic.LoadUint32(&rcv.metricsReceived)
				atomic.AddUint32(&rcv.metricsReceived, -cnt)
				rcv.Stat("tcp.metricsReceived", float64(cnt))

				active := atomic.LoadInt32(&rcv.active)
				atomic.AddInt32(&rcv.active, -active)
				rcv.Stat("tcp.active", float64(active))
			case <-rcv.exit:
				rcv.listener.Close()
				return
			}
		}
	}()

	go func() {
		defer rcv.listener.Close()

		for {

			conn, err := rcv.listener.Accept()
			if err != nil {
				if strings.Contains(err.Error(), "use of closed network connection") {
					break
				}
				logrus.Warningf("Failed to accept connection: %s", err)
				continue
			}

			go rcv.handleConnection(conn)
		}

	}()

	return nil
}

// Stop all listeners
func (rcv *TCP) Stop() {
	close(rcv.exit)
}
