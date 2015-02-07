package carbon

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"strings"
	"sync/atomic"
	"time"

	"github.com/Sirupsen/logrus"
)

// TCPReceiver receive metrics from TCP and UDP sockets
type TCPReceiver struct {
	out             chan *Message
	exit            chan bool
	graphPrefix     string
	metricsReceived uint32
	active          int32 // counter
}

// NewTCPReceiver create new instance of TCPReceiver
func NewTCPReceiver(out chan *Message) *TCPReceiver {
	return &TCPReceiver{
		out:  out,
		exit: make(chan bool),
	}
}

// SetGraphPrefix for internal cache metrics
func (rcv *TCPReceiver) SetGraphPrefix(prefix string) {
	rcv.graphPrefix = prefix
}

// Stat sends internal statistics to cache
func (rcv *TCPReceiver) Stat(metric string, value float64) {
	msg := NewMessage()

	msg.Name = fmt.Sprintf("%s%s", rcv.graphPrefix, metric)
	msg.Value = value
	msg.Timestamp = time.Now().Unix()

	rcv.out <- msg
}

func (rcv *TCPReceiver) handleConnection(conn net.Conn) {
	atomic.AddInt32(&rcv.active, 1)
	defer atomic.AddInt32(&rcv.active, -1)

	defer conn.Close()
	conn.SetReadDeadline(time.Now().Add(time.Minute))
	reader := bufio.NewReader(conn)

	for {
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
			if msg, err := ParseTextMessage(string(line)); err != nil {
				logrus.Info(err)
			} else {
				atomic.AddUint32(&rcv.metricsReceived, 1)
				rcv.out <- msg
			}
		}
	}
}

// Listen bind port. Receive messages and send to out channel
func (rcv *TCPReceiver) Listen(addr *net.TCPAddr) error {
	sock, err := net.ListenTCP("tcp", addr)
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
				rcv.Stat("tcpMetricsReceived", float64(cnt))

				active := atomic.LoadInt32(&rcv.active)
				atomic.AddInt32(&rcv.active, -active)
				rcv.Stat("tcpActive", float64(active))
			case <-rcv.exit:
				sock.Close()
				return
			}
		}
	}()

	go func() {
		defer sock.Close()

		for {

			conn, err := sock.Accept()
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
func (rcv *TCPReceiver) Stop() {
	close(rcv.exit)
}
