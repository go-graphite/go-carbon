package carbon

import (
	"bytes"
	"fmt"
	"io"
	"net"
	"strings"
	"sync/atomic"
	"time"

	"github.com/Sirupsen/logrus"
)

// UDPReceiver receive metrics from TCP and UDP sockets
type UDPReceiver struct {
	out             chan *Message
	exit            chan bool
	graphPrefix     string
	metricsReceived uint32
}

// NewUDPReceiver create new instance of UDPReceiver
func NewUDPReceiver(out chan *Message) *UDPReceiver {
	return &UDPReceiver{
		out:  out,
		exit: make(chan bool),
	}
}

// SetGraphPrefix for internal cache metrics
func (rcv *UDPReceiver) SetGraphPrefix(prefix string) {
	rcv.graphPrefix = prefix
}

// Stat sends internal statistics to cache
func (rcv *UDPReceiver) Stat(metric string, value float64) {
	msg := NewMessage()

	msg.Name = fmt.Sprintf("%s%s", rcv.graphPrefix, metric)
	msg.Value = value
	msg.Timestamp = time.Now().Unix()

	rcv.out <- msg
}

// Listen bind port. Receive messages and send to out channel
func (rcv *UDPReceiver) Listen(addr *net.UDPAddr) error {
	sock, err := net.ListenUDP("udp", addr)
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
				rcv.Stat("udpMetricsReceived", float64(cnt))
			case <-rcv.exit:
				sock.Close()
				return
			}
		}
	}()

	go func() {
		defer sock.Close()

		var buf [2048]byte

		for {
			// @TODO: store incomplete lines
			rlen, _, err := sock.ReadFromUDP(buf[:])
			if err != nil {
				if strings.Contains(err.Error(), "use of closed network connection") {
					break
				}
				logrus.Error(err)
				continue
			}

			data := bytes.NewBuffer(buf[:rlen])

			for {
				line, err := data.ReadBytes('\n')

				if err != nil {
					if err == io.EOF {
						if len(line) > 0 {
							// @TODO: handle unfinished line
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

	}()

	return nil
}

// Stop all listeners
func (rcv *UDPReceiver) Stop() {
	close(rcv.exit)
}
