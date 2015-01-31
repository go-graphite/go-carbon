package carbon

import (
	"bytes"
	"io"
	"net"
	"strings"

	"github.com/Sirupsen/logrus"
)

// UDPReceiver receive metrics from TCP and UDP sockets
type UDPReceiver struct {
	out  chan *Message
	exit chan bool
}

// NewUDPReceiver create new instance of UDPReceiver
func NewUDPReceiver(out chan *Message) *UDPReceiver {
	return &UDPReceiver{
		out:  out,
		exit: make(chan bool),
	}
}

// Listen bind port. Receive messages and send to out channel
func (rcv *UDPReceiver) Listen(addr *net.UDPAddr) error {
	sock, err := net.ListenUDP("udp", addr)
	if err != nil {
		return err
	}

	go func() {
		select {
		case <-rcv.exit:
			sock.Close()
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
