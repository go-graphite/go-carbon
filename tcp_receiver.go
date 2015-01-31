package carbon

import (
	"bufio"
	"io"
	"net"
	"strings"
	"time"

	"github.com/Sirupsen/logrus"
)

// TcpReceiver receive metrics from TCP and UDP sockets
type TcpReceiver struct {
	out  chan *Message
	exit chan bool
}

// NewTcpReceiver create new instance of TcpReceiver
func NewTcpReceiver(out chan *Message) *TcpReceiver {
	return &TcpReceiver{
		out:  out,
		exit: make(chan bool),
	}
}

func (rcv *TcpReceiver) handleConnection(conn net.Conn) {
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
				rcv.out <- msg
			}
		}
	}
}

// Listen bind port. Receive messages and send to out channel
func (rcv *TcpReceiver) Listen(addr *net.TCPAddr) error {
	sock, err := net.ListenTCP("tcp", addr)
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
func (rcv *TcpReceiver) Stop() {
	close(rcv.exit)
}
