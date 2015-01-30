package carbon

import "net"

// UdpReceiver receive metrics from TCP and UDP sockets
type UdpReceiver struct {
	out  chan *Message
	exit chan bool
}

// NewUdpReceiver create new instance of UdpReceiver
func NewUdpReceiver(out chan *Message) *UdpReceiver {
	return &UdpReceiver{
		out:  out,
		exit: make(chan bool),
	}
}

// Listen bind port. Receive messages and send to out channel
func (rcv *UdpReceiver) Listen(addr *net.UDPAddr) error {
	_, err := net.ListenUDP("udp", addr)
	if err != nil {
		return err
	}
	return nil
}

// Stop all listeners
func (rcv *UdpReceiver) Stop() {
	close(rcv.exit)
}
