package carbon

import (
	"net"
	"testing"
	"time"

	"github.com/k0kubun/pp"
)

type udpTestCase struct {
	*testing.T
	receiver *UdpReceiver
	conn     net.Conn
	rcvChan  chan *Message
}

func newUdpTestCase(t *testing.T) *udpTestCase {
	test := &udpTestCase{
		T: t,
	}

	addr, err := net.ResolveUDPAddr("udp", "localhost:1818")
	if err != nil {
		t.Fatal(err)
	}

	test.rcvChan = make(chan *Message, 128)
	test.receiver = NewUdpReceiver(test.rcvChan)
	// defer receiver.Stop()

	if err = test.receiver.Listen(addr); err != nil {
		t.Fatal(err)
	}

	test.conn, err = net.Dial("udp", addr.String())
	if err != nil {
		t.Fatal(err)
	}
	// defer conn.Close()
	return test
}

func (test *udpTestCase) Finish() {
	if test.conn != nil {
		test.conn.Close()
		test.conn = nil
	}
	if test.receiver != nil {
		test.receiver.Stop()
		test.receiver = nil
	}
}

func (test *udpTestCase) Send(text string) {
	if _, err := test.conn.Write([]byte(text)); err != nil {
		test.Fatal(err)
	}
}

func TestUdpReceiver1(t *testing.T) {

	test := newUdpTestCase(t)
	defer test.Finish()

	test.Send("hello.world 42.15 1422698155\n")

	time.Sleep(10 * time.Millisecond)

	for i := 0; i < 1; i++ {
		select {
		case msg := <-test.rcvChan:
			pp.Println(msg)
		default:
			t.Fatalf("Message #%d not received", i)
		}
	}

}
