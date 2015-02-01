package carbon

import (
	"net"
	"testing"
	"time"
)

type udpTestCase struct {
	*testing.T
	receiver *UDPReceiver
	conn     net.Conn
	rcvChan  chan *Message
}

func newUDPTestCase(t *testing.T) *udpTestCase {
	test := &udpTestCase{
		T: t,
	}

	addr, err := net.ResolveUDPAddr("udp", "localhost:1818")
	if err != nil {
		t.Fatal(err)
	}

	test.rcvChan = make(chan *Message, 128)
	test.receiver = NewUDPReceiver(test.rcvChan)
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

func (test *udpTestCase) Eq(a *Message, b *Message) {
	if a.Name != b.Name || a.Value != b.Value || a.Timestamp != b.Timestamp {
		test.Fatalf("%#v != %#v", a, b)
	}
}

func TestUDPReceiver1(t *testing.T) {
	test := newUDPTestCase(t)
	defer test.Finish()

	time.Sleep(10 * time.Millisecond)

	test.Send("hello.world 42.15 1422698155\n")

	time.Sleep(10 * time.Millisecond)

	select {
	case msg := <-test.rcvChan:
		test.Eq(msg, &Message{
			Name:      "hello.world",
			Value:     42.15,
			Timestamp: 1422698155,
		})
	default:
		t.Fatalf("Message #0 not received")
	}
}

func TestUDPReceiver2(t *testing.T) {
	test := newUDPTestCase(t)
	defer test.Finish()

	time.Sleep(10 * time.Millisecond)

	test.Send("hello.world 42.15 1422698155\nmetric.name -72.11 1422698155\n")

	time.Sleep(10 * time.Millisecond)

	select {
	case msg := <-test.rcvChan:
		test.Eq(msg, &Message{
			Name:      "hello.world",
			Value:     42.15,
			Timestamp: 1422698155,
		})
	default:
		t.Fatalf("Message #0 not received")
	}

	select {
	case msg := <-test.rcvChan:
		test.Eq(msg, &Message{
			Name:      "metric.name",
			Value:     -72.11,
			Timestamp: 1422698155,
		})
	default:
		t.Fatalf("Message #1 not received")
	}
}
