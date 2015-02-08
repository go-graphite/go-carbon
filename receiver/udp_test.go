package receiver

import (
	"net"
	"testing"
	"time"

	"devroom.ru/lomik/carbon/points"
)

type udpTestCase struct {
	*testing.T
	receiver *UDP
	conn     net.Conn
	rcvChan  chan *points.Points
}

func newUDPTestCase(t *testing.T) *udpTestCase {
	test := &udpTestCase{
		T: t,
	}

	addr, err := net.ResolveUDPAddr("udp", "localhost:0")
	if err != nil {
		t.Fatal(err)
	}

	test.rcvChan = make(chan *points.Points, 128)
	test.receiver = NewUDP(test.rcvChan)
	// defer receiver.Stop()

	if err = test.receiver.Listen(addr); err != nil {
		t.Fatal(err)
	}

	test.conn, err = net.Dial("udp", test.receiver.Addr().String())
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

func (test *udpTestCase) Eq(a *points.Points, b *points.Points) {
	if a.Metric != b.Metric ||
		a.Data[0].Value != b.Data[0].Value ||
		a.Data[0].Timestamp != b.Data[0].Timestamp {
		test.Fatalf("%#v != %#v", a, b)
	}
}

func TestUDP1(t *testing.T) {
	test := newUDPTestCase(t)
	defer test.Finish()

	time.Sleep(10 * time.Millisecond)

	test.Send("hello.world 42.15 1422698155\n")

	time.Sleep(10 * time.Millisecond)

	select {
	case msg := <-test.rcvChan:
		test.Eq(msg, points.OnePoint("hello.world", 42.15, 1422698155))
	default:
		t.Fatalf("Message #0 not received")
	}
}

func TestUDP2(t *testing.T) {
	test := newUDPTestCase(t)
	defer test.Finish()

	time.Sleep(10 * time.Millisecond)

	test.Send("hello.world 42.15 1422698155\nmetric.name -72.11 1422698155\n")

	time.Sleep(10 * time.Millisecond)

	select {
	case msg := <-test.rcvChan:
		test.Eq(msg, points.OnePoint("hello.world", 42.15, 1422698155))
	default:
		t.Fatalf("Message #0 not received")
	}

	select {
	case msg := <-test.rcvChan:
		test.Eq(msg, points.OnePoint("metric.name", -72.11, 1422698155))
	default:
		t.Fatalf("Message #1 not received")
	}
}
