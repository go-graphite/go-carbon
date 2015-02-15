package receiver

import (
	"net"
	"testing"
	"time"

	"github.com/lomik/go-carbon/points"
)

type pickleTestCase struct {
	*testing.T
	receiver *TCP
	conn     net.Conn
	rcvChan  chan *points.Points
}

func newPickleTestCase(t *testing.T) *pickleTestCase {
	test := &pickleTestCase{
		T: t,
	}

	addr, err := net.ResolveTCPAddr("tcp", "localhost:0")
	if err != nil {
		t.Fatal(err)
	}

	test.rcvChan = make(chan *points.Points, 128)
	test.receiver = NewPickle(test.rcvChan)
	// defer receiver.Stop()

	if err = test.receiver.Listen(addr); err != nil {
		t.Fatal(err)
	}

	test.conn, err = net.Dial("tcp", test.receiver.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	// defer conn.Close()
	return test
}

func (test *pickleTestCase) Finish() {
	if test.conn != nil {
		test.conn.Close()
		test.conn = nil
	}
	if test.receiver != nil {
		test.receiver.Stop()
		test.receiver = nil
	}
}

func (test *pickleTestCase) Send(text string) {
	if _, err := test.conn.Write([]byte(text)); err != nil {
		test.Fatal(err)
	}
}

func (test *pickleTestCase) Eq(a *points.Points, b *points.Points) {
	if !a.Eq(b) {
		test.Fatalf("%#v != %#v", a, b)
	}
}

func TestPickle(t *testing.T) {
	test := newPickleTestCase(t)
	defer test.Finish()

	// [("param1", (1423931224, 60.2), (1423931284, 42)), ("param2", (1423931224, -15))]
	test.Send("\x00\x00\x00n(lp0\n(S'param1'\np1\n(I1423931224\nF60.2\ntp2\n(I1423931284\nI42\ntp3\ntp4\na(S'param2'\np5\n(I1423931224\nI-15\ntp6\ntp7\na.")

	time.Sleep(10 * time.Millisecond)

	select {
	case msg := <-test.rcvChan:
		test.Eq(msg, points.OnePoint("param1", 60.2, 1423931224).Add(42, 1423931284))
	default:
		t.Fatalf("Message #0 not received")
	}

	select {
	case msg := <-test.rcvChan:
		test.Eq(msg, points.OnePoint("param2", -15, 1423931224))
	default:
		t.Fatalf("Message #1 not received")
	}
}
