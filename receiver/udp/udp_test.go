package udp

import (
	"net"
	"testing"
	"time"

	"github.com/lomik/go-carbon/points"
	"github.com/lomik/go-carbon/receiver"
)

type udpTestCase struct {
	*testing.T
	receiver *UDP
	conn     net.Conn
	rcvChan  chan *points.Points
}

func newUDPTestCaseWithOptions(t *testing.T, logIncomplete bool) *udpTestCase {
	test := &udpTestCase{
		T: t,
	}

	addr, err := net.ResolveUDPAddr("udp", "localhost:0")
	if err != nil {
		t.Fatal(err)
	}

	test.rcvChan = make(chan *points.Points, 128)

	r, err := receiver.New("udp", map[string]interface{}{
		"protocol":       "udp",
		"log-incomplete": logIncomplete,
		"listen":         addr.String(),
	},
		func(p *points.Points) {
			test.rcvChan <- p
		},
	)

	if err != nil {
		t.Fatal(err)
	}
	test.receiver = r.(*UDP)

	test.conn, err = net.Dial("udp", test.receiver.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	// defer conn.Close()
	time.Sleep(5 * time.Millisecond)

	return test
}

func newUDPTestCase(t *testing.T) *udpTestCase {
	return newUDPTestCaseWithOptions(t, false)
}

func newUDPTestCaseLogIncomplete(t *testing.T) *udpTestCase {
	return newUDPTestCaseWithOptions(t, true)
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
	time.Sleep(5 * time.Millisecond)
}

func (test *udpTestCase) Eq(a *points.Points, b *points.Points) {
	if !a.Eq(b) {
		test.Fatalf("%#v != %#v", a, b)
	}
}

func TestUDP1(t *testing.T) {
	test := newUDPTestCase(t)
	defer test.Finish()

	test.Send("hello.world 42.15 1422698155\n")

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

	test.Send("hello.world 42.15 1422698155\nmetric.name -72.11 1422698155\n")

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

func TestUDPWithoutTrailingNewline(t *testing.T) {
	test := newUDPTestCase(t)
	defer test.Finish()

	test.Send("hello.world 42.15 1422698155\nmetric.name -33.14 1422698155")

	select {
	case msg := <-test.rcvChan:
		test.Eq(msg, points.OnePoint("hello.world", 42.15, 1422698155))
	default:
		t.Fatalf("Message #0 not received")
	}

	select {
	case msg := <-test.rcvChan:
		test.Eq(msg, points.OnePoint("metric.name", -33.14, 1422698155))
	default:
		t.Fatalf("Message #1 not received")
	}
}
