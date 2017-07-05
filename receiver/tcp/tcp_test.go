package tcp

import (
	"net"
	"testing"
	"time"

	"github.com/lomik/go-carbon/points"
	"github.com/lomik/go-carbon/receiver"
)

type tcpTestCase struct {
	*testing.T
	receiver *TCP
	conn     net.Conn
	rcvChan  chan *points.Points
}

func newTCPTestCase(t *testing.T, protocol string) *tcpTestCase {
	test := &tcpTestCase{
		T: t,
	}

	addr, err := net.ResolveTCPAddr("tcp", "localhost:0")
	if err != nil {
		t.Fatal(err)
	}

	test.rcvChan = make(chan *points.Points, 128)

	r, err := receiver.New(protocol, map[string]interface{}{
		"protocol": protocol,
		"listen":   addr.String(),
	},
		func(p *points.Points) {
			test.rcvChan <- p
		},
	)

	if err != nil {
		t.Fatal(err)
	}

	test.receiver = r.(*TCP)

	test.conn, err = net.Dial("tcp", test.receiver.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	// defer conn.Close()
	return test
}

func (test *tcpTestCase) Finish() {
	if test.conn != nil {
		test.conn.Close()
		test.conn = nil
	}
	if test.receiver != nil {
		test.receiver.Stop()
		test.receiver = nil
	}
}

func (test *tcpTestCase) Send(text string) {
	if _, err := test.conn.Write([]byte(text)); err != nil {
		test.Fatal(err)
	}
}

func (test *tcpTestCase) Eq(a *points.Points, b *points.Points) {
	if !a.Eq(b) {
		test.Fatalf("%#v != %#v", a, b)
	}
}

func TestTCP1(t *testing.T) {
	test := newTCPTestCase(t, "tcp")
	defer test.Finish()

	test.Send("hello.world 42.15 1422698155\n")

	time.Sleep(10 * time.Millisecond)

	select {
	case msg := <-test.rcvChan:
		test.Eq(msg, points.OnePoint("hello.world", 42.15, 1422698155))
	default:
		t.Fatalf("Message #0 not received")
	}
}

func TestTCP2(t *testing.T) {
	test := newTCPTestCase(t, "tcp")
	defer test.Finish()

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

func TestTCPIssue176(t *testing.T) {
	test := newTCPTestCase(t, "tcp")
	defer test.Finish()

	test.Send("hello.world 1.096378e+06 1422698155\nmetric.name 1.096378e+06 1422698155\n")

	time.Sleep(10 * time.Millisecond)

	select {
	case msg := <-test.rcvChan:
		test.Eq(msg, points.OnePoint("hello.world", 1096378.0, 1422698155))
	default:
		t.Fatalf("Message #0 not received")
	}

	select {
	case msg := <-test.rcvChan:
		test.Eq(msg, points.OnePoint("metric.name", 1096378.0, 1422698155))
	default:
		t.Fatalf("Message #1 not received")
	}
}
