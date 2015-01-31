package carbon

import (
	"net"
	"testing"
	"time"
)

type tcpTestCase struct {
	*testing.T
	receiver *TcpReceiver
	conn     net.Conn
	rcvChan  chan *Message
}

func newTcpTestCase(t *testing.T) *tcpTestCase {
	test := &tcpTestCase{
		T: t,
	}

	addr, err := net.ResolveTCPAddr("tcp", "localhost:1818")
	if err != nil {
		t.Fatal(err)
	}

	test.rcvChan = make(chan *Message, 128)
	test.receiver = NewTcpReceiver(test.rcvChan)
	// defer receiver.Stop()

	if err = test.receiver.Listen(addr); err != nil {
		t.Fatal(err)
	}

	test.conn, err = net.Dial("tcp", addr.String())
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

func (test *tcpTestCase) Eq(a *Message, b *Message) {
	if a.Name != b.Name || a.Value != b.Value || a.Timestamp != b.Timestamp {
		test.Fatalf("%#v != %#v", a, b)
	}
}

func TestTcpReceiver1(t *testing.T) {
	test := newTcpTestCase(t)
	defer test.Finish()

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

func TestTcpReceiver2(t *testing.T) {
	test := newTcpTestCase(t)
	defer test.Finish()

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
