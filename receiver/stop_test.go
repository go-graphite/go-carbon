package receiver

import (
	"net"
	"testing"
	"time"

	"github.com/lomik/go-carbon/points"
	"github.com/stretchr/testify/assert"
)

func TestStopUDP(t *testing.T) {
	assert := assert.New(t)

	addr, err := net.ResolveUDPAddr("udp", ":0")
	assert.NoError(err)

	ch := make(chan *points.Points, 128)

	for i := 0; i < 10; i++ {
		listener := NewUDP(ch)
		assert.NoError(listener.Listen(addr))
		addr = listener.Addr().(*net.UDPAddr) // listen same port in next iteration
		listener.Stop()
	}
}

func TestStopTCP(t *testing.T) {
	assert := assert.New(t)

	addr, err := net.ResolveTCPAddr("tcp", ":0")
	assert.NoError(err)

	ch := make(chan *points.Points, 128)

	for i := 0; i < 10; i++ {
		listener := NewTCP(ch)
		assert.NoError(listener.Listen(addr))
		addr = listener.Addr().(*net.TCPAddr) // listen same port in next iteration
		listener.Stop()
	}
}

func TestStopPickle(t *testing.T) {
	assert := assert.New(t)

	addr, err := net.ResolveTCPAddr("tcp", ":0")
	assert.NoError(err)

	ch := make(chan *points.Points, 128)

	for i := 0; i < 10; i++ {
		listener := NewPickle(ch)
		assert.NoError(listener.Listen(addr))
		addr = listener.Addr().(*net.TCPAddr) // listen same port in next iteration
		listener.Stop()
	}
}

func TestStopConnectedTCP(t *testing.T) {
	test := newTCPTestCase(t, false)
	defer test.Finish()

	ch := test.rcvChan
	test.Send("hello.world 42.15 1422698155\n")
	time.Sleep(10 * time.Millisecond)

	select {
	case msg := <-ch:
		test.Eq(msg, points.OnePoint("hello.world", 42.15, 1422698155))
	default:
		t.Fatalf("Message #0 not received")
	}

	test.receiver.Stop()
	test.receiver = nil
	time.Sleep(10 * time.Millisecond)

	test.Send("metric.name -72.11 1422698155\n")
	time.Sleep(10 * time.Millisecond)

	select {
	case <-ch:
		t.Fatalf("Message #0 received")
	default:
	}
}
