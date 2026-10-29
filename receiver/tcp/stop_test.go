package tcp

import (
	"errors"
	"net"
	"syscall"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/points"
	"github.com/go-graphite/go-carbon/receiver"
)

func TestStopTCP(t *testing.T) {
	addr, err := net.ResolveTCPAddr("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 10; i++ {
		r, err := receiver.New("tcp", map[string]interface{}{
			"protocol": "tcp",
			"listen":   addr.String(),
		},
			nil,
		)
		if err != nil {
			t.Fatal(err)
		}
		addr = r.(*TCP).Addr().(*net.TCPAddr) // listen same port in next iteration
		r.Stop()
	}
}

func TestStopPickle(t *testing.T) {
	addr, err := net.ResolveTCPAddr("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 10; i++ {
		r, err := receiver.New("pickle", map[string]interface{}{
			"protocol": "pickle",
			"listen":   addr.String(),
		},
			nil,
		)
		if err != nil {
			t.Fatal(err)
		}
		addr = r.(*TCP).Addr().(*net.TCPAddr) // listen same port in next iteration
		r.Stop()
	}
}

func TestStopConnectedTCP(t *testing.T) {
	test := newTCPTestCase(t, "tcp")
	defer test.Finish()

	ch := test.rcvChan
	test.Send("hello.world 42.15 1422698155\n")
	select {
	case msg := <-ch:
		test.Eq(msg, points.OnePoint("hello.world", 42.15, 1422698155))
	case <-time.After(5 * time.Second):
		t.Fatalf("Message #0 not received")
	}

	test.receiver.Stop()
	test.receiver = nil
	test.WaitClosed()
	_, err := test.conn.Write([]byte("metric.name -72.11 1422698155\n"))
	if err != nil && !errors.Is(err, syscall.EPIPE) && !errors.Is(err, syscall.ECONNRESET) {
		t.Fatal(err)
	}

	select {
	case <-ch:
		t.Fatalf("Message #0 received")
	default:
	}
}
