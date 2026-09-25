package tcp

import (
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/points"
	"github.com/go-graphite/go-carbon/receiver"
	"github.com/stretchr/testify/assert"
)

func TestStopTCP(t *testing.T) {
	assert := assert.New(t)

	addr, err := net.ResolveTCPAddr("tcp", ":0")
	assert.NoError(err)

	for i := 0; i < 10; i++ {
		r, err := receiver.New("tcp", map[string]interface{}{
			"protocol": "tcp",
			"listen":   addr.String(),
		},
			nil,
		)
		assert.NoError(err)
		addr = r.(*TCP).Addr().(*net.TCPAddr) // listen same port in next iteration
		r.Stop()
	}
}

func TestStopTCPWorkers(t *testing.T) {
	addr, err := net.ResolveTCPAddr("tcp", ":0")
	if err != nil {
		t.Fatal(err)
	}

	r, err := receiver.New("tcp", map[string]interface{}{
		"protocol": "tcp",
		"listen":   addr.String(),
		"workers":  2,
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	r.Stop()
}

func TestStopTCPWorkersDrainsBatches(t *testing.T) {
	addr, err := net.ResolveTCPAddr("tcp", "localhost:0")
	if err != nil {
		t.Fatal(err)
	}

	firstTwo := make(chan struct{}, 2)
	release := make(chan struct{})
	received := make(chan *points.Points, 4)
	var calls int32
	r, err := receiver.New("tcp", map[string]interface{}{
		"protocol":    "tcp",
		"listen":      addr.String(),
		"workers":     2,
		"buffer-size": 1,
	}, func(p *points.Points) {
		received <- p
		if atomic.AddInt32(&calls, 1) <= 2 {
			firstTwo <- struct{}{}
			<-release
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	rcv := r.(*TCP)
	released := false
	defer func() {
		if !released {
			close(release)
		}
		r.Stop()
	}()

	for _, line := range []string{"metric.a 1 1\n", "metric.b 1 1\n"} {
		rcv.batchBuffer <- tcpBatch{data: []byte(line), peer: "test"}
	}

	for i := 0; i < 2; i++ {
		select {
		case <-firstTwo:
		case <-time.After(time.Second):
			t.Fatal("workers did not begin processing")
		}
	}
	rcv.batchBuffer <- tcpBatch{data: []byte("metric.c 1 1\n"), peer: "test"}

	server, client := net.Pipe()
	defer client.Close()
	rcv.producers.Add(1)
	rcv.Go(func(exit chan bool) {
		defer rcv.producers.Done()
		rcv.HandleConnection(server)
	})
	written := make(chan error, 1)
	go func() {
		_, err := client.Write([]byte("metric.d 1 1\n"))
		written <- err
	}()
	if err := <-written; err != nil {
		t.Fatal(err)
	}

	stopped := make(chan struct{})
	go func() {
		r.Stop()
		close(stopped)
	}()
	select {
	case <-stopped:
		t.Fatal("Stop returned before queued batches drained")
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	released = true

	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("Stop did not finish")
	}
	if len(received) != 4 {
		t.Fatalf("received %d metrics, want 4", len(received))
	}
}

func TestStopPickle(t *testing.T) {
	assert := assert.New(t)

	addr, err := net.ResolveTCPAddr("tcp", ":0")
	assert.NoError(err)

	for i := 0; i < 10; i++ {
		r, err := receiver.New("pickle", map[string]interface{}{
			"protocol": "pickle",
			"listen":   addr.String(),
		},
			nil,
		)
		assert.NoError(err)
		addr = r.(*TCP).Addr().(*net.TCPAddr) // listen same port in next iteration
		r.Stop()
	}
}

func TestStopConnectedTCP(t *testing.T) {
	test := newTCPTestCase(t, "tcp")
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
