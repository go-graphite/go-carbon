package tcp

import (
	"fmt"
	"net"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/cache"
	"github.com/go-graphite/go-carbon/points"
	"github.com/go-graphite/go-carbon/receiver"
)

type tcpTestCase struct {
	*testing.T
	receiver *TCP
	conn     net.Conn
	rcvChan  chan *points.Points
}

func TestMain(m *testing.M) {
	Register()
	os.Exit(m.Run())
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

func TestTCPWorkersProcessLinesConcurrently(t *testing.T) {
	addr, err := net.ResolveTCPAddr("tcp", "localhost:0")
	if err != nil {
		t.Fatal(err)
	}

	started := make(chan struct{}, 2)
	release := make(chan struct{})
	r, err := receiver.New("tcp", map[string]interface{}{
		"protocol": "tcp",
		"listen":   addr.String(),
		"workers":  2,
	}, func(*points.Points) {
		started <- struct{}{}
		<-release
	})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Stop()
	defer close(release)

	conn, err := net.Dial("tcp", r.(*TCP).Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	send := func(line string) {
		t.Helper()
		if _, err := conn.Write([]byte(line)); err != nil {
			t.Fatal(err)
		}
		select {
		case <-started:
		case <-time.After(time.Second):
			t.Fatal("workers did not process two lines concurrently")
		}
	}
	send("one 1 1\n")
	send("two 2 2\n")
}

func BenchmarkTCPSingleConnection(b *testing.B) {
	const pointsPerWrite = 256

	var payload strings.Builder
	for i := 0; i < pointsPerWrite; i++ {
		fmt.Fprintf(&payload, "benchmark.metric.%d 1 1\n", i)
	}
	data := []byte(payload.String())

	for _, workers := range []int{1, 4} {
		b.Run(fmt.Sprintf("workers=%d", workers), func(b *testing.B) {
			addr, err := net.ResolveTCPAddr("tcp", "localhost:0")
			if err != nil {
				b.Fatal(err)
			}

			core := cache.New()
			expected := int64(b.N * pointsPerWrite)
			var received int64
			done := make(chan struct{})

			r, err := receiver.New("tcp", map[string]interface{}{
				"protocol":    "tcp",
				"listen":      addr.String(),
				"buffer-size": 1024,
				"workers":     workers,
			}, func(p *points.Points) {
				core.Add(p)
				if atomic.AddInt64(&received, int64(len(p.Data))) == expected {
					close(done)
				}
			})
			if err != nil {
				b.Fatal(err)
			}

			conn, err := net.Dial("tcp", r.(*TCP).Addr().String())
			if err != nil {
				r.Stop()
				b.Fatal(err)
			}

			b.SetBytes(int64(len(data)))
			b.ResetTimer()
for i := 0; i < b.N; i++ {
				n, err := conn.Write(data)
				if err != nil {
					b.Fatal(err)
				}
				if n != len(data) {
					b.Fatalf("short write: %d of %d bytes", n, len(data))
				}
			}
			<-done
			b.StopTimer()

			conn.Close()
			r.Stop()
			b.ReportMetric(float64(expected)/b.Elapsed().Seconds(), "points/s")
		})
	}
}
