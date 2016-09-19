package receiver

import (
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/lomik/go-carbon/cache"
	"github.com/lomik/go-carbon/points"
)

type tcpTestCase struct {
	*testing.T
	receiver *TCP
	conn     net.Conn
	cache    *cache.Cache
	sendTime time.Time
}

func newTCPTestCase(t *testing.T, isPickle bool) *tcpTestCase {
	test := &tcpTestCase{
		T:        t,
		cache:    cache.New(),
		sendTime: time.Time{},
	}

	addr, err := net.ResolveTCPAddr("tcp", "localhost:0")
	if err != nil {
		t.Fatal(err)
	}

	scheme := "tcp"
	if isPickle {
		scheme = "pickle"
	}

	r, err := New(scheme+"://"+addr.String(), test.cache)
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
	test.sendTime = time.Now()
}

func (test *tcpTestCase) Eq(a *points.Points, b *points.Points) {
	if !a.Eq(b) {
		test.Fatalf("%#v != %#v", a, b)
	}
}

func (test *tcpTestCase) Get(metric string) (*points.Points, bool) {
	delta := time.Now().Sub(test.sendTime).Seconds()/1000 - 10
	if delta < 0 {
		time.Sleep(time.Duration(-delta) * time.Millisecond)
	}

	return test.cache.GetMetric(metric)
}

func (test *tcpTestCase) GetEq(metric string, b *points.Points) {
	m, ok := test.Get(metric)
	if ok {
		test.Eq(m, b)
	} else {
		test.Fatalf("Metric %s not found", metric)
	}
}

func TestTCP1(t *testing.T) {
	test := newTCPTestCase(t, false)
	defer test.Finish()

	metric := "hello.world"
	test.Send(fmt.Sprintf("%s 42.15 1422698155\n", metric))

	test.GetEq(metric, points.OnePoint(metric, 42.15, 1422698155))
}

func TestTCP2(t *testing.T) {
	test := newTCPTestCase(t, false)
	defer test.Finish()

	metric1 := "hello.world"
	metric2 := "metric.name"
	test.Send(fmt.Sprintf("%s 42.15 1422698155\n%s -72.11 1422698155\n", metric1, metric2))

	test.GetEq(metric1, points.OnePoint(metric1, 42.15, 1422698155))
	test.GetEq(metric2, points.OnePoint(metric2, -72.11, 1422698155))
}
