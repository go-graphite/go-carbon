package receiver

import (
	"net"
	"testing"
	"time"

	"github.com/lomik/go-carbon/cache"
	"github.com/lomik/go-carbon/logging"
	"github.com/lomik/go-carbon/points"
	"github.com/stretchr/testify/assert"
)

type udpTestCase struct {
	*testing.T
	receiver *UDP
	conn     net.Conn
	cache    *cache.Cache
	sendTime time.Time
}

func newUDPTestCaseWithOptions(t *testing.T, logIncomplete bool) *udpTestCase {
	test := &udpTestCase{
		T:        t,
		cache:    cache.New(),
		sendTime: time.Time{},
	}

	addr, err := net.ResolveUDPAddr("udp", "localhost:0")
	if err != nil {
		t.Fatal(err)
	}

	r, err := New("udp://"+addr.String(), test.cache, UDPLogIncomplete(logIncomplete))
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
	test.sendTime = time.Now()
	time.Sleep(5 * time.Millisecond)
}

func (test *udpTestCase) Get(metric string) (*points.Points, bool) {
	delta := time.Now().Sub(test.sendTime).Seconds()/1000 - 10
	if delta < 0 {
		time.Sleep(time.Duration(-delta) * time.Millisecond)
	}

	return test.cache.GetMetric(metric)
}

func (test *udpTestCase) GetEq(metric string, b *points.Points) {
	m, ok := test.Get(metric)
	if ok {
		test.Eq(m, b)
	} else {
		test.Fatalf("Metric %s not found", metric)
	}
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
	test.GetEq("hello.world", points.OnePoint("hello.world", 42.15, 1422698155))
}

func TestUDP2(t *testing.T) {
	test := newUDPTestCase(t)
	defer test.Finish()

	test.Send("hello.world 42.15 1422698155\nmetric.name -72.11 1422698155\n")
	test.GetEq("hello.world", points.OnePoint("hello.world", 42.15, 1422698155))
	test.GetEq("metric.name", points.OnePoint("metric.name", -72.11, 1422698155))
}

func TestChunkedUDP(t *testing.T) {
	test := newUDPTestCase(t)
	defer test.Finish()

	test.Send("hello.world 42.15 1422698155\nmetri")
	test.Send("c.name -72.11 1422698155\n")

	test.GetEq("hello.world", points.OnePoint("hello.world", 42.15, 1422698155))
	test.GetEq("metric.name", points.OnePoint("metric.name", -72.11, 1422698155))
}

func TestLogIncompleteMessage(t *testing.T) {
	assert := assert.New(t)

	// 3 lines
	logging.Test(func(log logging.TestOut) {
		test := newUDPTestCaseLogIncomplete(t)
		defer test.Finish()

		test.Send("metric1 42 1422698155\nmetric2 43 1422698155\nmetric3 4")
		assert.Contains(log.String(), "metric1 42 1422698155\\n...(21 bytes)...\\nmetric3 4")
	})

	// > 3 lines
	logging.Test(func(log logging.TestOut) {
		test := newUDPTestCaseLogIncomplete(t)
		defer test.Finish()

		test.Send("metric1 42 1422698155\nmetric2 43 1422698155\nmetric3 44 1422698155\nmetric4 45 ")
		assert.Contains(log.String(), "metric1 42 1422698155\\n...(43 bytes)...\\nmetric4 45 ")
	})

	// 2 lines
	logging.Test(func(log logging.TestOut) {
		test := newUDPTestCaseLogIncomplete(t)
		defer test.Finish()

		test.Send("metric1 42 1422698155\nmetric2 43 14226981")
		assert.Contains(log.String(), "metric1 42 1422698155\\nmetric2 43 14226981")
	})

	// single line
	logging.Test(func(log logging.TestOut) {
		test := newUDPTestCaseLogIncomplete(t)
		defer test.Finish()

		test.Send("metric1 42 1422698155")
		assert.Contains(log.String(), "metric1 42 1422698155")
	})
}
