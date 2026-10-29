package udp

import (
	"net"
	"os"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/points"
	"github.com/go-graphite/go-carbon/receiver"
)

type udpTestCase struct {
	*testing.T
	receiver *UDP
	conn     net.Conn
	rcvChan  chan *points.Points
}

func TestMain(m *testing.M) {
	Register()
	os.Exit(m.Run())
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

	return test
}

func newUDPTestCase(t *testing.T) *udpTestCase {
	return newUDPTestCaseWithOptions(t, false)
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
}

func (test *udpTestCase) Eq(want *points.Points) {
	test.Helper()
	timer := time.NewTimer(time.Second)
	defer timer.Stop()
	select {
	case got := <-test.rcvChan:
		if !got.Eq(want) {
			test.Fatalf("%#v != %#v", got, want)
		}
	case <-timer.C:
		test.Fatal("message not received before deadline")
	}
}

func TestUDP1(t *testing.T) {
	test := newUDPTestCase(t)
	defer test.Finish()

	test.Send("hello.world 42.15 1422698155\n")

	test.Eq(points.OnePoint("hello.world", 42.15, 1422698155))
}

func TestUDP2(t *testing.T) {
	test := newUDPTestCase(t)
	defer test.Finish()

	test.Send("hello.world 42.15 1422698155\nmetric.name -72.11 1422698155\n")

	test.Eq(points.OnePoint("hello.world", 42.15, 1422698155))

	test.Eq(points.OnePoint("metric.name", -72.11, 1422698155))
}

func TestUDPWithoutTrailingNewline(t *testing.T) {
	test := newUDPTestCase(t)
	defer test.Finish()

	test.Send("hello.world 42.15 1422698155\nmetric.name -33.14 1422698155")

	test.Eq(points.OnePoint("hello.world", 42.15, 1422698155))

	test.Eq(points.OnePoint("metric.name", -33.14, 1422698155))
}
