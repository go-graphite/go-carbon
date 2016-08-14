package receiver

import (
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/lomik/go-carbon/cache"
	"github.com/lomik/go-carbon/points"
	"github.com/stretchr/testify/assert"
)

func TestStopUDP(t *testing.T) {
	assert := assert.New(t)

	addr, err := net.ResolveUDPAddr("udp", ":0")
	assert.NoError(err)

	for i := 0; i < 10; i++ {
		r, err := New("udp://"+addr.String(), cache.New())
		assert.NoError(err)
		addr = r.(*UDP).Addr().(*net.UDPAddr) // listen same port in next iteration
		r.Stop()
	}
}

func TestStopTCP(t *testing.T) {
	assert := assert.New(t)

	addr, err := net.ResolveTCPAddr("tcp", ":0")
	assert.NoError(err)

	for i := 0; i < 10; i++ {
		r, err := New("tcp://"+addr.String(), cache.New())
		assert.NoError(err)
		addr = r.(*TCP).Addr().(*net.TCPAddr) // listen same port in next iteration
		r.Stop()
	}
}

func TestStopPickle(t *testing.T) {
	assert := assert.New(t)

	addr, err := net.ResolveTCPAddr("tcp", ":0")
	assert.NoError(err)

	for i := 0; i < 10; i++ {
		r, err := New("pickle://"+addr.String(), cache.New())
		assert.NoError(err)
		addr = r.(*TCP).Addr().(*net.TCPAddr) // listen same port in next iteration
		r.Stop()
	}
}

func TestStopConnectedTCP(t *testing.T) {
	test := newTCPTestCase(t, false)
	defer test.Finish()

	metric := "hello.world"
	test.Send(fmt.Sprintf("%s 42.15 1422698155\n", metric))
	test.GetEq(metric, points.OnePoint(metric, 42.15, 1422698155))

	test.receiver.Stop()
	test.receiver = nil
	time.Sleep(10 * time.Millisecond)

	test.Send("metric.name -72.11 1422698155\n")

	_, ok := test.Get("metric.name")
	assert.False(t, ok, "Metric was sent despite stopped receiver")
}
