package cache

import (
	"net"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestStopCarbonLink(t *testing.T) {
	assert := assert.New(t)

	addr, err := net.ResolveTCPAddr("tcp", ":0")
	assert.NoError(err)

	cache := New()

	for i := 0; i < 10; i++ {
		listener := NewCarbonlinkListener(cache)
		assert.NoError(listener.Listen(addr))
		addr = listener.Addr().(*net.TCPAddr) // listen same port in next iteration
		listener.Stop()
	}
}
