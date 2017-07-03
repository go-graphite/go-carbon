package receiver

import (
	"net"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestStopUDP(t *testing.T) {
	assert := assert.New(t)

	addr, err := net.ResolveUDPAddr("udp", ":0")
	assert.NoError(err)

	for i := 0; i < 10; i++ {
		r, err := New("udp://" + addr.String())
		assert.NoError(err)
		addr = r.(*UDP).Addr().(*net.UDPAddr) // listen same port in next iteration
		r.Stop()
	}
}
