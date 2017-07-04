package udp

import (
	"net"
	"testing"

	"github.com/lomik/go-carbon/receiver"
	"github.com/stretchr/testify/assert"
)

func TestStopUDP(t *testing.T) {
	assert := assert.New(t)

	addr, err := net.ResolveUDPAddr("udp", ":0")
	assert.NoError(err)

	for i := 0; i < 10; i++ {
		r, err := receiver.New("udp", map[string]interface{}{
			"protocol": "udp",
			"listen":   addr.String(),
		},
			nil,
		)
		assert.NoError(err)
		addr = r.(*UDP).Addr().(*net.UDPAddr) // listen same port in next iteration
		r.Stop()
	}
}
