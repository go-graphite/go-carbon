package receiver

import (
	"net"
	"testing"

	"github.com/lomik/go-carbon/points"
	"github.com/lomik/go-carbon/receiver"
	"github.com/stretchr/testify/assert"
)

func TestUDPStop(t *testing.T) {
	assert := assert.New(t)

	udpAddr, err := net.ResolveUDPAddr("udp", ":0")
	assert.NoError(err)

	ch := make(chan *points.Points, 128)

	for i := 0; i < 10; i++ {
		udpListener := receiver.NewUDP(ch)
		assert.NoError(udpListener.Listen(udpAddr))
		udpAddr = udpListener.Addr().(*net.UDPAddr) // listen same port in next iteration
		udpListener.Stop()
	}
}
