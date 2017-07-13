package http

import (
	"net"
	"testing"

	"github.com/lomik/go-carbon/receiver"
	"github.com/stretchr/testify/assert"
)

func TestStopHTTP(t *testing.T) {
	assert := assert.New(t)

	addr, err := net.ResolveTCPAddr("tcp", ":0")
	assert.NoError(err)

	for i := 0; i < 10; i++ {
		r, err := receiver.New("http", map[string]interface{}{
			"protocol": "http",
			"listen":   addr.String(),
		},
			nil,
		)
		assert.NoError(err)
		addr = r.(*HTTP).Addr().(*net.TCPAddr) // listen same port in next iteration
		r.Stop()
	}
}
