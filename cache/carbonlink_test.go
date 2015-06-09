package cache

import (
	"bytes"
	"encoding/binary"
	"io"
	"net"
	"testing"
	"time"

	"github.com/lomik/go-carbon/logging"
	"github.com/lomik/go-carbon/points"
	"github.com/stretchr/testify/assert"
)

const sampleCacheQuery = "\x00\x00\x00Y\x80\x02}q\x01(U\x06metricq\x02U,carbon.agents.carbon_agent_server.cache.sizeq\x03U\x04typeq\x04U\x0bcache-queryq\x05u."
const sampleCacheQuery2 = "\x00\x00\x00Y\x80\x02}q\x01(U\x06metricq\x02U,carbon.agents.carbon_agent_server.param.sizeq\x03U\x04typeq\x04U\x0bcache-queryq\x05u."

func TestCarbonlinkRead(t *testing.T) {
	assert := assert.New(t)

	reader := bytes.NewReader([]byte(sampleCacheQuery))

	req, err := ReadCarbonlinkRequest(reader)

	assert.NoError(err)
	assert.NotNil(req)
	assert.Equal("cache-query", req.Type)
	assert.Equal("carbon.agents.carbon_agent_server.cache.size", req.Metric)
}

func TestCarbonlink(t *testing.T) {
	assert := assert.New(t)

	cache := New()
	cache.Start()
	cache.SetOutputChanSize(0)

	msg1 := points.OnePoint(
		"carbon.agents.carbon_agent_server.cache.size",
		42.17,
		1422797285,
	)

	msg2 := points.OnePoint(
		"carbon.agents.carbon_agent_server.param.size",
		-42.14,
		1422797267,
	)

	msg3 := points.OnePoint(
		"carbon.agents.carbon_agent_server.param.size",
		15,
		1422795966,
	)

	cache.In() <- msg1
	cache.In() <- msg2
	cache.In() <- msg3

	defer cache.Stop()

	addr, err := net.ResolveTCPAddr("tcp", "localhost:0")
	assert.NoError(err)

	carbonlink := NewCarbonlinkListener(cache.Query())
	defer carbonlink.Stop()

	assert.NoError(carbonlink.Listen(addr))

	conn, err := net.Dial("tcp", carbonlink.Addr().String())
	assert.NoError(err)

	conn.SetDeadline(time.Now().Add(time.Second))
	defer conn.Close()

	var replyLength int32
	var data []byte

	/* MESSAGE 1 */

	_, err = conn.Write([]byte(sampleCacheQuery))
	assert.NoError(err)

	err = binary.Read(conn, binary.BigEndian, &replyLength)
	assert.NoError(err)

	data = make([]byte, replyLength)

	err = binary.Read(conn, binary.BigEndian, data)
	assert.NoError(err)

	// {u'datapoints': [(1422797285, 42.17)]}
	assert.Equal("\x80\x02}(X\n\x00\x00\x00datapoints](J\xe5)\xceTG@E\x15\xc2\x8f\\(\xf6\x86eu.", string(data))

	/* MESSAGE 2 */
	_, err = conn.Write([]byte(sampleCacheQuery2))
	assert.NoError(err)

	err = binary.Read(conn, binary.BigEndian, &replyLength)
	assert.NoError(err)

	data = make([]byte, replyLength)

	err = binary.Read(conn, binary.BigEndian, data)
	assert.NoError(err)

	// {u'datapoints': [(1422797267, -42.14), (1422795966, 15.0)]}
	assert.Equal("\x80\x02}(X\n\x00\x00\x00datapoints](J\xd3)\xceTG\xc0E\x11\xeb\x85\x1e\xb8R\x86J\xbe$\xceTG@.\x00\x00\x00\x00\x00\x00\x86eu.",
		string(data))

	/* MESSAGE 3 */
	/* Remove carbon.agents.carbon_agent_server.param.size from cache and request again */

	for {
		c := <-cache.Out()
		if c.Metric == "carbon.agents.carbon_agent_server.param.size" {
			break
		}
	}

	_, err = conn.Write([]byte(sampleCacheQuery2))
	assert.NoError(err)

	err = binary.Read(conn, binary.BigEndian, &replyLength)
	assert.NoError(err)

	data = make([]byte, replyLength)

	err = binary.Read(conn, binary.BigEndian, data)
	assert.NoError(err)

	assert.Equal("\x80\x02}(X\n\x00\x00\x00datapoints](eu.", string(data))

	/* WRONG MESSAGE TEST */
	logging.Test(func(log *bytes.Buffer) { // silent logs
		_, err = conn.Write([]byte("\x00\x00\x00\x05aaaaa"))
		assert.NoError(err)

		err = binary.Read(conn, binary.BigEndian, &replyLength)

		assert.Error(err)
		assert.Equal(io.EOF, err)
	})
}
