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

	reqData, err := ReadCarbonlinkRequest(reader)
	assert.NoError(err)

	req, err := ParseCarbonlinkRequest(reqData)

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

func TestCarbonlinkErrors(t *testing.T) {
	assert := assert.New(t)

	cache := New()
	cache.Start()
	cache.SetOutputChanSize(0)

	addr, err := net.ResolveTCPAddr("tcp", "localhost:0")
	assert.NoError(err)

	carbonlink := NewCarbonlinkListener(cache.Query())
	carbonlink.SetReadTimeout(10 * time.Millisecond)
	defer carbonlink.Stop()

	assert.NoError(carbonlink.Listen(addr))

	table := []*struct {
		closeAfterWrite bool
		msg             []byte
		logContains     []string
	}{
		{ // connect and disconnect
			true,
			[]byte{},
			[]string{
				"] D [carbonlink] read carbonlink request from",
				"Can't read message length",
				// "EOF",
			},
		},
		{ // connect, send msg length and disconnect
			true,
			[]byte(sampleCacheQuery2[:4]),
			[]string{
				"] D [carbonlink] read carbonlink request from",
				"Can't read message body",
				// "EOF",
			},
		},
		{ // connect and wait timeout
			false,
			[]byte{},
			[]string{
				"] D [carbonlink] read carbonlink request from",
				"Can't read message length",
				// "i/o timeout",
			},
		},
		{ // connect, send msg length and wait timeout
			false,
			[]byte(sampleCacheQuery2[:4]),
			[]string{
				"] D [carbonlink] read carbonlink request from",
				"Can't read message body",
				// "i/o timeout",
			},
		},
		{ // send broken pickle
			false,
			[]byte(sampleCacheQuery2[:len(sampleCacheQuery2)-1] + "a"),
			[]string{
				"] W [carbonlink] parse carbonlink request from",
				"Pickle Machine failed",
			},
		},
	}

	for _, test := range table {
		logging.TestWithLevel("debug", func(log *bytes.Buffer) {
			conn, err := net.Dial("tcp", carbonlink.Addr().String())
			assert.NoError(err)

			if len(test.msg) > 0 {
				_, err = conn.Write(test.msg)
				assert.NoError(err)
			}

			if test.closeAfterWrite {
				conn.Close()
			} else {
				defer conn.Close()
			}

			time.Sleep(100 * time.Millisecond)

			for _, logMsg := range test.logContains {
				assert.Contains(log.String(), logMsg)
			}
		})
	}
}
