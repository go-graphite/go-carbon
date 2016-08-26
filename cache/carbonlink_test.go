package cache

import (
	"bytes"
	"encoding/binary"
	"net"
	"reflect"
	"testing"
	"time"

	"github.com/lomik/go-carbon/points"
	"github.com/stretchr/testify/assert"
)

const sampleCacheQuery = "\x00\x00\x00Y\x80\x02}q\x01(U\x06metricq\x02U,carbon.agents.carbon_agent_server.cache.sizeq\x03U\x04typeq\x04U\x0bcache-queryq\x05u."
const sampleCacheQuery2 = "\x00\x00\x00Y\x80\x02}q\x01(U\x06metricq\x02U,carbon.agents.carbon_agent_server.param.sizeq\x03U\x04typeq\x04U\x0bcache-queryq\x05u."

func TestCarbonlink(t *testing.T) {
	assert := assert.New(t)

	cache := New()
	cache.SetOutputChanSize(0)
	cache.Start()

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

	time.Sleep(50 * time.Millisecond)

	addr, err := net.ResolveTCPAddr("tcp", "localhost:0")
	assert.NoError(err)

	carbonlink := NewCarbonlinkListener(cache.Query())
	defer carbonlink.Stop()

	assert.NoError(carbonlink.Listen(addr))

	NewClient := func() (conn net.Conn, cleanup func()) {
		conn, err := net.Dial("tcp", carbonlink.Addr().String())
		assert.NoError(err)

		conn.SetDeadline(time.Now().Add(time.Second))
		return conn, func() { conn.Close() }
	}

	var replyLength int32
	var data []byte

	/* MESSAGE 1 */
	conn, cleanup := NewClient()
	_, err = conn.Write([]byte(sampleCacheQuery))
	assert.NoError(err)

	err = binary.Read(conn, binary.BigEndian, &replyLength)
	assert.NoError(err)

	data = make([]byte, replyLength)

	err = binary.Read(conn, binary.BigEndian, data)
	assert.NoError(err)

	// {u'datapoints': [(1422797285, 42.17)]}
	assert.Equal("\x80\x02}(X\n\x00\x00\x00datapoints](J\xe5)\xceTG@E\x15\xc2\x8f\\(\xf6\x86eu.", string(data))
	cleanup()

	/* MESSAGE 2 */
	conn, cleanup = NewClient()
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
	cleanup()

	/* MESSAGE 3 */
	/* Remove carbon.agents.carbon_agent_server.param.size from cache and request again */
	conn, cleanup = NewClient()
	for {
		c := <-cache.Out()
		cache.Confirm() <- c
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
	cleanup()

	/* WRONG MESSAGE TEST */
	conn, cleanup = NewClient()
	_, err = conn.Write([]byte("\x00\x00\x00\x05aaaaa"))
	assert.NoError(err)

	err = binary.Read(conn, binary.BigEndian, &replyLength)

	assert.Error(err)
	cleanup()

	/* Invalid querytype */
	conn, cleanup = NewClient()
	_, err = conn.Write([]byte("\x00\x00\x00(\x80\x02}q\x00(U\x06metricq\x01U\x03barq\x02U\x04typeq\x03U\x03fooq\x04u."))
	assert.NoError(err)

	err = binary.Read(conn, binary.BigEndian, &replyLength)

	assert.Error(err)
	cleanup()
}

func TestCarbonlinkErrors(t *testing.T) {
	assert := assert.New(t)

	cache := New()
	cache.SetOutputChanSize(0)
	cache.Start()

	addr, err := net.ResolveTCPAddr("tcp", "localhost:0")
	assert.NoError(err)

	carbonlink := NewCarbonlinkListener(cache.Query())
	listenerTimeout := 10 * time.Millisecond
	carbonlink.SetReadTimeout(listenerTimeout)
	defer carbonlink.Stop()

	err = carbonlink.Listen(addr)
	if err != nil {
		t.Errorf("Received %q when attempting to start CarbonlinkListener", err)
	}

	table := []*struct {
		waitAfterWrite time.Duration
		msg            []byte
	}{
		{ // connect and disconnect
			time.Millisecond * 0,
			[]byte{},
		},
		{ // connect, send msg length and disconnect
			time.Millisecond * 0,
			[]byte(sampleCacheQuery2[:4]),
		},
		{ // connect and wait timeout
			listenerTimeout * 2,
			[]byte{},
		},
		{ // connect, send msg length and wait timeout
			listenerTimeout * 2,
			[]byte(sampleCacheQuery2[:4]),
		},
		{ // send broken pickle
			listenerTimeout * 2,
			[]byte(sampleCacheQuery2[:len(sampleCacheQuery2)-1] + "a"),
		},
	}

	for _, test := range table {
		remoteaddr, _ := net.ResolveTCPAddr("tcp", carbonlink.Addr().String())
		conn, err := net.DialTCP("tcp", addr, remoteaddr)
		assert.NoError(err)

		if len(test.msg) > 0 {
			_, err = conn.Write(test.msg)
			if err != nil {
				t.Errorf("Received %q when attempting to write to CarbonlinkListener", err)
			}
		}
		time.Sleep(test.waitAfterWrite)
		if test.waitAfterWrite > listenerTimeout {
			conn.SetWriteBuffer(0)
			n, err := conn.Write([]byte("x"))
			if err == nil || n > 0 {
				t.Errorf("CarbonlinkListener did not close connection as expected")
			}
		}
		conn.Close()

	}
}

func TestReadCarbonlinkRequest(t *testing.T) {

	tests := []struct {
		name    string
		data    []byte
		want    []byte
		wantErr bool
	}{
		{name: "Empty reader",
			data:    []byte{},
			wantErr: true},
		// Length is expected to be 32 bits
		{name: "Short Length",
			data:    []byte("\x00"),
			wantErr: true},
		{name: "Length, but no body",
			data:    []byte("\x00\x00\x00\x01"),
			wantErr: true},
		{name: "Valid Length and body",
			data: []byte("\x00\x00\x00\x01x"),
			want: []byte("x"),
		},
		{name: "Length zero, no body",
			data: []byte("\x00\x00\x00\x00"),
			want: []byte(""),
		},
	}
	for _, tt := range tests {
		got, err := ReadCarbonlinkRequest(bytes.NewReader(tt.data))
		if (err != nil) != tt.wantErr {
			t.Errorf("%q. ReadCarbonlinkRequest() error = %v, wantErr %v", tt.name, err, tt.wantErr)
			continue
		}
		if !reflect.DeepEqual(got, tt.want) {
			t.Errorf("%q. ReadCarbonlinkRequest() = %v, want %v", tt.name, got, tt.want)
		}
	}
}

func TestNewCarbonlinkRequest(t *testing.T) {
	want := &CarbonlinkRequest{}
	got := NewCarbonlinkRequest()
	if !reflect.DeepEqual(got, want) {
		t.Errorf("NewCarbonlinkRequest() = %v, want %v", got, want)
	}
}

func TestParseCarbonlinkRequest(t *testing.T) {
	tests := []struct {
		name    string
		data    []byte
		want    *CarbonlinkRequest
		wantErr bool
	}{
		{name: "Good query",
			data: []byte(sampleCacheQuery)[4:],
			want: &CarbonlinkRequest{
				Type:   "cache-query",
				Metric: "carbon.agents.carbon_agent_server.cache.size"},
		},
		{name: "Invalid query type",
			data: []byte("\x80\x02}q\x00(U\x06Metricq\x01U\x03barq\x02U\x04Typeq\x03U\x03fooq\x04u."),
			want: &CarbonlinkRequest{
				Type:   "foo",
				Metric: "bar",
			},
		},
		{name: "Garbage",
			data:    []byte("garbage"),
			wantErr: true,
		},
	}
	for _, tt := range tests {
		got, err := ParseCarbonlinkRequest(tt.data)
		if (err != nil) != tt.wantErr {
			t.Errorf("%q. ParseCarbonlinkRequest() error = %v, wantErr %v", tt.name, err, tt.wantErr)
			continue
		}
		if !reflect.DeepEqual(got, tt.want) {
			t.Errorf("%q. ParseCarbonlinkRequest() = %v, want %v", tt.name, got, tt.want)
		}
	}
}
