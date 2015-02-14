package cache

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"testing"
	"time"

	"devroom.ru/lomik/carbon/points"
)

const sampleCacheQuery = "\x00\x00\x00Y\x80\x02}q\x01(U\x06metricq\x02U,carbon.agents.carbon_agent_server.cache.sizeq\x03U\x04typeq\x04U\x0bcache-queryq\x05u."
const sampleCacheQuery2 = "\x00\x00\x00Y\x80\x02}q\x01(U\x06metricq\x02U,carbon.agents.carbon_agent_server.param.sizeq\x03U\x04typeq\x04U\x0bcache-queryq\x05u."

func TestCarbonlinkRead(t *testing.T) {

	reader := bytes.NewReader([]byte(sampleCacheQuery))

	req, err := ReadCarbonlinkRequest(reader)

	if err != nil {
		t.Fatal(err)
	}

	if req == nil {
		t.Fatal("req is nil")
	}

	if req.Type != "cache-query" {
		t.Fatalf("%#v != \"cache-query\"", req.Type)
	}

	if req.Metric != "carbon.agents.carbon_agent_server.cache.size" {
		t.Fatalf("%#v != \"carbon.agents.carbon_agent_server.cache.size\"", req.Metric)
	}

}

func TestCarbonlink(t *testing.T) {
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

	addr, err := net.ResolveTCPAddr("tcp", "localhost:1919")
	if err != nil {
		t.Fatal(err)
	}

	carbonlink := NewCarbonlinkListener(cache.Query())
	defer carbonlink.Stop()

	if err = carbonlink.Listen(addr); err != nil {
		t.Fatal(err)
	}

	conn, err := net.Dial("tcp", addr.String())
	if err != nil {
		t.Fatal(err)
	}
	conn.SetDeadline(time.Now().Add(time.Second))
	defer conn.Close()

	var replyLength int32
	var data []byte

	/* MESSAGE 1 */

	if _, err := conn.Write([]byte(sampleCacheQuery)); err != nil {
		t.Fatal(err)
	}

	if err := binary.Read(conn, binary.BigEndian, &replyLength); err != nil {
		t.Fatal(err)
	}

	data = make([]byte, replyLength)

	if err := binary.Read(conn, binary.BigEndian, data); err != nil {
		t.Fatal(err)
	}

	// {u'datapoints': [(1422797285, 42.17)]}
	if string(data) != "\x80\x02}(X\n\x00\x00\x00datapoints](J\xe5)\xceTG@E\x15\xc2\x8f\\(\xf6\x86eu." {
		fmt.Printf("%#v\n", string(data))
		t.Fatal("wrong reply from carbonlink")
	}

	/* MESSAGE 2 */
	if _, err := conn.Write([]byte(sampleCacheQuery2)); err != nil {
		t.Fatal(err)
	}

	if err := binary.Read(conn, binary.BigEndian, &replyLength); err != nil {
		t.Fatal(err)
	}

	data = make([]byte, replyLength)

	if err := binary.Read(conn, binary.BigEndian, data); err != nil {
		t.Fatal(err)
	}

	// {u'datapoints': [(1422797267, -42.14), (1422795966, 15.0)]}
	if string(data) != "\x80\x02}(X\n\x00\x00\x00datapoints](J\xd3)\xceTG\xc0E\x11\xeb\x85\x1e\xb8R\x86J\xbe$\xceTG@.\x00\x00\x00\x00\x00\x00\x86eu." {
		fmt.Printf("%#v\n", string(data))
		t.Fatal("wrong reply from carbonlink")
	}

	/* MESSAGE 3 */
	/* Remove carbon.agents.carbon_agent_server.param.size from cache and request again */

	for {
		c := <-cache.Out()
		if c.Metric == "carbon.agents.carbon_agent_server.param.size" {
			break
		}
	}

	if _, err := conn.Write([]byte(sampleCacheQuery2)); err != nil {
		t.Fatal(err)
	}

	if err := binary.Read(conn, binary.BigEndian, &replyLength); err != nil {
		t.Fatal(err)
	}

	data = make([]byte, replyLength)

	if err := binary.Read(conn, binary.BigEndian, data); err != nil {
		t.Fatal(err)
	}

	if string(data) != "\x80\x02}(X\n\x00\x00\x00datapoints](eu." {
		fmt.Printf("%#v\n", string(data))
		t.Fatal("wrong reply from carbonlink")
	}

	// conn.Read(b)

	/* WRONG MESSAGE TEST */
	if _, err := conn.Write([]byte("\x00\x00\x00\x05aaaaa")); err != nil {
		t.Fatal(err)
	}

	if err := binary.Read(conn, binary.BigEndian, &replyLength); err == nil || err != io.EOF {
		t.Fatal("Connection not closed on wrong request")
	}

}
