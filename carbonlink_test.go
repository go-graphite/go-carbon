package carbon

import (
	"bytes"
	"testing"
)

func TestCarbonlinkRead(t *testing.T) {

	reader := bytes.NewReader([]byte("\x00\x00\x00Y\x80\x02}q\x01(U\x06metricq\x02U,carbon.agents.carbon_agent_server.cache.sizeq\x03U\x04typeq\x04U\x0bcache-queryq\x05u."))

	req, err := ReadRequest(reader)

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
