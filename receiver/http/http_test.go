package http

import (
	"bytes"
	"fmt"
	"net"
	"net/http"
	"testing"

	"github.com/lomik/go-carbon/points"
	"github.com/lomik/go-carbon/receiver"
	"github.com/stretchr/testify/assert"
)

func TestHttp(t *testing.T) {
	addr, err := net.ResolveTCPAddr("tcp", "localhost:0")
	if err != nil {
		t.Fatal(err)
	}

	received := make([]*points.Points, 0)

	r, err := receiver.New("http", map[string]interface{}{
		"protocol": "http",
		"listen":   addr.String(),
	},
		func(p *points.Points) {
			received = append(received, p)
		},
	)

	if err != nil {
		t.Fatal(err)
	}

	defer r.Stop()

	table := []struct {
		Body        string
		ContentType string
		Expected    []*points.Points
		Error       bool
	}{
		{
			Body: "hello.world 42.15 1422698155\nmetric.name -72.11 1422698155\n",
			Expected: []*points.Points{
				points.OnePoint("hello.world", 42.15, 1422698155),
				points.OnePoint("metric.name", -72.11, 1422698155),
			},
		},
		{
			Body:  "hello.world 42.15 1422698155\nmetric.nam",
			Error: true,
		},
		{
			Body:        "hello.world 42.15 1422698155\nmetric.name -72.11 1422698155\n",
			ContentType: "application/python-pickle",
			Error:       true,
		},
		{
			Body:        "(lp0\n(S'param1'\np1\n(I1423931224\nF60.2\ntp2\n(I1423931284\nI42\ntp3\ntp4\na(S'param2'\np5\n(I1423931224\nI-15\ntp6\ntp7\na.",
			ContentType: "application/python-pickle",
			Expected: []*points.Points{
				points.OnePoint("param1", 60.2, 1423931224).Add(42, 1423931284),
				points.OnePoint("param2", -15, 1423931224),
			},
		},
		{
			Body:        "\n*\n\x06param1\x12\x0f\x08\xd8\xee\xfd\xa6\x05\x11\x9a\x99\x99\x99\x99\x19N@\x12\x0f\x08\x94\xef\xfd\xa6\x05\x11\x00\x00\x00\x00\x00\x00E@\n\x19\n\x06param2\x12\x0f\x08\xd8\xee\xfd\xa6\x05\x11\x00\x00\x00\x00\x00\x00.\xc0",
			ContentType: "application/protobuf",
			Expected: []*points.Points{
				points.OnePoint("param1", 60.2, 1423931224).Add(42, 1423931284),
				points.OnePoint("param2", -15, 1423931224),
			},
		},
	}

	url := fmt.Sprintf("http://%s/", r.(*HTTP).Addr())

	for index, tc := range table {
		received = make([]*points.Points, 0)
		resp, err := http.Post(url, tc.ContentType, bytes.NewReader([]byte(tc.Body)))
		if !tc.Error {
			assert.Nil(t, err, fmt.Sprintf("test #%d", index))
			assert.Equal(t, 200, resp.StatusCode, fmt.Sprintf("test #%d", index))
			assert.Equal(t, tc.Expected, received, fmt.Sprintf("test #%d", index))
		} else {
			assert.NotEqual(t, 200, resp.StatusCode, fmt.Sprintf("test #%d", index))
			assert.Equal(t, 0, len(received), fmt.Sprintf("test #%d", index))
		}
	}
}
