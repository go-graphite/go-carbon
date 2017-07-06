package http

import (
	"bytes"
	"fmt"
	"net"
	"net/http"
	"testing"

	"github.com/lomik/go-carbon/points"
	"github.com/lomik/go-carbon/receiver"
)

func TestHttp(t *testing.T) {
	addr, err := net.ResolveTCPAddr("tcp", "localhost:0")
	if err != nil {
		t.Fatal(err)
	}

	// test.rcvChan = make(chan *points.Points, 128)

	r, err := receiver.New("http", map[string]interface{}{
		"protocol": "http",
		"listen":   addr.String(),
	},
		func(p *points.Points) {
			// test.rcvChan <- p
		},
	)

	if err != nil {
		t.Fatal(err)
	}

	defer r.Stop()

	table := []struct {
		Body        string
		ContentType string
		Output      []*points.Points
		Error       bool
	}{
		{
			Body: "hello.world 42.15 1422698155\nmetric.name -72.11 1422698155\n",
		},
	}

	url := fmt.Sprintf("http://%s/", r.(*HTTP).Addr())

	for _, tc := range table {
		resp, err := http.Post(url, tc.ContentType, bytes.NewReader([]byte(tc.Body)))
		fmt.Println(err, resp)
	}

	// fmt.Println("epta")
}
