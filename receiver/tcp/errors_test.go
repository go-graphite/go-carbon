package tcp

import (
	"errors"
	"fmt"
	"io"
	"net"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/points"
	"github.com/go-graphite/go-carbon/receiver/parse"
	"go.uber.org/zap"
)

func TestTCPReadErrorStats(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
		want float64
	}{
		{name: "eof", err: io.EOF},
		{name: "read_failure", err: errors.New("connection reset by peer"), want: 1},
		{name: "wrapped_eof", err: fmt.Errorf("read failed: %w", io.EOF), want: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server, client := net.Pipe()
			defer client.Close()
			rcv := &TCP{logger: zap.NewNop(), decompressor: newDecompressor("")}
			rcv.Start()
			defer rcv.Stop()
			rcv.HandleConnection(&failingReadConn{Conn: server, err: tc.err})
			rcv.Stat(func(name string, value float64) {
				want := float64(0)
				if name == "errors" {
					want = tc.want
				}
				if value != want {
					t.Errorf("stat %s = %v, want %v", name, value, want)
				}
			})
		})
	}
}

func TestFramedErrorStats(t *testing.T) {
	for _, tc := range []struct {
		name        string
		payload     string
		parser      func([]byte) ([]*points.Points, error)
		errors      float64
		parseErrors float64
	}{
		{
			name:        "pickle_parse",
			payload:     "\x00\x00\x00\x01\xff",
			parser:      parse.Pickle,
			parseErrors: 1,
		},
		{name: "protobuf_parse", payload: "\x00\x00\x00\x01\xff", parser: parse.Protobuf, parseErrors: 1},
		{name: "frame_size", payload: "\x00\x00\x01\x00", parser: parse.Protobuf, errors: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server, client := net.Pipe()
			defer client.Close()
			rcv := &TCP{logger: zap.NewNop(), maxMessageSize: 64, frameParser: tc.parser}
			rcv.Start()
			defer rcv.Stop()
			done := make(chan struct{})
			go func() {
				rcv.handleFraming(server)
				close(done)
			}()
			if err := client.SetWriteDeadline(time.Now().Add(2 * time.Second)); err != nil {
				t.Fatal(err)
			}
			if _, err := io.WriteString(client, tc.payload); err != nil {
				t.Fatal(err)
			}
			select {
			case <-done:
			case <-time.After(2 * time.Second):
				t.Fatal("handleFraming did not finish")
			}
			stats := make(map[string]float64)
			rcv.Stat(func(name string, value float64) { stats[name] = value })
			if stats["errors"] != tc.errors || stats["parseErrors"] != tc.parseErrors {
				t.Fatalf("stats = %v, want errors:%v parseErrors:%v", stats, tc.errors, tc.parseErrors)
			}
		})
	}
}

type failingReadConn struct {
	net.Conn
	err error
}

func (c *failingReadConn) Read([]byte) (int, error) { return 0, c.err }
