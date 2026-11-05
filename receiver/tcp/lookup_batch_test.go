package tcp

import (
	"bytes"
	"fmt"
	"net"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/go-graphite/go-carbon/points"
	"go.uber.org/zap"
)

func TestLookupBatchConfigAndDefault(t *testing.T) {
	options := NewOptions()
	if options.CacheLookupBatch {
		t.Fatal("experimental path enabled by default")
	}
	if _, err := toml.Decode("cache-lookup-batch=true\nlisten='127.0.0.1:0'", options); err != nil {
		t.Fatal(err)
	}
	r, err := newTCP("tcp", options, func(*points.Points) {})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Stop()
	if !r.cacheLookupBatch {
		t.Fatal("TOML setting not installed")
	}
}

func TestLookupBatchPreservesPlaintextOrderAndErrors(t *testing.T) {
	var wire bytes.Buffer
	for i := range 211 {
		fmt.Fprintf(&wire, "namespace.%d %d %d\n", i%23, i, i%11)
		if i%47 == 0 {
			wire.WriteString("invalid\n\n")
		}
	}
	wire.WriteString(strings.Repeat("long.", 14000) + "name 9 1\n")
	wire.WriteString("tail.metric 3 2\nunfinished 1")
	for _, compression := range []string{"", "gzip", "snappy"} {
		t.Run(compression, func(t *testing.T) {
			old := handlePlainConnection(t, compression, wire.Bytes())
			var got []*points.Points
			maxBatch := 0
			next := handlePlainConnectionConfigured(t, compression, wire.Bytes(), func(r *TCP) {
				r.cacheLookupBatch = true
				r.SetPointBatchOut(func(names [][]byte, data []points.Point) {
					maxBatch = max(maxBatch, len(data))
					for i, p := range data {
						got = append(got, points.OnePoint(string(names[i]), p.Value, p.Timestamp))
					}
				})
			})
			if !reflect.DeepEqual(old.points, got) || old.metricsReceived != next.metricsReceived || old.errors != next.errors || old.parseErrors != next.parseErrors {
				t.Fatal("batching changed contents, order or error accounting")
			}
			if maxBatch > 64 || maxBatch == 0 {
				t.Fatal("invalid batch bound")
			}
		})
	}
}

func TestLookupBatchFlushesBeforePartialRead(t *testing.T) {
	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()
	seen := make(chan string, 8)
	r := &TCP{logger: zap.NewNop(), decompressor: newDecompressor(""), cacheLookupBatch: true}
	r.SetPointBatchOut(func(names [][]byte, data []points.Point) {
		for _, name := range names {
			seen <- string(name)
		}
	})
	r.Start()
	defer r.Stop()
	done := make(chan struct{})
	go func() { r.HandleConnection(server); close(done) }()
	if err := client.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := client.Write([]byte("first 1 1\nsecond 2 2\npart")); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"first", "second"} {
		select {
		case got := <-seen:
			if got != want {
				t.Fatal("changed order")
			}
		case <-time.After(5 * time.Second):
			t.Fatal("did not flush before blocked read")
		}
	}
	client.Close()
	<-done
}
