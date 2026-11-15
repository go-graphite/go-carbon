package tcp

import (
	"bytes"
	"fmt"
	"net"
	"reflect"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/points"
	"go.uber.org/zap"
)

func TestPointBatchSizeDifferential(t *testing.T) {
	var wire bytes.Buffer
	for i := range 5000 {
		fmt.Fprintf(&wire, "namespace.%d %d %d\n", i%127, i, i%37)
		if i%251 == 0 {
			wire.WriteString("invalid\n\n")
		}
	}
	wire.WriteString("unfinished 1")
	for _, compression := range []string{"", "gzip", "snappy"} {
		t.Run(compression, func(t *testing.T) {
			old := handlePlainConnection(t, compression, wire.Bytes())
			var got []*points.Points
			maxBatch := 0
			next := handlePlainConnectionConfigured(t, compression, wire.Bytes(), func(r *TCP) {
				r.cacheLookupBatch = true
				for _, invalid := range []int{-1, 0, 1025} {
					if err := r.SetPointBatchSize(invalid); err == nil {
						t.Fatal("accepted invalid batch size")
					}
				}
				if err := r.SetPointBatchSize(1024); err != nil {
					t.Fatal(err)
				}
				r.SetPointBatchOut(func(names [][]byte, data []points.Point) {
					maxBatch = max(maxBatch, len(data))
					for i, p := range data {
						got = append(got, points.OnePoint(string(names[i]), p.Value, p.Timestamp))
					}
				})
			})
			if !reflect.DeepEqual(old.points, got) || old.metricsReceived != next.metricsReceived || old.errors != next.errors || old.parseErrors != next.parseErrors {
				t.Fatal("batch size changed point values/order or errors")
			}
			if maxBatch <= 64 || maxBatch > 1024 {
				t.Fatalf("batch size=%d", maxBatch)
			}
		})
	}
}

func TestPointBatchSizeFlushesBeforeBlockedRead(t *testing.T) {
	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()
	seen := make(chan string, 2)
	r := &TCP{logger: zap.NewNop(), decompressor: newDecompressor(""), cacheLookupBatch: true}
	if err := r.SetPointBatchSize(1024); err != nil {
		t.Fatal(err)
	}
	r.SetPointBatchOut(func(names [][]byte, _ []points.Point) {
		for _, n := range names {
			seen <- string(n)
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
			t.Fatal("buffered complete points across blocked read")
		}
	}
	_ = client.Close()
	<-done
}
