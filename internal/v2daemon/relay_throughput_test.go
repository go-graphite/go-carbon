package v2daemon

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/internal/v2recv"
)

// TestLegacyThroughputEndpoint is an opt-in loopback-only measurement fixture.
// A separate control socket waits for the raw receiver's final durable drain;
// it does not add acknowledgements to the legacy data protocol.
func TestLegacyThroughputEndpoint(t *testing.T) {
	root := os.Getenv("V2_LEGACY_BENCH_ROOT")
	if root == "" {
		t.Skip("requires measurement driver")
	}
	s, e := Open(filepath.Join(root, "data"), Config{Layer: 2, Producers: 1, Shards: 1, GroupInterval: time.Hour})
	if e != nil {
		t.Fatal(e)
	}
	l, e := net.Listen("tcp", "127.0.0.1:0")
	if e != nil {
		t.Fatal(e)
	}
	server, e := s.ReceiveTCP(l, v2recv.Config{BatchSize: 256})
	if e != nil {
		t.Fatal(e)
	}
	control, e := net.Listen("tcp", "127.0.0.1:0")
	if e != nil {
		t.Fatal(e)
	}
	fmt.Println("LEGACY_READY " + l.Addr().String() + " " + control.Addr().String())
	c, e := control.Accept()
	if e != nil {
		t.Fatal(e)
	}
	var wanted [8]byte
	if _, e = io.ReadFull(c, wanted[:]); e != nil {
		t.Fatal(e)
	}
	target := binary.BigEndian.Uint64(wanted[:])
	deadline := time.Now().Add(time.Minute)
	for server.Stats().Accepted < target && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if server.Stats().Accepted != target {
		t.Fatal("legacy receive timeout", server.Stats())
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	if e = server.Wait(ctx); e != nil {
		t.Fatal(e)
	}
	var result [8]byte
	binary.BigEndian.PutUint64(result[:], s.Producers[0].SyncGroups)
	if _, e = c.Write(result[:]); e != nil {
		t.Fatal(e)
	}
	c.Close()
	if e = s.Close(); e != nil {
		t.Fatal(e)
	}
}
