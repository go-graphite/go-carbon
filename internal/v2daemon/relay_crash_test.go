package v2daemon

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/internal/v2core"
	"github.com/go-graphite/go-carbon/internal/v2placement"
	"github.com/go-graphite/go-carbon/internal/v2recv"
	"github.com/go-graphite/go-carbon/internal/v2wire"
)

func relayCrashConfig(t *testing.T, root string) (Config, v2placement.Document) {
	t.Helper()
	routing := v2placement.Routing{NameVersion: v2placement.NameVersion, JumpSizes: []uint32{1}, Span: 4096, Namespaces: []v2placement.Namespace{{ID: "metrics", Prefix: ""}}}
	bootstrap := []v2placement.BootstrapReplica{{ID: "site-a", JumpSize: 1, Slots: []string{"store-a/inc-1"}}}
	groups, e := v2placement.Bootstrap(routing, bootstrap)
	if e != nil {
		t.Fatal(e)
	}
	groups[0].Replicas[0].Producers = []v2placement.Lane{{Producer: "relay-a/inc-1", Stream: "lane-0"}}
	doc := v2placement.Document{Schema: 1, Domain: "relay-crash-test", Revision: 1, Operation: "bootstrap", Routing: routing, Bootstrap: bootstrap, Stores: []v2placement.Store{{ID: "store-a/inc-1", Write: "a:2003", Read: "a:8080", Copy: "a:8081"}}, Groups: groups}
	a, e := v2placement.OpenAuthority(filepath.Join(root, "authority"), doc, groups[0].Selector.GroupID(), "site-a", "store-a/inc-1")
	if e != nil {
		t.Fatal(e)
	}
	return Config{Layer: 2, Producers: 1, Shards: 1, Placement: a, GroupInterval: time.Hour}, doc
}
func TestRelayACKSurvivesStoreSIGKILL(t *testing.T) {
	key := bytes.Repeat([]byte{0x4b}, 32)
	if root := os.Getenv("V2_STORE_ACK_CRASH_ROOT"); root != "" {
		cfg, _ := relayCrashConfig(t, root)
		s, e := Open(filepath.Join(root, "data"), cfg)
		if e != nil {
			t.Fatal(e)
		}
		if expected := os.Getenv("V2_STORE_EXPECT_CURSOR"); expected != "" && fmt.Sprint(s.Producers[0].cursor) != expected {
			t.Fatalf("durable cursor %d, expected %s", s.Producers[0].cursor, expected)
		}

		l, e := net.Listen("tcp", "127.0.0.1:0")
		if e != nil {
			t.Fatal(e)
		}
		if _, e = s.ReceivePlacedTCP(l, v2recv.Config{PlacementKey: key, PlacementProducer: "relay-a/inc-1"}, nil); e != nil {
			t.Fatal(e)
		}
		if s.Producers[0].cursor > 0 {
			checkRelayCrashRow(t, s)
		}
		fmt.Println("ACK_READY " + l.Addr().String())
		select {}
	}
	root := t.TempDir()
	cmd := exec.Command(os.Args[0], "-test.run=^TestRelayACKSurvivesStoreSIGKILL$")
	cmd.Env = append(os.Environ(), "V2_STORE_ACK_CRASH_ROOT="+root)
	stdout, e := cmd.StdoutPipe()
	if e != nil {
		t.Fatal(e)
	}
	cmd.Stderr = os.Stderr
	if e = cmd.Start(); e != nil {
		t.Fatal(e)
	}
	defer func() { _ = cmd.Process.Kill(); _ = cmd.Wait() }()
	ready := make(chan string, 1)
	go func() {
		scan := bufio.NewScanner(stdout)
		for scan.Scan() {
			if strings.HasPrefix(scan.Text(), "ACK_READY ") {
				ready <- strings.TrimPrefix(scan.Text(), "ACK_READY ")
				return
			}
		}
		ready <- ""
	}()
	var addr string
	select {
	case addr = <-ready:
		if addr == "" {
			t.Fatal("store failed to start")
		}
	case <-time.After(15 * time.Second):
		t.Fatal("store startup timeout")
	}
	// Reopen of authority is read-only here; the child owns the data root.
	cfg, doc := relayCrashConfig(t, root)
	sel, v, e := doc.Routing.Select("example.metric")
	if e != nil {
		t.Fatal(e)
	}
	f := v2wire.Frame{Kind: v2wire.Data, Stream: v2wire.Stream{Group: sel.GroupID(), Replica: "site-a", Store: "store-a/inc-1", MapDigest: doc.Digest(), Revision: 1, Epoch: 1, Producer: "relay-a/inc-1", Lane: "lane-0"}, Sequence: 1, VShard: v, Name: "example.metric", Bits: 0x8000000000000000, Timestamp: 100}
	send := func(addr string) {
		t.Helper()
		c, e := net.Dial("tcp", addr)
		if e != nil {
			t.Fatal(e)
		}
		defer c.Close()
		_ = c.SetDeadline(time.Now().Add(10 * time.Second))
		if e = v2wire.Write(c, f, key); e != nil {
			t.Fatal(e)
		}
		ack, e := v2wire.Read(c, key)
		if e != nil || ack != f.Acknowledgement() {
			t.Fatal("missing durable ACK", ack, e)
		}
	}
	send(addr)
	if e = cmd.Process.Kill(); e != nil {
		t.Fatal(e)
	}
	_ = cmd.Wait()
	s, e := Open(filepath.Join(root, "data"), cfg)
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	if s.Producers[0].cursor != 1 {
		t.Fatal("ACKed row lost after kill")
	}
	checkRelayCrashRow(t, s)
	l, e := net.Listen("tcp", "127.0.0.1:0")
	if e != nil {
		t.Fatal(e)
	}
	server, e := s.ReceivePlacedTCP(l, v2recv.Config{PlacementKey: key, PlacementProducer: "relay-a/inc-1"}, nil)
	if e != nil {
		t.Fatal(e)
	}
	send(l.Addr().String())
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if e = server.Wait(ctx); e != nil {
		t.Fatal(e)
	}
	if s.Producers[0].cursor != 1 {
		t.Fatal("ACKed retry duplicated after kill")
	}
}

func checkRelayCrashRow(t *testing.T, s *Store) {
	t.Helper()
	snap := s.Core.DBs()[0].NewSnapshot()
	rows, e := v2core.Read(snap, 1)
	snap.Close()
	if e != nil || uint64(len(rows)) != s.Producers[0].cursor {
		t.Fatalf("ACKed core value/order lost: %+v %v", rows, e)
	}
	for i, row := range rows {
		if row.Time != 100 || row.Bits != 0x8000000000000000 || row.Seq != uint64(i+1) {
			t.Fatalf("ACKed core value/order lost: %+v", rows)
		}
	}
}
