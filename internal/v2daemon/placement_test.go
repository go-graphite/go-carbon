package v2daemon

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/internal/v2placement"
	"github.com/go-graphite/go-carbon/points"
)

func placedFixture(t *testing.T) (*Store, v2placement.Stamp, v2placement.Document) {
	t.Helper()
	r := v2placement.Routing{NameVersion: v2placement.NameVersion, JumpSizes: []uint32{1}, Span: 4096, Namespaces: []v2placement.Namespace{{ID: "metrics", Prefix: ""}}}
	b := []v2placement.BootstrapReplica{{ID: "site-a", JumpSize: 1, Slots: []string{"store-a/inc-1"}}}
	groups, e := v2placement.Bootstrap(r, b)
	if e != nil {
		t.Fatal(e)
	}
	lane := v2placement.Lane{Producer: "relay/inc-1", Stream: "lane-0"}
	groups[0].Replicas[0].Producers = []v2placement.Lane{lane}
	d := v2placement.Document{Schema: 1, Domain: "test", Revision: 1, Operation: "bootstrap", Routing: r, Bootstrap: b, Stores: []v2placement.Store{{ID: "store-a/inc-1", Write: "a:2003", Read: "a:8080", Copy: "a:8081"}}, Groups: groups}
	a, e := v2placement.OpenAuthority(t.TempDir(), d, groups[0].Selector.GroupID(), "site-a", "store-a/inc-1")
	if e != nil {
		t.Fatal(e)
	}
	s, e := Open(t.TempDir(), Config{Layer: 2, Producers: 1, Shards: 1, GroupInterval: time.Hour, Placement: a})
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() {
		if e := s.Close(); e != nil {
			t.Error(e)
		}
	})
	return s, v2placement.Stamp{Group: groups[0].Selector.GroupID(), Replica: "site-a", MapDigest: d.Digest(), Epoch: 1, Lane: lane}, d
}
func TestPlacedSinksAndDurableFence(t *testing.T) {
	s, stamp, d := placedFixture(t)
	p := s.Producers[0]
	names := []string{"metric.a"}
	data := []points.Point{{Timestamp: 100, Value: 1}}
	seq := []uint64{1}
	if e := p.Append(names, data, seq, 100); !errors.Is(e, v2placement.ErrUnversioned) {
		t.Fatal(e)
	}
	if e := p.AppendNumeric(nil); !errors.Is(e, v2placement.ErrUnversioned) {
		t.Fatal(e)
	}
	if e := p.AppendApplications(nil); !errors.Is(e, v2placement.ErrUnversioned) {
		t.Fatal(e)
	}
	stale := stamp
	stale.Epoch++
	if e := p.AppendPlaced(stale, names, data, seq, 100); !errors.Is(e, v2placement.ErrStaleEpoch) {
		t.Fatal(e)
	}
	if p.cursor != 0 || p.catalog.Count() != 0 {
		t.Fatal("rejection mutated catalog/journal")
	}
	if e := p.AppendPlaced(stamp, names, data, seq, 100); e != nil {
		t.Fatal(e)
	}
	if e := p.AppendApplicationsPlaced(stamp, []Application{{Name: "metric.b", Order: 2, Clock: 100, Points: data, Sequence: seq}}); e != nil {
		t.Fatal(e)
	}
	ends := []v2placement.EndMarker{{Lane: stamp.Lane, Final: 2, Durable: 2}}
	certified := false
	f, e := s.FencePlacement(context.Background(), "move-1", ends, func(frontiers [][]uint64) (v2placement.Roots, error) {
		certified = true
		if len(frontiers) != 1 || frontiers[0][0] != 2 {
			t.Errorf("fence before durable core: %v", frontiers)
		}
		h := strings.Repeat("a", 64)
		return v2placement.Roots{Parts: h, Catalog: h, Policy: h, Tombstones: h}, nil
	})
	if e != nil {
		t.Fatal(e)
	}
	if !certified {
		t.Fatal("missing coverage certification")
	}
	if e = f.Validate(d); e != nil {
		t.Fatal(e)
	}
	if p.pendingRows != 0 {
		t.Fatal("fence did not drain accepted rows")
	}
	if e = p.AppendPlaced(stamp, names, data, seq, 100); !errors.Is(e, v2placement.ErrStaleEpoch) {
		t.Fatal("fenced named sink", e)
	}
	if e = p.AppendApplicationsPlaced(stamp, []Application{{Name: "metric.b", Order: 2, Clock: 100, Points: data, Sequence: seq}}); !errors.Is(e, v2placement.ErrStaleEpoch) {
		t.Fatal("fenced application sink", e)
	}
}
func TestFenceWithoutCoverageStaysClosed(t *testing.T) {
	s, stamp, _ := placedFixture(t)
	if _, e := s.FencePlacement(context.Background(), "move-1", []v2placement.EndMarker{{Lane: stamp.Lane}}, nil); e == nil {
		t.Fatal("uncertified fence succeeded")
	}
	if e := s.Producers[0].AppendPlaced(stamp, []string{"a"}, []points.Point{{Timestamp: 1}}, []uint64{1}, 1); !errors.Is(e, v2placement.ErrStaleEpoch) {
		t.Fatal("uncertified fence reopened", e)
	}
}

// Retained logs currently omit placement stamps. Never turn a fresh placed
// producer into an unversioned replay path after restart.
func TestPlacedRestartRefusesUnstampedJournal(t *testing.T) {
	original, _, _ := placedFixture(t)
	a := original.cfg.Placement
	root := t.TempDir()
	cfg := Config{Layer: 2, Producers: 1, Shards: 1, Placement: a}
	s, err := Open(root, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = Open(root, cfg)
	if err == nil {
		_ = s.Close()
		t.Fatal("reopened placed journal without stamp contract")
	}
}
