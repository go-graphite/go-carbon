package v2daemon

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/internal/v2core"
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
	if e := p.Append(names, nil, data, seq, 100); !errors.Is(e, v2placement.ErrUnversioned) {
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

// Legacy unplaced records have no authenticated authority. Enabling placement
// must not silently assign them the current map on restart.
func TestPlacedRestartRefusesUnstampedJournal(t *testing.T) {
	original, _, _ := placedFixture(t)
	a := original.cfg.Placement
	root := t.TempDir()
	cfg := Config{Layer: 2, Producers: 1, Shards: 1}
	s, err := Open(root, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err = s.Producers[0].Append([]string{"a"}, nil, []points.Point{{Timestamp: 100, Value: 1}}, []uint64{1}, 100); err != nil {
		t.Fatal(err)
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	cfg.Placement = a
	s, err = Open(root, cfg)
	if err == nil {
		_ = s.Close()
		t.Fatal("reopened placed journal without stamp contract")
	}
}

func TestPlacedRestartPreservesAuthorityAndApplications(t *testing.T) {
	for _, layer := range []int{2} {
		t.Run(fmt.Sprintf("L%d", layer), func(t *testing.T) {
			fixture, stamp, _ := placedFixture(t)
			cfg := restartConfig(layer)
			cfg.Placement = fixture.cfg.Placement
			root := t.TempDir()
			s, err := Open(root, cfg)
			if err != nil {
				t.Fatal(err)
			}
			apps := restartApps(t, 0, 6)
			if err = s.Producers[0].AppendApplicationsPlaced(stamp, apps); err != nil {
				t.Fatal(err)
			}
			if err = s.Close(); err != nil {
				t.Fatal(err)
			}
			for attempt := 0; attempt < 2; attempt++ {
				s, err = Open(root, cfg)
				if err != nil {
					t.Fatal(err)
				}
				checkRestartOracle(t, s, apps, layer)
				if attempt == 0 {
					tail := restartApps(t, 6, 8)
					if err = s.Producers[0].AppendApplicationsPlaced(stamp, tail); err != nil {
						t.Fatal(err)
					}
					apps = append(apps, tail...)
				}
				if err = s.Close(); err != nil {
					t.Fatal(err)
				}
			}
			cfg.Placement = nil
			if unplaced, err := Open(root, cfg); err == nil {
				_ = unplaced.Close()
				t.Fatal("placed records replayed without authority")
			}
		})
	}
}

func TestPlacedRestartRejectsDifferentPersistedMap(t *testing.T) {
	s, stamp, d := placedFixture(t)
	root := filepath.Dir(s.lock.Name())
	cfg := s.cfg
	if err := s.Producers[0].AppendPlaced(stamp, []string{"a"}, []points.Point{{Timestamp: 100, Value: 1}}, []uint64{1}, 100); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	d.Domain = "other-authority"
	a, err := v2placement.OpenAuthority(t.TempDir(), d, stamp.Group, stamp.Replica, "store-a/inc-1")
	if err != nil {
		t.Fatal(err)
	}
	cfg.Placement = a
	if reopened, err := Open(root, cfg); !errors.Is(err, v2placement.ErrStaleEpoch) {
		if reopened != nil {
			_ = reopened.Close()
		}
		t.Fatalf("different map replayed: %v", err)
	}
}

func TestPlacedRestartRecoversClosedEpochWithoutReopeningIntake(t *testing.T) {
	s, stamp, _ := placedFixture(t)
	root := filepath.Dir(s.lock.Name())
	cfg := s.cfg
	if err := s.Producers[0].AppendPlaced(stamp, []string{"a"}, []points.Point{{Timestamp: 100, Value: math.Copysign(0, -1)}}, []uint64{7}, 100); err != nil {
		t.Fatal(err)
	}
	if _, err := s.FencePlacement(context.Background(), "fenced-recovery", []v2placement.EndMarker{{Lane: stamp.Lane, Final: 1, Durable: 1}}, nil); err == nil {
		t.Fatal("uncertified fence succeeded")
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(root, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	snap := reopened.Core.DBs()[0].NewSnapshot()
	got, err := v2core.Read(snap, 1)
	_ = snap.Close()
	want := []v2core.Row{{ID: 1, Time: 100, Bits: math.Float64bits(math.Copysign(0, -1)), Seq: 7}}
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("accepted prefix changed: got%+v want%+v err%v", got, want, err)
	}
	if err = reopened.Producers[0].AppendPlaced(stamp, []string{"a"}, []points.Point{{Timestamp: 101}}, []uint64{8}, 101); !errors.Is(err, v2placement.ErrStaleEpoch) {
		t.Fatalf("closed intake reopened after replay: %v", err)
	}
}

func TestPlacementEnvelopeBindsOriginalStamp(t *testing.T) {
	s, stamp, _ := placedFixture(t)
	p := s.Producers[0]
	if err := p.AppendPlaced(stamp, []string{"a"}, []points.Point{{Timestamp: 100, Value: 1}}, []uint64{1}, 100); err != nil {
		t.Fatal(err)
	}
	record := bytes.Clone(p.records[0])
	inner, gotStamp, err := decodePlacementEnvelope(record)
	if err != nil || gotStamp == nil || *gotStamp != stamp || len(inner) < 4 || !bytes.Equal(inner[:4], applicationMagic) {
		t.Fatalf("placement stamp changed: got%+v want%+v err%v", gotStamp, stamp, err)
	}
	// Both authority metadata and application bytes are covered by the checksum.
	for _, index := range []int{10, len(record) - 5} {
		changed := bytes.Clone(record)
		changed[index] ^= 1
		if _, _, err = decodePlacementEnvelope(changed); err == nil {
			t.Fatal("changed placement envelope accepted", index)
		}
	}
	for _, change := range []func(*v2placement.Stamp){
		func(s *v2placement.Stamp) { s.Group += "-wrong" },
		func(s *v2placement.Stamp) { s.Replica += "-wrong" },
		func(s *v2placement.Stamp) { s.MapDigest += "-wrong" },
		func(s *v2placement.Stamp) { s.Epoch++ },
		func(s *v2placement.Stamp) { s.Lane.Stream += "-wrong" },
	} {
		changed := stamp
		change(&changed)
		if err := p.placement.Replay(changed); err == nil {
			t.Fatal("replay accepted changed authority", changed)
		}
	}
}
