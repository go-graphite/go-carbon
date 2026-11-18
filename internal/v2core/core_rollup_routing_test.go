package v2core

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"testing"

	"github.com/cockroachdb/pebble"
	"github.com/go-graphite/go-carbon/internal/partsformat"
	"github.com/go-graphite/go-carbon/internal/sealrollup"
)

func TestCoreRollupPortableOwner(t *testing.T) {
	// Different numeric IDs/producers for names in the same portable vshard
	// must not multiply that shard's SSTs across physical owners.
	prior := map[uint16]string{}
	var names []string
	for i := 0; len(names) == 0; i++ {
		name := fmt.Sprintf("namespace.metric.%d", i)
		shard := partsformat.Route(partsformat.ID(name))
		if old, ok := prior[shard]; ok {
			names = []string{old, name}
		} else {
			prior[shard] = name
		}
	}
	cfg := coreTestConfig()
	cfg.Rollups = true
	cfg.KeepBaseRows = true
	cfg.Shards = 3
	cfg.Producers = 2
	root := t.TempDir()
	policies := make([][]byte, 2)
	for i, name := range names {
		_, policies[i] = rollupTestPolicy(t, name, "1s:1d,1m:30d", sealrollup.Average, .5)
	}
	for epoch := 0; epoch < 2; epoch++ {
		e, err := Open(root, cfg, Hooks{})
		if err != nil {
			t.Fatal(err)
		}
		for i := range names {
			id := uint64(i + 1)
			b := Batch{ID: id, Policy: policies[i], Order: uint64(epoch + 1), Clock: 1000 + int64(epoch), Points: []Row{{ID: id, Time: 999 + int64(epoch), Bits: math.Float64bits(float64(i)), Seq: uint64(epoch + 1)}}}
			if epoch == 1 {
				b.Registered, err = e.RegisterApplicationPolicy(id, names[i], policies[i])
				if err != nil {
					t.Fatal(err)
				}
				b.Policy = nil
			}
			if err = e.AppendBatches(i, []Batch{b}, uint64(epoch+1), true); err != nil {
				t.Fatal(err)
			}
		}
		seenRows, seenMetrics := 0, 0
		for owner, db := range e.DBs() {
			snap := db.NewSnapshot()
			err = ScanRaw(snap, func(r Row) error {
				seenRows++
				want := int(partsformat.Route(partsformat.ID(names[r.ID-1]))) % cfg.Shards
				if owner != want {
					return fmt.Errorf("id%d owner%d want%d", r.ID, owner, want)
				}
				return nil
			})
			if err == nil {
				err = ScanRollups(snap, func(p RollupPolicy, m RollupMetadata, _ []sealrollup.Record) error {
					seenMetrics++
					if m.Frontier.Order != uint64(epoch+1) {
						return fmt.Errorf("lost frontier: %+v", m)
					}
					if owner != int(partsformat.Route(p.Name))%cfg.Shards {
						return fmt.Errorf("archive in wrong owner")
					}
					return nil
				})
			}
			_ = snap.Close()
			if err != nil {
				t.Fatal(err)
			}
		}
		if seenRows != (epoch+1)*2 || seenMetrics != 2 {
			t.Fatalf("missing rows/metrics: %d/%d", seenRows, seenMetrics)
		}
		if err = e.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestCoreRollupKeyTemplatesRemainCanonical(t *testing.T) {
	for _, name := range []string{"namespace.metric;a=1;b=2", "namespace.\x00binary", "日本.metric"} {
		p, _ := rollupTestPolicy(t, name, "1s:1d,1m:30d", sealrollup.Average, .5)
		p.Group = "group\x00exact"
		policy, err := EncodeRollupPolicy(p)
		if err != nil {
			t.Fatal(err)
		}
		r := newCoreRollups()
		for order := uint64(1); order <= 2; order++ {
			plan, err := r.plan(coreSealInput{Batches: []coreBatch{{ID: 7, Policy: policy, Order: order, Clock: 1000, Points: []row{{ID: 7, Time: 997 + int64(order), Bits: 1 << 63, Seq: order}}}}})
			if err != nil {
				t.Fatal(err)
			}
			count := 0
			for _, file := range plan.Files {
				for _, kv := range file {
					k, err := partsformat.Decode(kv.Key)
					if err != nil {
						t.Fatal(err)
					}
					expected, err := partsformat.Encode(k)
					if err != nil || !bytes.Equal(expected, kv.Key) || k.Group != p.Group || k.Metric != p.Name || k.Ordinal != 0 {
						t.Fatalf("noncanonical key: %+v %v", k, err)
					}
					count++
				}
			}
			if count < 2 {
				t.Fatal("missing metadata/archive", count)
			}
			plan.Commit()
		}
	}
}

func TestCoreRollupRoutingVersionRejection(t *testing.T) {
	cfg := coreTestConfig()
	cfg.Shards = 1
	cfg.Producers = 1
	cfg.Rollups = true
	root := t.TempDir()
	_, policy := rollupTestPolicy(t, "namespace.metric", "1s:1d", sealrollup.Average, .5)
	e, err := Open(root, cfg, Hooks{})
	if err != nil {
		t.Fatal(err)
	}
	if err = e.AppendBatches(0, []Batch{{ID: 7, Policy: policy, Order: 1, Clock: 1000, Points: []Row{{ID: 7, Time: 999, Seq: 1}}}}, 1, true); err != nil {
		t.Fatal(err)
	}
	// Reconstruct the old marker-less owner configuration on disk. Opening it
	// under canonical vshard routing must refuse the incompatible ownership.
	// Rewrite it after Close: a rollup owner's closing maintenance seal
	// persists its in-memory frontier.
	f := e.owners[0].meta
	f.Config.RollupRoutingVersion = 0
	data, err := json.Marshal(f)
	if err != nil {
		t.Fatal(err)
	}
	if err = e.Close(); err != nil {
		t.Fatal(err)
	}
	db, err := pebble.Open(filepath.Join(root, "owner-000"), &pebble.Options{})
	if err != nil {
		t.Fatal(err)
	}
	if err = db.Set(coreMetaKey, data, pebble.Sync); err != nil {
		t.Fatal(err)
	}
	if err = db.Close(); err != nil {
		t.Fatal(err)
	}
	e, err = Open(root, cfg, Hooks{})
	if err == nil {
		_ = e.Close()
		t.Fatal("accepted old numeric rollup routing")
	}
}

func TestCoreRollupReadOnlyOracle(t *testing.T) {
	cfg := coreTestConfig()
	cfg.Shards = 1
	cfg.Producers = 1
	cfg.Rollups = true
	cfg.KeepBaseRows = true
	root := t.TempDir()
	_, policy := rollupTestPolicy(t, "namespace.metric", "1s:1d", sealrollup.Average, .5)
	e, err := Open(root, cfg, Hooks{})
	if err != nil {
		t.Fatal(err)
	}
	row := Row{ID: 7, Time: 999, Bits: 1 << 63, Seq: 1}
	batch := Batch{ID: 7, Policy: policy, Order: 1, Clock: 1000, Points: []Row{row}}
	if err = e.AppendBatches(0, []Batch{batch}, 1, true); err != nil {
		t.Fatal(err)
	}
	if err = e.Close(); err != nil {
		t.Fatal(err)
	}
	e, err = OpenReadOnly(root, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if e.owners[0].plan != nil {
		t.Fatal("read-only oracle reconstructed writable reducer")
	}
	snap := e.DBs()[0].NewSnapshot()
	rows, err := Read(snap, 7)
	_ = snap.Close()
	if err != nil || len(rows) != 1 || rows[0] != row {
		t.Fatalf("raw bits changed: %+v %v", rows, err)
	}
	if err = e.AppendBatches(0, []Batch{batch}, 2, true); !errors.Is(err, os.ErrPermission) {
		t.Fatalf("read-only admission: %v", err)
	}
	if err = e.DBs()[0].Set([]byte("write"), nil, pebble.Sync); err == nil {
		t.Fatal("read-only database write succeeded")
	}
	if err = e.Close(); err != nil {
		t.Fatal(err)
	}
	// A later writable reopen still reconstructs canonical state normally.
	e, err = Open(root, cfg, Hooks{})
	if err != nil {
		t.Fatal(err)
	}
	if e.owners[0].plan == nil {
		t.Fatal("lost writable reducer")
	}
	if err = e.Close(); err != nil {
		t.Fatal(err)
	}
}
