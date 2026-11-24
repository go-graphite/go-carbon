package v2core

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/go-graphite/go-carbon/internal/sealrollup"
	"github.com/go-graphite/go-carbon/internal/v2part"
)

func TestSealOrderSummaryCertificate(t *testing.T) {
	for _, tc := range []struct {
		name                                   string
		version                                uint8
		reset, seq                             uint64
		history, duplicate, upto, materialized bool
		want                                   bool
	}{
		{name: "v1", version: 1, seq: 1, want: true},
		{name: "v2", version: 2, seq: 1, want: true},
		{name: "v3", version: 3, seq: 1, want: true},
		{name: "v4", version: 4, reset: 1, seq: 2, want: true},
		{name: "legacy-reset", version: 3, reset: 1, seq: 2},
		{name: "unnumbered", version: 3},
		{name: "history", version: 4, seq: 1, history: true},
		{name: "duplicate-boundary", version: 4, seq: 1, duplicate: true},
		{name: "upto", version: 4, seq: 1, upto: true},
		{name: "materialized", version: 4, seq: 1, materialized: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			config := sealrollup.Config{Method: sealrollup.Last, Archives: []sealrollup.Archive{{Step: 1, Count: 20000}}}
			if tc.history {
				config.Archives = append(config.Archives, sealrollup.Archive{Step: 10, Count: 20000})
			}
			digest, err := sealrollup.PolicyDigest(config)
			if err != nil {
				t.Fatal(err)
			}
			definition, _ := json.Marshal(config)
			name := v2part.ID("seal.summary.metric")
			policy, err := EncodeRollupPolicy(RollupPolicy{Name: name, Group: "g", Config: config})
			if err != nil {
				t.Fatal(err)
			}
			key := v2part.Key{Group: "g", VShard: v2part.Route(name), Part: v2part.PartID{1}, Metric: name}
			spec := v2part.Spec{Scope: v2part.Scope{Group: "g", VShard: v2part.GroupVShard, Part: key.Part}, Source: v2part.Provenance{Stream: "seal", Incarnation: v2part.PartID{9}, MinOrder: 1, MaxOrder: 10000, Seq: tc.seq}}
			for a := range config.Archives {
				spec.Policies = append(spec.Policies, v2part.ArchivePolicy{Archive: uint32(a), Digest: sha256.Sum256(definition), Definition: definition})
			}
			var file []coreKV
			var tuples []plannedKV
			for i := 0; i < v2part.MaxDenseSlots+2; i++ {
				r := &sealrollup.Record{Delta: sealrollup.Delta{Slot: int64(i), Order: uint64(i + 2), Sample: sealrollup.Sample{Time: int64(i + 1000), Bits: uint64(i), Sequence: uint64(i + 2)}}}
				if tc.duplicate && i == v2part.MaxDenseSlots {
					r.Slot--
					r.Order = uint64(i + 3)
				}
				if tc.upto {
					r.Kind = sealrollup.KindMat
					r.Upto = 1
				}
				if tc.materialized {
					r.Kind = sealrollup.KindMat
				}
				key.BlockStart = r.Slot
				key.Ordinal = 0

				k, err := v2part.Encode(key)
				if err != nil {
					t.Fatal(err)
				}
				file = append(file, coreKV{Key: k, Record: r, Name: name})
				tuples = append(tuples, plannedKV{name: name, vshard: key.VShard, record: r})
			}
			key.Archive = v2part.MetadataArchive
			key.BlockStart = 0
			key.Ordinal = 0
			k, _ := v2part.Encode(key)
			envelope := AppendRollupMetadata(nil, RollupMetadata{Version: tc.version, ID: 1, Policy: policy, ResetOrder: tc.reset, Frontier: sealrollup.Frontier{Policy: digest, Order: 10000, MaxTime: 10000}})
			file = append(file, coreKV{Key: k, Value: envelope, Name: name})
			tuples = append(tuples, plannedKV{name: name, vshard: key.VShard, value: envelope})
			if tc.duplicate {
				orders, n := sealOrderEncoding(len(tuples), func(i int) (*sealrollup.Record, []byte) { return tuples[i].record, tuples[i].value }, true)
				if orders != v2part.ExactOrders || n != len(tuples) {
					t.Fatalf("certificate: duplicate name orders=%v n=%d", orders, n)
				}
				return
			}
			dir := t.TempDir()
			a, b := filepath.Join(dir, "reference.sst"), filepath.Join(dir, "tuple.sst")
			da, err := writePlannedRollupPart(a, file, spec)
			if err != nil {
				t.Fatal(err)
			}
			db, err := writePlannedRollupTuples(b, plannedRollupPart{Spec: spec, Segments: [][]plannedKV{tuples}})
			if err != nil {
				t.Fatal(err)
			}
			ba, _ := os.ReadFile(a)
			bb, _ := os.ReadFile(b)
			if !bytes.Equal(ba, bb) || !reflect.DeepEqual(da, db) {
				t.Fatal("handoff changed bytes/descriptor")
			}
			runs, summaries := 0, 0
			if err = v2part.Read(context.Background(), b, &db, func(k v2part.Key, data []byte) error {
				if k.Archive == v2part.MetadataArchive {
					return nil
				}
				runs++
				if v2part.HasOrderSummary(data) {
					summaries++
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			if runs < 2 || (summaries == runs) != tc.want || !tc.want && summaries != 0 {
				t.Fatalf("certificate: summaries=%d runs=%d want=%v", summaries, runs, tc.want)
			}
			if err = VerifyMovePart(context.Background(), b, db); err != nil {
				t.Fatal(err)
			}
			t.Logf("%s runs=%d summaries=%d", tc.name, runs, summaries)
		})
	}
}

func TestSealOrderSummaryFirstSealRestart(t *testing.T) {
	t.Setenv("CORE_MERGE_WORKERS", "0")
	root := t.TempDir()
	e := mergeFixtureIdle(t, root, nil)
	defer func() {
		if e != nil {
			_ = e.close()
		}
	}()
	policy, raw := rollupTestPolicy(t, "seal.summary.first", "1s:1h", sealrollup.Last, 0)
	points := []row{{ID: 7, Time: 3601, Bits: 1 << 63, Seq: 1}, {ID: 7, Time: 3602, Bits: 0x7ff8000000000042, Seq: 2}}
	if err := e.submitBatches(0, []coreBatch{{ID: 7, Policy: raw, Order: 1, Clock: 3603, Points: points}}, 1, true); err != nil {
		t.Fatal(err)
	}
	parts := seqLiveParts(t, e.owners[0])
	if len(parts) != 1 {
		t.Fatalf("first seal parts %d", len(parts))
	}
	part := parts[0]
	path := filepath.Join(e.owners[0].dir, "parts", fmt.Sprintf("%x.sst", part.SHA256))
	summaries := 0
	if err := v2part.Read(context.Background(), path, &part, func(k v2part.Key, data []byte) error {
		if v2part.HasOrderSummary(data) {
			summaries++
		}
		return nil
	}); err != nil || summaries == 0 {
		t.Fatalf("first seal summaries=%d err=%v", summaries, err)
	}
	check := func(engine *coreEngine) {
		t.Helper()
		snap := engine.owners[0].db.NewSnapshot()
		defer snap.Close()
		index, err := NewRollupSnapshot(context.Background(), snap, nil, nil, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		records, err := index.Records(context.Background(), string(policy.Name))
		if err != nil {
			t.Fatal(err)
		}
		if len(records) != 2 || records[0].Sample.Bits != points[0].Bits || records[1].Sample.Bits != points[1].Bits {
			t.Fatalf("records %+v", records)
		}
	}
	check(e)
	prepared, err := PrepareMove(context.Background(), t.TempDir(), []MovePart{{Path: path, Descriptor: part}})
	if err != nil {
		t.Fatal(err)
	}
	defer prepared.Close()
	receiver := mergeFixtureIdle(t, t.TempDir(), nil)
	defer receiver.close()
	if err = receiver.AdoptMove(context.Background(), prepared, map[string]uint64{string(policy.Name): 7}, 3603); err != nil {
		t.Fatal(err)
	}
	check(receiver)
	if err = e.close(); err != nil {
		t.Fatal(err)
	}
	e = mergeFixtureIdle(t, root, nil)
	check(e)
}
