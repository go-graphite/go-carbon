package v2core

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/cockroachdb/pebble"

	"github.com/go-graphite/go-carbon/internal/sealrollup"
	"github.com/go-graphite/go-carbon/internal/v2part"
)

// Exercise real merged bytes, whole-name boundaries, the portable validator,
// and the native reader independently of writer certification.
func TestMergeOrderSummaryCertificate(t *testing.T) {
	for _, tc := range []struct {
		name                                       string
		version                                    uint8
		reset, seq                                 uint64
		history, prefix, copied, duplicates, force bool
		want                                       bool
	}{
		{name: "reject-history", version: 4, seq: 1, history: true, force: true},
		{name: "reject-legacy-reset", version: 3, seq: 1, reset: 1, force: true},
		{name: "v1", version: 1, seq: 1, prefix: true, want: true},
		{name: "v2", version: 2, seq: 1, prefix: true, want: true},
		{name: "v3", version: 3, seq: 1, prefix: true, want: true},
		{name: "v4", version: 4, reset: 1, seq: 2, prefix: true, want: true},
		{name: "legacy-reset", version: 3, reset: 1, seq: 2, prefix: true},
		{name: "legacy-seq", version: 3, prefix: true},
		{name: "non-prefix", version: 4, seq: 1},
		{name: "copied", version: 4, seq: 1, prefix: true, copied: true},
		{name: "history", version: 4, seq: 1, prefix: true, history: true},
		{name: "duplicate-boundary", version: 4, seq: 1, prefix: true, duplicates: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			if err := os.Mkdir(filepath.Join(dir, "parts"), 0700); err != nil {
				t.Fatal(err)
			}
			config := sealrollup.Config{Method: sealrollup.Last, Archives: []sealrollup.Archive{{Step: 1, Count: 20000}}}
			if tc.history {
				config.Archives = append(config.Archives, sealrollup.Archive{Step: 10, Count: 20000})
			}
			digest, err := sealrollup.PolicyDigest(config)
			if err != nil {
				t.Fatal(err)
			}
			name := v2part.ID("summary.metric")
			policy, err := EncodeRollupPolicy(RollupPolicy{Name: name, Group: "g", Config: config})
			if err != nil {
				t.Fatal(err)
			}
			key := v2part.Key{Group: "g", VShard: v2part.Route(name), Part: v2part.PartID{1}, Metric: name}
			spec := v2part.Spec{Scope: v2part.Scope{Group: "g", VShard: v2part.GroupVShard, Part: key.Part}, Source: v2part.Provenance{Stream: "summary", Incarnation: v2part.PartID{9}, MinOrder: 1, MaxOrder: 10000, Seq: tc.seq}}
			definition, _ := json.Marshal(config)
			for a := range config.Archives {
				spec.Policies = append(spec.Policies, v2part.ArchivePolicy{Archive: uint32(a), Digest: sha256.Sum256(definition), Definition: definition})
			}
			path := filepath.Join(dir, "input.sst")
			w, err := v2part.NewDenseWriter(path, spec)
			if err != nil {
				t.Fatal(err)
			}
			defer w.Abort()
			if tc.force {
				if err = w.SetOrderEncoding(v2part.SummaryOrders); err != nil {
					t.Fatal(err)
				}
			}
			var records []sealrollup.Record
			for i := 0; i < v2part.MaxDenseSlots+2; i++ {
				r := sealrollup.Record{Delta: sealrollup.Delta{Slot: int64(i), Order: uint64(i + 2), Sample: sealrollup.Sample{Time: int64(i + 1000), Bits: math.Float64bits(float64(i))}}}
				if tc.duplicates && i == v2part.MaxDenseSlots {
					r.Slot--
				}
				records = append(records, r)
			}
			k0, _ := v2part.Encode(key)
			if err = w.AppendVersions(k0, records); err != nil {
				t.Fatal(err)
			}
			md := RollupMetadata{Version: tc.version, ID: 1, Policy: policy, ResetOrder: tc.reset, Frontier: sealrollup.Frontier{Order: 10000, MaxTime: 10000, Policy: digest}}
			key.Archive = v2part.MetadataArchive
			key.BlockStart = 0
			k, _ := v2part.Encode(key)
			if err = w.SetEncoded(k, AppendRollupMetadata(nil, md)); err != nil {
				t.Fatal(err)
			}
			d, err := w.Close()
			if err != nil {
				t.Fatal(err)
			}
			if tc.force {
				if err = VerifyMovePart(context.Background(), path, d); err == nil || !strings.Contains(err.Error(), "order summary") {
					t.Fatalf("portable accepted invalid summary: %v", err)
				}
				db, err := pebble.Open(filepath.Join(dir, "db"), &pebble.Options{})
				if err != nil {
					t.Fatal(err)
				}
				defer db.Close()
				if err = db.Ingest([]string{path}); err != nil {
					t.Fatal(err)
				}
				snap := db.NewSnapshot()
				defer snap.Close()
				index, err := NewRollupSnapshot(context.Background(), snap, nil, nil, nil, 0)
				if err != nil {
					t.Fatal(err)
				}
				err = index.ScanArchive(context.Background(), string(name), 0, 0, 20000, func(sealrollup.Record) error { return nil })
				if err == nil || !strings.Contains(err.Error(), "order summary") {
					t.Fatalf("native accepted invalid summary: %v", err)
				}
				return
			}
			if err = os.Rename(path, filepath.Join(dir, "parts", hex.EncodeToString(d.SHA256[:])+".sst")); err != nil {
				t.Fatal(err)
			}
			out := filepath.Join(dir, "output.sst")
			excluded := map[string]bool{string(name): tc.copied}
			merged, err := mergeParts(context.Background(), dir, out, []v2part.Descriptor{d}, 0, 0, tc.prefix, nil, nil, nil, excluded)
			if err != nil {
				t.Fatal(err)
			}
			runs, summaries := 0, 0
			err = v2part.Read(context.Background(), out, &merged, func(k v2part.Key, b []byte) error {
				if k.Archive == v2part.MetadataArchive {
					return nil
				}
				runs++
				if v2part.HasOrderSummary(b) {
					summaries++
				}
				return nil
			})
			if err != nil {
				t.Fatal(err)
			}
			if runs < 2 || (summaries == runs) != tc.want || !tc.want && summaries != 0 {
				t.Fatalf("certificate: summaries=%d runs=%d want=%v", summaries, runs, tc.want)
			}
			if err = VerifyMovePart(context.Background(), out, merged); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestMergeOrderSummaryMixedRestartAdoption(t *testing.T) {
	t.Setenv("CORE_MERGE_WORKERS", "0")
	ctx := context.Background()
	root := t.TempDir()
	e := mergeFixtureIdle(t, root, nil)
	defer func() {
		if e != nil {
			_ = e.close()
		}
	}()
	policy, raw := rollupTestPolicy(t, "summary.lifecycle", "1s:1h", sealrollup.Last, 0)
	digest, _ := sealrollup.PolicyDigest(policy.Config)
	install := func(seq, reset, order uint64, slot int64, bits uint64) {
		o := e.owners[0]
		o.partSource.Seq = seq
		md := RollupMetadata{Version: 4, ID: 7, Policy: raw, Revision: seq, ResetOrder: reset, Frontier: sealrollup.Frontier{Order: 1000, MaxTime: 4000, Policy: digest}}
		r := sealrollup.Record{Delta: sealrollup.Delta{Slot: slot, Order: order, Sample: sealrollup.Sample{Time: 3600 + slot, Bits: bits}}}
		installMergeRecords(t, o, byte(seq), []RollupMetadata{md}, [][]sealrollup.Record{{r}})
		o.partSource.Seq = 0
	}
	read := func(engine *coreEngine) map[int64]uint64 {
		snap := engine.owners[0].db.NewSnapshot()
		defer snap.Close()
		index, err := NewRollupSnapshot(ctx, snap, nil, nil, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		got := map[int64]uint64{}
		for range 2 {
			err = index.ScanArchive(ctx, string(policy.Name), 0, 0, 3600, func(r sealrollup.Record) error { got[r.Slot] = r.Sample.Bits; return nil })
			if err != nil {
				t.Fatal(err)
			}
		}
		return got
	}
	check := func(want map[int64]uint64) {
		t.Helper()
		if got := read(e); !reflect.DeepEqual(got, want) {
			t.Fatalf("samples %v want %v", got, want)
		}
	}
	install(1, 0, 90, 0, 1<<63)
	install(2, 0, 100, 1, 0x7ff8000000000042)
	want := map[int64]uint64{0: 1 << 63, 1: 0x7ff8000000000042}
	check(want)
	if changed, err := e.owners[0].mergeSealedParts(nil, true); err != nil || !changed {
		t.Fatalf("merge %v %v", changed, err)
	}
	parts := seqLiveParts(t, e.owners[0])
	if len(parts) != 1 {
		t.Fatalf("parts %d", len(parts))
	}
	part := parts[0]
	path := filepath.Join(e.owners[0].dir, "parts", hex.EncodeToString(part.SHA256[:])+".sst")
	summaries := 0
	if err := v2part.Read(ctx, path, &part, func(k v2part.Key, b []byte) error {
		if v2part.HasOrderSummary(b) {
			summaries++
		}
		return nil
	}); err != nil || summaries == 0 {
		t.Fatalf("summary %d %v", summaries, err)
	}
	// A genuine summary is transportable and preserves IEEE bits under adoption.
	prepared, err := PrepareMove(ctx, t.TempDir(), []MovePart{{Path: path, Descriptor: part}})
	if err != nil {
		t.Fatal(err)
	}
	defer prepared.Close()
	receiver := mergeFixtureIdle(t, t.TempDir(), nil)
	defer receiver.close()
	if err = receiver.AdoptMove(ctx, prepared, map[string]uint64{string(policy.Name): 7}, 4000); err != nil {
		t.Fatal(err)
	}
	if got := read(receiver); !reflect.DeepEqual(got, want) {
		t.Fatalf("adopted %v want %v", got, want)
	}
	// A lower exact row order in a later native part still wins an equal time.
	install(3, 0, 1, 0, math.Float64bits(9))
	want[0] = math.Float64bits(9)
	check(want)
	if err = e.close(); err != nil {
		t.Fatal(err)
	}
	e = mergeFixtureIdle(t, root, nil)
	check(want)
	if changed, err := e.owners[0].mergeSealedParts(nil, true); err != nil || !changed {
		t.Fatalf("mixed merge %v %v", changed, err)
	}
	check(want)
	install(4, 3, 2, 2, math.Float64bits(5))
	want = map[int64]uint64{2: math.Float64bits(5)}
	check(want)
	if changed, err := e.owners[0].mergeSealedParts(nil, true); err != nil || !changed {
		t.Fatalf("reset merge %v %v", changed, err)
	}
	check(want)
	if err = e.close(); err != nil {
		t.Fatal(err)
	}
	e = mergeFixtureIdle(t, root, nil)
	check(want)
}

// A kinds run takes the versioned path, but later blocks and members must still
// reject summaries whose policy needs exact history orders.
func TestMergeOrderSummaryMixedLegacyReject(t *testing.T) {
	r := sealrollup.Record{Delta: sealrollup.Delta{Slot: 1, Order: 1, Sample: sealrollup.Sample{Time: 1, Bits: math.Float64bits(1)}}}
	raw, err := v2part.EncodeSlots([]sealrollup.Record{r})
	if err != nil {
		t.Fatal(err)
	}
	summary := append([]byte(nil), raw[:5]...)
	summary[4] |= 1 << 3
	summary = append(summary, 1<<1)
	summary = append(summary, raw[5:]...)
	if _, err = v2part.DecodeSlots(0, 1, summary); err != nil {
		t.Fatal(err)
	}
	r.Kind, r.Upto = sealrollup.KindMat, 1
	kinds, err := v2part.EncodeSlots([]sealrollup.Record{r})
	if err != nil {
		t.Fatal(err)
	}
	config := sealrollup.Config{Method: sealrollup.Last, Archives: []sealrollup.Archive{{Step: 1, Count: 100}, {Step: 10, Count: 100}}}
	for _, split := range []bool{false, true} {
		first := &mergeCursor{seq: 1, arena: append([]byte(nil), kinds...), blocks: []mergeBlock{{archive: 0, slot: 1, start: 0, end: len(kinds)}}}
		members := []*mergeCursor{first}
		if split {
			members = append(members, &mergeCursor{seq: 2, arena: summary, blocks: []mergeBlock{{archive: 0, slot: 1, start: 0, end: len(summary)}}})
		} else {
			first.arena = append(first.arena, summary...)
			first.blocks = append(first.blocks, mergeBlock{archive: 0, slot: 1, start: len(kinds), end: len(first.arena)})
		}
		var versions mergeVersions
		if _, err = versions.merge(members, config, &RollupMetadata{Version: 4}, 0, true); err == nil || !strings.Contains(err.Error(), "order summary") {
			t.Fatalf("split=%v: accepted invalid mixed summary: %v", split, err)
		}
	}
}
