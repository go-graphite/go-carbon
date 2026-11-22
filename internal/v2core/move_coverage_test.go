package v2core

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"testing"

	"github.com/go-graphite/go-carbon/internal/sealrollup"
	"github.com/go-graphite/go-carbon/internal/v2part"
)

// A mixed-window union cannot recover an average's weight, or the value an
// overlapping receiver point replaces, after a donor TTL merge drops raw.
// Each pair has identical final SST bytes and descriptors, but distinct union
// values. No adopt algorithm over those immutable inputs can distinguish them.
func TestMoveAdoptRetiredDonorCoverage(t *testing.T) {
	for _, method := range []sealrollup.Method{sealrollup.Average, sealrollup.Sum} {
		name := "average-loses-weight"
		if method == sealrollup.Sum {
			name = "sum-loses-overwritten-contribution"
		}
		t.Run(name, func(t *testing.T) { testMoveAdoptRetiredDonorCoverage(t, method) })
	}
}

func testMoveAdoptRetiredDonorCoverage(t *testing.T, method sealrollup.Method) {
	policy := adoptPolicy(t, method)
	var p RollupPolicy
	if err := json.Unmarshal(policy, &p); err != nil {
		t.Fatal(err)
	}
	savedPartID := mergePartID
	defer func() { mergePartID = savedPartID }()
	mergePartID = func(id *v2part.PartID) error { *id = v2part.PartID{42}; return nil }
	var parts []MovePart
	var expected []uint64
	histories := [][]sealrollup.Sample{
		{moveSample(1000, 2), moveSample(1001, 2), moveSample(1002, 2), moveSample(1003, 2), moveSample(1005, 2)},
		{moveSample(1000, 2), moveSample(1001, 2), moveSample(1002, 2), moveSample(1003, 2), moveSample(1004, 2), moveSample(1005, 2)},
	}
	receiver := []sealrollup.Sample{moveSample(1006, 10), moveSample(1007, 10), moveSample(1008, 10), moveSample(1009, 10)}
	labels := []string{"five-donor-slots", "six-donor-slots"}
	if method == sealrollup.Sum {
		histories = [][]sealrollup.Sample{
			{moveSample(1000, 1), moveSample(1001, 2), moveSample(1002, 3), moveSample(1003, 4), moveSample(1004, 5)},
			{moveSample(1000, 2), moveSample(1001, 1), moveSample(1002, 3), moveSample(1003, 4), moveSample(1004, 5)},
		}
		receiver = []sealrollup.Sample{moveSample(1000, 10), moveSample(1005, 6), moveSample(1006, 7), moveSample(1007, 8), moveSample(1008, 9)}
		labels = []string{"replaces-one", "replaces-two"}
	}
	for _, samples := range histories {
		var rows []Row
		var versions []sealrollup.Version
		var union []sealrollup.Sample
		for i, sample := range samples {
			rows = append(rows, Row{ID: 99, Time: sample.Time, Bits: sample.Bits, Seq: uint64(i + 1)})
			versions = append(versions, sealrollup.Version{Archive: 0, Time: sample.Time, Bits: sample.Bits, Order: sealrollup.Tick(100, 0)})
			overwritten := slices.ContainsFunc(receiver, func(r sealrollup.Sample) bool { return r.Time == sample.Time })
			if !overwritten {
				union = append(union, sample)
			}
		}
		union = append(union, receiver...)
		slices.SortFunc(union, compareHistorySamples)
		want, ok := sealrollup.AggregateWindow(p.Config, 1, 1000, union)
		if !ok {
			t.Fatal("complete union did not pass XFF")
		}
		expected = append(expected, want.Bits)
		// Advance the donor's admission clock while the old coarse window is
		// still retained (1000+240 > 1200), but its raw rows expired (<1200).
		later := Row{ID: 99, Time: 1200, Bits: math.Float64bits(7), Seq: 1}
		versions = append(versions, sealrollup.Version{Archive: 0, Time: later.Time, Bits: later.Bits, Order: sealrollup.Tick(101, 0)})
		plan, err := newCoreRollups().plan(coreSealInput{Batches: []Batch{
			{ID: 99, Order: 100, Clock: 1010, Policy: policy, Points: rows},
			{ID: 99, Order: 101, Clock: 1200, Policy: policy, Points: []Row{later}},
		}})
		if err != nil {
			t.Fatal(err)
		}
		var entries []KV
		var template v2part.Key
		for _, kv := range plan.Files[0] {
			key, err := v2part.Decode(kv.Key)
			if err != nil {
				t.Fatal(err)
			}
			key.Part = v2part.PartID{42}
			template = key
			if key.Archive == v2part.MetadataArchive {
				encoded, err := v2part.Encode(key)
				if err != nil {
					t.Fatal(err)
				}
				entries = append(entries, KV{Key: encoded, Value: kv.Value})
			}
		}
		for _, v := range versions {
			r := v.Record(p.Config)
			key := template
			key.Archive, key.BlockStart = uint32(r.Archive), r.Slot
			encoded, err := v2part.Encode(key)
			if err != nil {
				t.Fatal(err)
			}
			entries = append(entries, KV{Key: encoded, Record: &r})
		}
		slices.SortFunc(entries, func(a, b KV) int { return bytes.Compare(a.Key, b.Key) })
		dir := t.TempDir()
		path := filepath.Join(dir, "input.sst")
		d, err := WriteDenseRollupPart(path, entries, v2part.Provenance{Incarnation: v2part.PartID{1}, Stream: "donor"})
		if err != nil {
			t.Fatal(err)
		}
		if err := os.Mkdir(filepath.Join(dir, "parts"), 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.Link(path, filepath.Join(dir, "parts", hex.EncodeToString(d.SHA256[:])+".sst")); err != nil {
			t.Fatal(err)
		}
		path = filepath.Join(dir, "retired-donor.sst")
		d, err = mergePartFiles(context.Background(), dir, path, []v2part.Descriptor{d}, 1200, nil)
		if err != nil {
			t.Fatal(err)
		}
		parts = append(parts, MovePart{Path: path, Descriptor: d})
	}
	a, err := os.ReadFile(parts[0].Path)
	if err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(parts[1].Path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(a, b) || !reflect.DeepEqual(parts[0].Descriptor, parts[1].Descriptor) {
		t.Fatal("counterexample donors must have identical SST bytes and descriptors")
	}
	if expected[0] == expected[1] {
		t.Fatal("counterexample unions must differ")
	}
	t.Logf("identical donor SST sha=%x bytes=%d; exact union values: %.17g / %.17g", parts[0].Descriptor.SHA256, len(a), math.Float64frombits(expected[0]), math.Float64frombits(expected[1]))
	for i, part := range parts {
		t.Run(labels[i], func(t *testing.T) {
			cfg := adoptConfig()
			cfg.DeferMerges = true
			root := t.TempDir()
			e, err := Open(root, cfg, Hooks{})
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if e != nil {
					_ = e.Close()
				}
			}()
			adoptLive(t, e, policy, 1, 1, receiver...)
			prepared, err := PrepareMove(context.Background(), t.TempDir(), []MovePart{part})
			if err != nil {
				t.Fatal(err)
			}
			defer prepared.Close()
			if err := e.AdoptMove(context.Background(), prepared, map[string]uint64{"move.metric": 7}, 1200); err != nil {
				t.Fatal(err)
			}
			check := func(stage string) {
				_, records := adoptedRecords(t, e)
				for _, r := range records {
					if r.Archive == 1 && r.Sample.Time == 1000 {
						if r.Sample.Bits != expected[i] {
							t.Errorf("%s: got %.17g want %.17g", stage, math.Float64frombits(r.Sample.Bits), math.Float64frombits(expected[i]))
						}
						return
					}
				}
				t.Errorf("%s: mixed window missing after donor raw retirement", stage)
			}
			check("adopt")
			if err := e.Close(); err != nil {
				t.Fatal(err)
			}
			e = nil
			e, err = Open(root, cfg, Hooks{})
			if err != nil {
				t.Fatal(err)
			}
			check("restart")
		})
	}
}
