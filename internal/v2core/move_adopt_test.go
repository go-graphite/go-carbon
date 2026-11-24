package v2core

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"slices"
	"testing"

	"github.com/go-graphite/go-carbon/internal/sealrollup"
	"github.com/go-graphite/go-carbon/internal/v2part"
)

func adoptConfig() Config {
	return Config{Rollups: true, Producers: 1, Shards: 1, MaxRows: 128, PartitionSeconds: 86400, AppendParts: true}
}

func adoptPolicy(t *testing.T, method sealrollup.Method) []byte {
	t.Helper()
	b, err := EncodeRollupPolicy(RollupPolicy{Name: "move.metric", Group: "test", Config: sealrollup.Config{Method: method, XFilesFactor: .5, Archives: []sealrollup.Archive{{Step: 1, Count: 120}, {Step: 10, Count: 24}}}})
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func adoptDonor(t *testing.T, policy []byte, samples ...sealrollup.Sample) MovePart {
	t.Helper()
	r := newCoreRollups()
	rows := make([]Row, len(samples))
	for i, s := range samples {
		rows[i] = Row{ID: 99, Time: s.Time, Bits: s.Bits, Seq: uint64(i + 1)}
	}
	plan, err := r.plan(coreSealInput{Batches: []Batch{{ID: 99, Order: 100, Clock: 1010, Policy: policy, Points: rows}}})
	if err != nil {
		t.Fatal(err)
	}
	defer plan.Abort()
	path := filepath.Join(t.TempDir(), "donor.sst")
	d, err := WriteDenseRollupPart(path, plan.Files[0], v2part.Provenance{Incarnation: v2part.PartID{1}, Stream: "donor"})
	if err != nil {
		t.Fatal(err)
	}
	return MovePart{Path: path, Descriptor: d}
}

func adoptLive(t *testing.T, e *Engine, policy []byte, order, cursor uint64, samples ...sealrollup.Sample) {
	t.Helper()
	var rows []Row
	for i, s := range samples {
		rows = append(rows, Row{ID: 7, Time: s.Time, Bits: s.Bits, Seq: uint64(i + 1)})
	}
	if err := e.AppendBatches(0, []Batch{{ID: 7, Order: order, Clock: 1010, Policy: policy, Points: rows}}, cursor, true); err != nil {
		t.Fatal(err)
	}
}

func adoptedRecords(t *testing.T, e *Engine) (RollupMetadata, []sealrollup.Record) {
	t.Helper()
	snapshot := e.DBs()[0].NewSnapshot()
	defer snapshot.Close()
	var meta RollupMetadata
	var records []sealrollup.Record
	err := scanRollups(snapshot, e.identities, func(_ RollupPolicy, m RollupMetadata, r []sealrollup.Record) error { meta = m; records = r; return nil })
	if err != nil {
		t.Fatal(err)
	}
	slices.SortFunc(records, func(a, b sealrollup.Record) int {
		if a.Archive != b.Archive {
			return a.Archive - b.Archive
		}
		return int(a.Slot - b.Slot)
	})
	return meta, records
}

func TestMoveAdoptIdentityReducerRestart(t *testing.T) {
	for _, method := range []sealrollup.Method{sealrollup.Average, sealrollup.Sum, sealrollup.Last, sealrollup.Max, sealrollup.Min, sealrollup.First} {
		t.Run(fmt.Sprint(method), func(t *testing.T) {
			ctx := context.Background()
			policy := adoptPolicy(t, method)
			root := t.TempDir()
			e, err := Open(root, adoptConfig(), Hooks{})
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if e != nil {
					_ = e.Close()
				}
			}()
			adoptLive(t, e, policy, 1, 2, moveSample(1000, 2), moveSample(1002, 4))
			donor := adoptDonor(t, policy, moveSample(1000, 1), moveSample(1001, 3), moveSample(1002, 3), sealrollup.Sample{Time: 1003, Bits: 0x8000000000000000}, sealrollup.Sample{Time: 1004, Bits: 0x7ff8000000000042})
			prepared, err := PrepareMove(ctx, t.TempDir(), []MovePart{donor})
			if err != nil {
				t.Fatal(err)
			}
			defer prepared.Close()
			if err = e.AdoptMove(ctx, prepared, map[string]uint64{"move.metric": 7}, 1010); err != nil {
				t.Fatal(err)
			}
			meta, records := adoptedRecords(t, e)
			if meta.ID != 7 || meta.Frontier.Order != 1 {
				t.Fatalf("adopted donor identity/order: %+v", meta)
			}
			raw := map[int64]uint64{}
			for _, r := range records {
				if r.Archive == 0 {
					raw[r.Sample.Time] = r.Sample.Bits
				}
			}
			want := map[int64]uint64{1000: math.Float64bits(2), 1001: math.Float64bits(3), 1002: math.Float64bits(4), 1003: 0x8000000000000000, 1004: 0x7ff8000000000042}
			if !reflect.DeepEqual(raw, want) {
				t.Fatalf("raw=%v want %v", raw, want)
			}
			// Continuing the receiving stream must use the adopted fine baseline.
			adoptLive(t, e, policy, 2, 3, moveSample(1005, 5))
			_, before := adoptedRecords(t, e)
			if err = e.AdoptMove(ctx, prepared, map[string]uint64{"move.metric": 7}, 1010); err != nil {
				t.Fatal(err)
			}
			if err = e.DBs()[0].Compact([]byte{0}, []byte{255, 255}, true); err != nil {
				t.Fatal(err)
			}
			if err = e.Close(); err != nil {
				t.Fatal(err)
			}
			e = nil
			e, err = Open(root, adoptConfig(), Hooks{})
			if err != nil {
				t.Fatal(err)
			}
			afterMeta, after := adoptedRecords(t, e)
			if afterMeta.ID != 7 || !reflect.DeepEqual(before, after) {
				t.Fatal("restart/compaction changed identity or slots")
			}
			// A reference reducer fed the same final winning raw set is the oracle.
			var p RollupPolicy
			if err = json.Unmarshal(policy, &p); err != nil {
				t.Fatal(err)
			}
			winning := append([]sealrollup.Record(nil), records...)
			winning = append(winning, sealrollup.Record{Delta: sealrollup.Delta{Archive: 0, Slot: 45, Sample: moveSample(1005, 5), Order: 2}})
			var fine []sealrollup.Record
			for _, r := range winning {
				if r.Archive == 0 {
					fine = append(fine, r)
				}
			}
			expected := slices.Clone(fine)
			var samples []sealrollup.Sample
			for _, r := range fine {
				samples = append(samples, r.Sample)
			}
			slices.SortFunc(samples, compareHistorySamples)
			sample, ok := sealrollup.AggregateWindow(p.Config, 1, 1000, samples)
			if !ok {
				t.Fatal("oracle did not pass XFF")
			}
			expected = append(expected, sealrollup.Record{Delta: sealrollup.Delta{Archive: 1, Slot: 4, Sample: sample}})
			values := func(rs []sealrollup.Record) map[[2]int64]sealrollup.Sample {
				out := map[[2]int64]sealrollup.Sample{}
				for _, r := range rs {
					sample := r.Sample
					sample.Sequence = 0
					out[[2]int64{int64(r.Archive), r.Slot}] = sample
				}
				return out
			}
			if !reflect.DeepEqual(values(after), values(expected)) {
				t.Fatalf("reducer coarse mismatch\ngot %v\nwant %v", values(after), values(expected))
			}
		})
	}
}

func TestMoveAdoptCrash(t *testing.T) {
	if root := os.Getenv("V2_ADOPT_ROOT"); root != "" {
		var part MovePart
		b, err := os.ReadFile(os.Getenv("V2_ADOPT_PART"))
		if err != nil {
			t.Fatal(err)
		}
		if err = json.Unmarshal(b, &part); err != nil {
			t.Fatal(err)
		}
		prepared, err := PrepareMove(context.Background(), os.Getenv("V2_ADOPT_PREPARED"), []MovePart{part})
		if err != nil {
			t.Fatal(err)
		}
		defer prepared.Close()
		e, err := Open(root, adoptConfig(), Hooks{Stage: func(stage string) {
			if stage == os.Getenv("V2_ADOPT_STAGE") {
				_ = exec.Command("kill", "-KILL", fmtPID()).Run()
			}
		}})
		if err != nil {
			t.Fatal(err)
		}
		defer e.Close()
		if err = e.AdoptMove(context.Background(), prepared, map[string]uint64{"move.metric": 7}, 1010); err != nil {
			t.Fatal(err)
		}
		return
	}
	for _, stage := range []string{"move-staged-part", "move-before-ingest", "move-after-ingest"} {
		t.Run(stage, func(t *testing.T) {
			root := t.TempDir()
			policy := adoptPolicy(t, sealrollup.Sum)
			e, err := Open(root, adoptConfig(), Hooks{})
			if err != nil {
				t.Fatal(err)
			}
			adoptLive(t, e, policy, 1, 1, moveSample(1000, 2))
			if err = e.Close(); err != nil {
				t.Fatal(err)
			}
			part := adoptDonor(t, policy, moveSample(1000, 1), moveSample(1001, 3))
			b, _ := json.Marshal(part)
			path := filepath.Join(t.TempDir(), "part.json")
			if err = os.WriteFile(path, b, 0600); err != nil {
				t.Fatal(err)
			}
			prep := t.TempDir()
			cmd := exec.Command(os.Args[0], "-test.run=^TestMoveAdoptCrash$")
			cmd.Env = append(os.Environ(), "V2_ADOPT_ROOT="+root, "V2_ADOPT_PART="+path, "V2_ADOPT_PREPARED="+prep, "V2_ADOPT_STAGE="+stage)
			if output, err := cmd.CombinedOutput(); err == nil {
				t.Fatalf("child survived %s: %s", stage, output)
			}
			e, err = Open(root, adoptConfig(), Hooks{})
			if err != nil {
				t.Fatal(err)
			}
			defer e.Close()
			if stage != "move-after-ingest" {
				_, records := adoptedRecords(t, e)
				for _, record := range records {
					if record.Archive == 0 && record.Sample.Time == 1001 {
						t.Fatal("uncommitted staged part became visible after restart")
					}
				}
			}
			prepared, err := PrepareMove(context.Background(), prep, []MovePart{part})
			if err != nil {
				t.Fatal(err)
			}
			defer prepared.Close()
			if err = e.AdoptMove(context.Background(), prepared, map[string]uint64{"move.metric": 7}, 1010); err != nil {
				t.Fatal(err)
			}
			meta, rs := adoptedRecords(t, e)
			if meta.ID != 7 || len(rs) != 2 {
				t.Fatalf("recovery identity=%d records=%v", meta.ID, rs)
			}
		})
	}
}

func TestMoveAdoptNewNameCoarseContinuation(t *testing.T) {
	e, err := Open(t.TempDir(), adoptConfig(), Hooks{})
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	policy := adoptPolicy(t, sealrollup.Sum)
	part := adoptDonor(t, policy, moveSample(1000, 1), moveSample(1001, 2), moveSample(1002, 3), moveSample(1003, 4), moveSample(1004, 5))
	prepared, err := PrepareMove(context.Background(), t.TempDir(), []MovePart{part})
	if err != nil {
		t.Fatal(err)
	}
	defer prepared.Close()
	if err := e.AdoptMove(context.Background(), prepared, map[string]uint64{"move.metric": 7}, 1010); err != nil {
		t.Fatal(err)
	}
	adoptLive(t, e, policy, 1, 1, moveSample(1005, 6))
	_, records := adoptedRecords(t, e)
	for _, record := range records {
		if record.Archive == 1 && record.Sample.Time == 1000 && record.Sample.Bits == math.Float64bits(21) {
			return
		}
	}
	t.Fatalf("first local update did not replace adopted coarse baseline: %v", records)
}

func fmtPID() string { return fmt.Sprint(os.Getpid()) }
