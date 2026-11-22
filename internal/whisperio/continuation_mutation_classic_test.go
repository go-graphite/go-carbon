package whisperio

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"reflect"
	"testing"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	whisper "github.com/go-graphite/go-whisper"
)

type legacyMutationCodec struct{ ContinuationCodec }

func (c *legacyMutationCodec) Update(ctx context.Context, kv store.ContinuationIO, m store.Metadata, p []store.Point, r int, clock func() time.Time, maintenance *store.ContinuationMaintenance) error {
	return c.updateWithLegacyMutationProof(ctx, kv, m, p, r, clock, maintenance)
}

func mutationClassicFixture(t testing.TB, referenced bool, retentions []store.Retention, archives [][]store.Point, clock func() time.Time) (mapContinuationIO, store.Metadata) {
	t.Helper()
	m := store.Metadata{MetricConfig: store.MetricConfig{Name: "proof.classic", Retentions: retentions, AggregationMethod: store.Sum, XFilesFactor: .5}}
	codec := &ContinuationCodec{}
	kv := mapContinuationIO{}
	if !referenced {
		snap := store.Snapshot{Metadata: m}
		for ai, r := range retentions {
			snap.Archives = append(snap.Archives, store.Archive{Retention: r, Points: archives[ai]})
		}
		if err := codec.RestoreArchives(context.Background(), kv, snap, clock); err != nil {
			t.Fatal(err)
		}
		return kv, m
	}
	parts := make([][]splicePart, len(retentions))
	bases := make([]int64, len(retentions))
	for ai, points := range archives {
		bases[ai] = points[0].Timestamp
		for start := 0; start < len(points); start += 4096 {
			end := min(start+4096, len(points))
			data, err := store.EncodeSealedPoints(points[start:end], retentions[ai].Step)
			if err != nil {
				t.Fatal(err)
			}
			payload := &splicePayload{data: data, sealed: true}
			parts[ai] = append(parts[ai], splicePart{fragment: spliceFragment{first: points[start].Timestamp, count: end - start, payload: payload}, slot: int(points[start].Timestamp / int64(retentions[ai].Step) % int64(retentions[ai].Count)), count: end - start})
		}
	}
	keys, err := codec.writeReferencedArchives(context.Background(), m, parts, bases, &store.NativeCombineClass{}, clock)
	if err != nil {
		t.Fatal(err)
	}
	for _, item := range keys {
		kv[string(item.Key)] = item.Value
	}
	return kv, m
}

func TestClassicMutationDifferential(t *testing.T) {
	for _, referenced := range []bool{false, true} {
		t.Run(fmt.Sprintf("referenced=%v", referenced), func(t *testing.T) {
			ctx := context.Background()
			now := int64(1700000000)
			clock := func() time.Time { return time.Unix(now, 0) }
			r := []store.Retention{{Step: 1, Count: 512}, {Step: 8, Count: 256}}
			archives := make([][]store.Point, 2)
			for ts := now - 256; ts < now; ts++ {
				archives[0] = append(archives[0], store.Point{Timestamp: ts, Value: float64(ts % 31)})
			}
			for ts := now - 1024; ts < now; ts += 8 {
				archives[1] = append(archives[1], store.Point{Timestamp: ts, Value: float64(ts % 17)})
			}
			kv, m := mutationClassicFixture(t, referenced, r, archives, clock)
			var dbs [2]*store.Store
			for i, codec := range []store.ContinuationCodec{&legacyMutationCodec{}, &ContinuationCodec{}} {
				db, err := store.Open(t.TempDir(), store.Options{ExactBlocks: true, Continuation: codec, Now: clock, SyncInterval: time.Hour, CacheSize: 1 << 20, MemTableSize: 1 << 20})
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() {
					if err := db.Close(); err != nil {
						t.Error(err)
					}
				})
				if _, err = db.ImportTransfer(ctx, []store.TransferMetric{classicTransfer(kv, store.Snapshot{Metadata: m})}); err != nil {
					t.Fatal(err)
				}
				dbs[i] = db
			}
			scopes := [][2]int64{{now - 100, now - 50}, {now - 257, now + 1}, {now - 1023, now - 511}}
			proof := func(db *store.Store, scope [2]int64) string {
				t.Helper()
				p, _, err := db.MutationProof(ctx, m.Name, scope[0], scope[1])
				if err != nil || p == "" {
					t.Fatalf("proof %q: %v", p, err)
				}
				return p
			}
			check := func() {
				t.Helper()
				a, err := dbs[0].Snapshot(ctx, m.Name)
				if err != nil {
					t.Fatal(err)
				}
				b, err := dbs[1].Snapshot(ctx, m.Name)
				if err != nil {
					t.Fatal(err)
				}
				if err := store.EqualSnapshots(a, b); err != nil {
					t.Fatal(err)
				}
			}
			for seq, batch := range [][]store.Point{
				{{Timestamp: now, Value: 3}},       // Append; propagation must include coarse targets.
				{{Timestamp: now - 80, Value: 99}}, // In-scope OOO correction.
				{{Timestamp: now - 79, Value: math.Copysign(0, -1)}, {Timestamp: now - 78, Value: math.Float64frombits(1)}},
				{{Timestamp: now - 80, Value: math.Float64frombits(0x7ff8000000001234)}},
				{{Timestamp: now + 8, Value: 7}},    // Next rollup interval.
				{{Timestamp: now + 512, Value: 11}}, // Wrap and replace the circular owner.
			} {
				old := make([][2]string, len(scopes))
				for j, scope := range scopes {
					for i, db := range dbs {
						old[j][i] = proof(db, scope)
					}
				}
				for _, db := range dbs {
					if err := db.UpdateMany(ctx, m.Name, batch); err != nil {
						t.Fatalf("batch %d: %v", seq, err)
					}
				}
				for j, scope := range scopes {
					legacyChanged := proof(dbs[0], scope) != old[j][0]
					changed := proof(dbs[1], scope) != old[j][1]
					if legacyChanged && !changed {
						t.Fatalf("batch %d scope %v: new proof is less conservative", seq, scope)
					}
					if seq == 0 && j == 0 && !referenced && changed {
						t.Fatal("ordinary append invalidated disjoint historical scope")
					}
					if seq == 1 && j == 0 && !changed {
						t.Fatal("OOO correction retained a stale proof")
					}
				}
				check()
				now = max(now, batch[0].Timestamp+1)
			}
			for _, db := range dbs {
				before := proof(db, scopes[0])
				if err := db.Delete(ctx, m.Name); err != nil {
					t.Fatal(err)
				}
				if proof(db, scopes[0]) == before {
					t.Fatal("deletion retained proof")
				}
			}
		})
	}
}

type memoryMutationIO struct {
	mapContinuationIO
	baseline mapContinuationIO
	ranges   []store.MutationRange
	complete bool
}

func (m *memoryMutationIO) MutationBaseline() (store.ContinuationIO, func()) {
	return m.baseline, func() {}
}
func (m *memoryMutationIO) MutationWindows() []store.MutationRange {
	return []store.MutationRange{{From: 1699999900, Until: 1700000001}}
}
func (m *memoryMutationIO) RecordMutation(ai int, from, until int64) {
	m.ranges = append(m.ranges, store.MutationRange{Archive: ai, From: from, Until: until})
}
func (m *memoryMutationIO) CompleteMutation() { m.complete = true }
func cloneMutationMap(src mapContinuationIO) mapContinuationIO {
	dst := mapContinuationIO{}
	for k, v := range src {
		dst[k] = bytes.Clone(v)
	}
	return dst
}

func TestClassicMutationStoredBytes(t *testing.T) {
	clock := func() time.Time { return time.Unix(1700000000, 0) }
	for _, refs := range []bool{false, true} {
		kv, m := mutationClassicFixture(t, refs, []store.Retention{{Step: 1, Count: 512}}, [][]store.Point{{{Timestamp: 1699999800, Value: 1}, {Timestamp: 1699999920, Value: 2}}}, clock)
		old, newKV := cloneMutationMap(kv), cloneMutationMap(kv)
		for _, p := range []store.Point{{Timestamp: 1699999990, Value: 4}, {Timestamp: 1699999920, Value: math.Copysign(0, -1)}, {Timestamp: 1699999920, Value: math.Float64frombits(0x7ff8000000001234)}, {Timestamp: 1700000320, Value: 5}} {
			a := &memoryMutationIO{mapContinuationIO: old, baseline: cloneMutationMap(old)}
			b := &memoryMutationIO{mapContinuationIO: newKV, baseline: cloneMutationMap(newKV)}
			c := &ContinuationCodec{}
			if err := c.updateWithLegacyMutationProof(context.Background(), a, m, []store.Point{p}, -1, clock, nil); err != nil {
				t.Fatal(err)
			}
			if err := c.Update(context.Background(), b, m, []store.Point{p}, -1, clock, nil); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(old, newKV) {
				t.Fatalf("refs=%v point=%+v: stored bytes differ", refs, p)
			}
		}
	}
}

func TestClassicMutationRollupTarget(t *testing.T) {
	ctx := context.Background()
	const now = int64(1700000000)
	clock := func() time.Time { return time.Unix(now, 0) }
	var fine []store.Point
	for ts := now - 256; ts < now; ts++ {
		fine = append(fine, store.Point{Timestamp: ts, Value: 1})
	}
	kv, m := mutationClassicFixture(t, false, []store.Retention{{Step: 1, Count: 512}, {Step: 8, Count: 256}}, [][]store.Point{fine, {{Timestamp: now - 1024, Value: 8}, {Timestamp: now - 8, Value: 8}}}, clock)
	db, err := store.Open(t.TempDir(), store.Options{ExactBlocks: true, Continuation: &ContinuationCodec{}, Now: clock})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err = db.ImportTransfer(ctx, []store.TransferMetric{classicTransfer(kv, store.Snapshot{Metadata: m})}); err != nil {
		t.Fatal(err)
	}
	before, _, err := db.MutationProof(ctx, m.Name, now-8, now-7)
	if err != nil {
		t.Fatal(err)
	}
	if err = db.UpdateMany(ctx, m.Name, []store.Point{{Timestamp: now - 1, Value: 10}}); err != nil {
		t.Fatal(err)
	}
	after, _, err := db.MutationProof(ctx, m.Name, now-8, now-7)
	if err != nil {
		t.Fatal(err)
	}
	if before == after {
		t.Fatal("out-of-scope fine correction changed an in-scope rollup without invalidating proof")
	}
}

func TestClassicMutationGroupDeletionAndFallbacks(t *testing.T) {
	clock := func() time.Time { return time.Unix(1700000000, 0) }
	kv, _ := mutationClassicFixture(t, false, []store.Retention{{Step: 1, Count: 512}}, [][]store.Point{{{Timestamp: 1699999800, Value: 1}, {Timestamp: 1699999930, Value: 2}}}, clock)
	before, err := readSumWriteView(kv, clock)
	if err != nil {
		t.Fatal(err)
	}
	windows := [][]whisper.ArchiveWindow{{{Archive: 0, From: 1699999900, Until: 1700000000}}}
	for _, kind := range []string{"delete", "anchor", "alias", "prefix", "namespace", "sidecar"} {
		t.Run(kind, func(t *testing.T) {
			current := cloneMutationMap(kv)
			changes := newContinuationOverlay(current)
			key := continuationBodyKey(before.file, 'p', 0, 0, 1)
			afterView := before
			switch kind {
			case "delete":
				changes.Delete(key)
			case "anchor":
				changes.Delete(continuationBodyKey(before.file, 'p', 0, 0, 0))
			case "alias":
				key[9] = 'a'
				changes.Delete(key)
			case "prefix":
				changes.DeletePrefix(continuationBlockPrefix(before.file, 'p', 0, 0))
			case "namespace":
				changes.Set(continuationNameKey("other"), []byte{1})
			case "sidecar":
				afterView.sidecar = true
			}
			if err := changes.commit(); err != nil {
				t.Fatal(err)
			}
			recorder := &memoryMutationIO{mapContinuationIO: current, baseline: kv}
			if err := refineClassicMutation(kv, current, changes, before, afterView, windows, recorder, clock); err != nil {
				t.Fatal(err)
			}
			if kind == "delete" {
				if !recorder.complete || len(recorder.ranges) != 1 || recorder.ranges[0].From != 1699999930 {
					t.Fatalf("deleted owner not recorded: %+v complete=%v", recorder.ranges, recorder.complete)
				}
			} else if recorder.complete {
				t.Fatalf("unsupported %s declared complete", kind)
			}
		})
	}
}
