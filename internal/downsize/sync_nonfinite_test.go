package downsize

import (
	"bytes"
	"context"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/vfs"
	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/internal/whisperio"
)

// Use actual store snapshots, sums and native import; a fake Copy would hide
// the original mismatch between sync's finite coverage and import precedence.
type nonFiniteSyncBackend struct {
	*fakeSync
	db map[string]*store.Store
}

func (b *nonFiniteSyncBackend) Info(ctx context.Context, origin string, names []string) (map[string][]syncArchive, error) {
	out := map[string][]syncArchive{}
	for _, name := range names {
		m, err := b.db[origin].Metadata(ctx, name)
		if err != nil {
			return nil, err
		}
		for _, r := range m.Retentions {
			out[name] = append(out[name], syncArchive{int64(r.Step), int64(r.Count)})
		}
	}
	return out, nil
}
func (b *nonFiniteSyncBackend) Fetch(ctx context.Context, origin string, names []string, from, until int64) (map[string]syncSeries, error) {
	out := map[string]syncSeries{}
	for _, name := range names {
		s, err := b.db[origin].Fetch(ctx, name, int(from), int(until))
		if err != nil {
			return nil, err
		}
		out[name] = syncSeries{Start: int64(s.FromTime), Step: int64(s.Step), Values: s.Values}
	}
	return out, nil
}
func (b *nonFiniteSyncBackend) Digest(context.Context, string, []string, int64, int64) (map[string][]syncDigest, error) {
	return nil, errSyncNoDigest
}
func (b *nonFiniteSyncBackend) SumDigest(ctx context.Context, origin string, names []string, step, from, until int64) (map[string][]syncDigest, error) {
	out := map[string][]syncDigest{}
	for _, name := range names {
		sums, err := b.db[origin].SumDigest(ctx, name, int(from), int(until), int(step))
		if err != nil {
			return nil, err
		}
		for _, s := range sums {
			out[name] = append(out[name], syncDigest{Step: int64(s.Step), Count: int64(s.Count), Hash: s.Sum, Points: int64(s.Points), From: int64(s.From), Until: int64(s.Until)})
		}
	}
	return out, nil
}
func (b *nonFiniteSyncBackend) Copy(ctx context.Context, source, target string, names []string, _ string) (syncCopyResult, error) {
	pin, err := b.db[source].PinNativeSource()
	if err != nil {
		return syncCopyResult{}, err
	}
	defer pin.Close()
	var wire bytes.Buffer
	if err := pin.Write(ctx, names, &wire); err != nil {
		return syncCopyResult{}, err
	}
	r := store.NativeRange{Operation: strings.Repeat("1", 64), Plan: strings.Repeat("2", 64), Stream: strings.Repeat("3", 64), Source: "donor", Names: names, End: int64(len(names)), Combine: true}
	receipt, err := b.db[target].NativeImport(ctx, r, &wire)
	return syncCopyResult{Copied: receipt.CopiedCount}, err
}
func TestSyncVerifyFillVerifyNonFinite(t *testing.T) {
	for _, continuation := range []bool{false, true} {
		t.Run(map[bool]string{false: "exact", true: "continuation"}[continuation], func(t *testing.T) {
			ctx := context.Background()
			clock := func() time.Time { return time.Unix(syncTestNow, 0) }
			b := &nonFiniteSyncBackend{fakeSync: newFakeSync(), db: map[string]*store.Store{}}
			cfg := store.MetricConfig{Name: "sync.nonfinite", Retentions: []store.Retention{{Step: 60, Count: 1440}, {Step: 1800, Count: 200}}, AggregationMethod: store.Average}
			ts := syncTestNow - 2*86400
			for _, origin := range []string{syncD1, syncT} {
				opts := store.Options{ExactBlocks: true, CacheSize: 1 << 20, MemTableSize: 1 << 20, Now: clock}
				if continuation {
					opts.Continuation = &whisperio.ContinuationCodec{}
				}
				db, err := store.Open("/db", opts.WithFS(vfs.NewMem()))
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() {
					if err := db.Close(); err != nil {
						t.Error(err)
					}
				})
				b.db[origin] = db
				if _, err := db.Create(ctx, cfg); err != nil {
					t.Fatal(err)
				}
				values := []float64{286.73400000000004, 989.205, 32}
				if origin == syncT {
					values = []float64{math.Inf(1), math.Inf(-1), 42}
				}
				for i, v := range values {
					if err := db.UpdateManyForArchive(ctx, cfg.Name, []store.Point{{Timestamp: ts + int64(i*1800), Value: v}}, cfg.Retentions[1].MaxRetention()); err != nil {
						t.Fatal(err)
					}
				}
			}
			s := &syncer{o: syncOptions{from: []string{syncD1}, to: syncT, now: clock, includeShort: true}, b: b}
			s.masks.Store(cfg.Name, uint64(1))
			before, err := s.verifyBatch(ctx, 1, []string{cfg.Name})
			if err != nil {
				t.Fatal(err)
			}
			if len(before.Skipped) != 0 || len(before.Holes) != 1 || before.Holes[0].Points != 2 {
				t.Fatalf("before: %+v", before)
			}
			result, err := b.Copy(ctx, syncD1, syncT, []string{cfg.Name}, "")
			if err != nil || result.Copied != 1 {
				t.Fatal(result, err)
			}
			after, err := s.verifyBatch(ctx, 2, []string{cfg.Name})
			if err != nil {
				t.Fatal(err)
			}
			if len(after.Holes) != 0 || len(after.Skipped) != 0 || len(after.Unindexed) != 0 {
				t.Fatalf("after: %+v", after)
			}
			snap, err := b.db[syncT].Snapshot(ctx, cfg.Name)
			if err != nil {
				t.Fatal(err)
			}
			for i, p := range snap.Archives[1].Points {
				want := []float64{286.73400000000004, 989.205, 42}[i]
				if math.Float64bits(p.Value) != math.Float64bits(want) {
					t.Fatalf("point%d changed: %v", i, p.Value)
				}
			}
		})
	}
}
