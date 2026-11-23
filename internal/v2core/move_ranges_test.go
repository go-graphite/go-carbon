package v2core

import (
	"context"
	"encoding/json"
	"math"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/go-graphite/go-carbon/internal/sealrollup"
	"github.com/go-graphite/go-carbon/internal/v2part"
)

func TestMoveOrderRanges(t *testing.T) {
	origin := func(stream string) partOrigin {
		return partOrigin{Class: CopiedPart, Incarnation: v2part.PartID{1}, Stream: stream}
	}
	part := func(stream string, lo, hi uint64) MovePart {
		return MovePart{Descriptor: v2part.Descriptor{Source: v2part.Provenance{Incarnation: v2part.PartID{1}, Stream: stream, MinOrder: lo, MaxOrder: hi}}}
	}
	for _, tc := range []struct {
		name     string
		bindings []partImport
		incoming []MovePart
		want     map[partOrigin]uint64
		failure  string
	}{
		{name: "extension-and-replay", bindings: []partImport{{Origin: origin("a"), BaseOrder: 3, Descriptor: part("a", 3, 10).Descriptor}}, incoming: []MovePart{part("a", 1, 20), part("a", 3, 10), part("b", 1, 1)}, want: map[partOrigin]uint64{origin("a"): 0, origin("b"): 20}},
		{name: "imported-ranges", incoming: []MovePart{part("a", 3, 10), part("b", 7, 20)}, want: map[partOrigin]uint64{origin("a"): 0, origin("b"): 10}},
		{name: "extension-overlap", bindings: []partImport{{Origin: origin("a"), BaseOrder: 1, Descriptor: part("a", 1, 10).Descriptor}, {Origin: origin("b"), BaseOrder: 11, Descriptor: part("b", 1, 10).Descriptor}}, incoming: []MovePart{part("a", 1, 11)}, failure: "overlap"},
		{name: "persisted-overlap", bindings: []partImport{{Origin: origin("a"), BaseOrder: 1, Descriptor: part("a", 1, 10).Descriptor}, {Origin: origin("b"), BaseOrder: 6, Descriptor: part("b", 1, 10).Descriptor}}, failure: "overlap"},
		{name: "exhaustion", incoming: []MovePart{part("a", 1, moveLiveOrder-1), part("b", 1, 1)}, failure: "exhausted"},
		{name: "persisted-exhaustion", bindings: []partImport{{Origin: origin("a"), BaseOrder: moveLiveOrder - 1, Descriptor: part("a", 1, 2).Descriptor}}, failure: "exhausted"},
		{name: "changed-offset", bindings: []partImport{{Origin: origin("a"), BaseOrder: 1, Descriptor: part("a", 1, 10).Descriptor}, {Origin: origin("a"), BaseOrder: 3, Descriptor: part("a", 2, 20).Descriptor}}, failure: "changed"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, err := pebble.Open(t.TempDir(), &pebble.Options{})
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			for i, b := range tc.bindings {
				value, err := json.Marshal(b)
				if err != nil {
					t.Fatal(err)
				}
				if err = db.Set(append(append([]byte{}, movePartPrefix...), byte(i)), value, pebble.Sync); err != nil {
					t.Fatal(err)
				}
			}
			got, err := moveOrderBases(db, tc.incoming)
			if tc.failure != "" {
				if err == nil || !strings.Contains(err.Error(), tc.failure) {
					t.Fatalf("want %s, got %v %v", tc.failure, got, err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			for o, want := range tc.want {
				if got[o] != want {
					t.Fatalf("%v: offset=%d want=%d", o, got[o], want)
				}
			}
			// Upto may reference a hole below the part's MinOrder: translate it
			// by the stream offset, including the nonzero imported second range.
			for _, p := range tc.incoming {
				o := origin(p.Descriptor.Source.Stream)
				r := sealrollup.Record{Delta: sealrollup.Delta{Order: p.Descriptor.Source.MinOrder}, Upto: sealrollup.Tick(1, 0)}
				b := partImport{Descriptor: p.Descriptor, BaseOrder: got[o] + p.Descriptor.Source.MinOrder}
				mapped, err := moveVersionRecord(r, &b)
				if err != nil || mapped.Upto != sealrollup.Tick(got[o]+1, 0) {
					t.Fatalf("Upto translation: %+v %v", mapped, err)
				}
			}
		})
	}
}

func TestMoveAuthorityLegacyVersions(t *testing.T) {
	for _, version := range []int{1, 2} {
		t.Run(string(rune('0'+version)), func(t *testing.T) {
			cfg := adoptConfig()
			cfg.DeferMerges = true
			root := t.TempDir()
			var expire atomic.Bool
			hooks := Hooks{Expire: func(RollupPolicy, RollupMetadata, time.Time) bool { return expire.Load() }, ExpiryTTL: expiryWindow(time.Nanosecond)}
			e, err := Open(root, cfg, hooks)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { e.Close() }()
			policy := adoptPolicy(t, sealrollup.Sum)
			adoptLive(t, e, policy, 100, 1, moveSample(1005, 6), moveSample(1006, 7), moveSample(1007, 8), moveSample(1008, 9), moveSample(1009, 10))
			donor := adoptDonor(t, policy, moveSample(1000, 1), moveSample(1001, 2), moveSample(1002, 3), moveSample(1003, 4), moveSample(1004, 5))
			prepared, err := PrepareMove(context.Background(), t.TempDir(), []MovePart{donor})
			if err != nil {
				t.Fatal(err)
			}
			defer prepared.Close()
			if err = e.AdoptMove(context.Background(), prepared, map[string]uint64{"move.metric": 7}, 1010); err != nil {
				t.Fatal(err)
			}
			value, closer, err := e.DBs()[0].Get(moveAuthorityKey("move.metric"))
			if err != nil {
				t.Fatal(err)
			}
			var a moveAuthority
			err = json.Unmarshal(value, &a)
			closer.Close()
			if err != nil {
				t.Fatal(err)
			}
			a.Version = version
			a.AtSeq = a.Meta.Frontier.Order // legacy authorities used input order
			a.Coarse = []sealrollup.Record{{Delta: sealrollup.Delta{Archive: 1, Slot: 4, Order: 1, Sample: moveSample(1000, 77)}}}
			a.Origins = []partOrigin{{Class: LivePart}}
			value, err = json.Marshal(a)
			if err != nil {
				t.Fatal(err)
			}
			if err = e.DBs()[0].Set(moveAuthorityKey("move.metric"), value, pebble.Sync); err != nil {
				t.Fatal(err)
			}
			check := func() {
				_, rs := adoptedRecords(t, e)
				for _, r := range rs {
					if r.Sample.Sequence != r.Order {
						t.Fatalf("decoded part sequence escaped the public scan: %+v", r)
					}
				}
				for _, r := range rs {
					if r.Archive == 1 && r.Sample.Time == 1000 && r.Sample.Bits == math.Float64bits(77) {
						return
					}
				}
				t.Fatalf("legacy authority value lost: %+v", rs)
			}
			check()
			if err = e.Close(); err != nil {
				t.Fatal(err)
			}
			e, err = Open(root, cfg, hooks)
			if err != nil {
				t.Fatal(err)
			}
			check()
			// A new sequence reset must fence an old order-based authority,
			// even when its input order is much larger than the part sequence.
			expire.Store(true)
			if err := e.AppendBatches(0, nil, 2, true); err != nil {
				t.Fatal(err)
			}
			expire.Store(false)
			adoptLive(t, e, policy, 101, 3, moveSample(1010, 11))
			meta, records := adoptedRecords(t, e)
			if meta.Version != 4 || meta.ResetOrder == 0 || meta.Expired {
				t.Fatalf("reset/revival metadata: %+v", meta)
			}
			for _, r := range records {
				if r.Sample.Time < 1010 {
					t.Fatalf("legacy authority survived reset: %+v", r)
				}
			}
		})
	}
}
