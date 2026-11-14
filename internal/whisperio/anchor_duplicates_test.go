package whisperio

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"slices"
	"testing"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
)

func addAnchorCopy(t *testing.T, kv mapContinuationIO, slot int, p store.Point) {
	t.Helper()
	for key, value := range kv {
		if len(key) != 20 || key[9] != 'p' {
			continue
		}
		var block store.PointBlock
		if err := store.DecodePointBlock(value, &block); err != nil {
			t.Fatal(err)
		}
		if block.Present[slot/64]&(uint64(1)<<uint(slot%64)) != 0 {
			t.Fatal("fixture slot already occupied")
		}
		block.Present[slot/64] |= uint64(1) << uint(slot%64)
		block.Points[slot] = p
		kv[key] = store.EncodePointBlock(&block)
		return
	}
	t.Fatal("fixture has no classic block")
}

func anchorRawHeader(t *testing.T, kv mapContinuationIO) []byte {
	t.Helper()
	fs := newContinuationFiles(kv, false)
	file, err := fs.OpenFile(fs.main(), os.O_RDONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	f := file.(*continuationFile)
	header := make([]byte, 16+12*len(f.data.layout.Archives))
	if _, err := f.ReadAt(header, 0); err != nil {
		t.Fatal(err)
	}
	return header
}

func assertAnchorRepairMultisetAndFetch(t *testing.T, kv mapContinuationIO, m store.Metadata, now time.Time, duplicates int) {
	t.Helper()
	ctx := context.Background()
	c := &ContinuationCodec{}
	clock := func() time.Time { return now }
	header := anchorRawHeader(t, kv)
	before, err := c.Snapshot(ctx, kv, m, clock)
	if err != nil {
		t.Fatal(err)
	}
	original := make(map[string][]byte, len(kv))
	for k, v := range kv {
		original[k] = bytes.Clone(v)
	}
	plan, err := c.PlanArchiveAnchors(ctx, kv)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Archives) != 1 || plan.Archives[0].Blocked != "" || !plan.Archives[0].Changed || plan.Archives[0].DuplicatePoints != duplicates {
		t.Fatalf("unsafe/unqualified plan: %+v", plan.Archives)
	}
	for k, v := range original {
		if !bytes.Equal(v, kv[k]) {
			t.Fatal("dry-run changed input")
		}
	}
	applyAnchorPlan(t, kv, plan)
	if !bytes.Equal(header, anchorRawHeader(t, kv)) {
		t.Fatal("raw policy/header bytes changed")
	}
	after, err := c.Snapshot(ctx, kv, m, clock)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.EqualSnapshots(before, after); err != nil {
		t.Fatal("complete physical multiset changed:", err)
	}
	for _, archive := range before.Archives {
		for _, p := range archive.Points {
			got, err := c.Fetch(ctx, kv, m, int(p.Timestamp)-archive.Retention.Step, int(p.Timestamp), clock)
			if err != nil {
				t.Fatal(err)
			}
			if len(got.Values) != 1 || math.Float64bits(got.Values[0]) != math.Float64bits(p.Value) {
				t.Fatalf("timestamp %d got %+v want bits %016x", p.Timestamp, got, math.Float64bits(p.Value))
			}
		}
	}
	again, err := c.PlanArchiveAnchors(ctx, kv)
	if err != nil {
		t.Fatal(err)
	}
	if len(again.Writes)+len(again.Deletes) != 0 || again.Archives[0].Changed || again.Archives[0].Misplaced != 0 || again.Archives[0].DuplicatePoints != duplicates {
		t.Fatalf("not idempotent: %+v", again.Archives)
	}
}

func TestAnchorRepairPreservesIdenticalPhysicalCopiesAndEveryIEEEBit(t *testing.T) {
	points := []store.Point{{Timestamp: 999780, Value: math.Copysign(0, -1)}, {Timestamp: 999810, Value: math.Float64frombits(0x7ff8000000000042)}, {Timestamp: 999990, Value: math.Inf(-1)}}
	kv, snapshot := absoluteAnchorFixture(t, points)
	for i, p := range []store.Point{points[0], points[0], points[1], points[2]} {
		addAnchorCopy(t, kv, i+1, p)
	}
	assertAnchorRepairMultisetAndFetch(t, kv, snapshot.Metadata, time.Unix(1000000, 0), 4)
}

func TestAnchorRepairStillBlocksConflictingDuplicateBitsAndDistinctTimestampAliases(t *testing.T) {
	for _, bad := range []store.Point{{Timestamp: 999780, Value: math.Copysign(0, -1)}, {Timestamp: 999780, Value: math.Float64frombits(0x7ff8000000000042)}, {Timestamp: 1000380, Value: 0}} {
		kv, _ := absoluteAnchorFixture(t, []store.Point{{Timestamp: 999780, Value: 0}, {Timestamp: 999990, Value: 2}})
		addAnchorCopy(t, kv, 5, bad)
		plan, err := (&ContinuationCodec{}).PlanArchiveAnchors(context.Background(), kv)
		if err != nil {
			t.Fatal(err)
		}
		if len(plan.Archives) == 0 || plan.Archives[0].Blocked == "" || len(plan.Writes)+len(plan.Deletes) != 0 {
			t.Fatalf("conflicting layout admitted: %+v", plan)
		}
	}
}

func TestAnchorRepairExactCapturedDuplicateLayouts(t *testing.T) {
	raw, err := os.ReadFile("testdata/anchor-duplicate-layouts.json")
	if err != nil {
		t.Fatal(err)
	}
	var fixtures []struct {
		Label      string `json:"label"`
		Wire       string `json:"wire_base64"`
		Duplicates int    `json:"duplicates"`
	}
	if err := json.Unmarshal(raw, &fixtures); err != nil {
		t.Fatal(err)
	}
	for _, fixture := range fixtures {
		t.Run(fixture.Label, func(t *testing.T) {
			wire, err := base64.StdEncoding.DecodeString(fixture.Wire)
			if err != nil {
				t.Fatal(err)
			}
			records, err := store.ReadTransfer(bytes.NewReader(wire), 64<<20)
			if err != nil {
				t.Fatal(err)
			}
			if len(records) != 1 {
				t.Fatalf("records=%d", len(records))
			}
			r := records[0]
			kv := mapContinuationIO{}
			for _, pair := range r.Keys {
				kv[string(pair.Key)] = bytes.Clone(pair.Value)
			}
			m := store.Metadata{MetricConfig: r.Config, ID: r.Version.ID, Generation: r.Version.Generation, Revision: r.Version.Revision, Continuation: r.Continuation}
			snap, err := (&ContinuationCodec{}).Snapshot(context.Background(), kv, m, time.Now)
			if err != nil {
				t.Fatal(err)
			}
			var timestamps []int64
			for _, a := range snap.Archives {
				for _, p := range a.Points {
					timestamps = append(timestamps, p.Timestamp)
				}
			}
			if len(timestamps) == 0 {
				t.Fatal("empty fixture")
			}
			now := time.Unix(slices.Max(timestamps)+int64(m.Retentions[0].Step), 0)
			assertAnchorRepairMultisetAndFetch(t, kv, m, now, fixture.Duplicates)
			t.Log(fmt.Sprintf("physical points=%d duplicate extras=%d", len(timestamps), fixture.Duplicates))
		})
	}
}
