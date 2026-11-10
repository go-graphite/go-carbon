package whisperio

import (
	"context"
	"errors"
	"testing"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
)

func TestReferencedClassicOverwriteAndEmptyShadow(t *testing.T) {
	ctx := context.Background()
	clock := func() time.Time { return time.Unix(1794000000, 0) }
	m := store.Metadata{MetricConfig: store.MetricConfig{Name: "references", Retentions: []store.Retention{{Step: 60, Count: 512}}, AggregationMethod: store.Sum}}
	points := make([]store.Point, 512)
	for i := range points {
		points[i] = store.Point{Timestamp: 1793969280 + int64(i)*60, Value: float64(i)}
	}
	data, err := store.EncodeSealedPoints(points, 60)
	if err != nil {
		t.Fatal(err)
	}
	payload := &splicePayload{data: data, sealed: true}
	part := splicePart{fragment: spliceFragment{first: points[0].Timestamp, count: 512, payload: payload}, slot: int(points[0].Timestamp / 60 % 512), count: 512}
	class := store.NativeCombineClass{}
	codec := &ContinuationCodec{}
	keys, err := codec.writeReferencedArchives(ctx, m, [][]splicePart{{part}}, []int64{points[0].Timestamp}, &class, clock)
	if err != nil {
		t.Fatal(err)
	}
	kv := mapContinuationIO{}
	for _, item := range keys {
		kv[string(item.Key)] = item.Value
	}
	fs := newContinuationFiles(kv, false).withClock(clock)
	d, err := fs.load("main.wsp")
	if err != nil {
		t.Fatal(err)
	}
	f := &continuationFile{fs: fs, data: d}
	before, err := f.classicGroup(0, d.layout.Archives[0], 0, false)
	if err != nil || before.Points[0] != points[0] {
		t.Fatal(before, err)
	}
	if err := f.setPointBlock(continuationBodyKey(d.id, 'p', 0, 0, 0), &store.PointBlock{}); err != nil {
		t.Fatal(err)
	}
	after, err := f.classicGroup(0, d.layout.Archives[0], 0, false)
	if err != nil || after.Present != [2]uint64{} {
		t.Fatal("empty override resurrected history", after, err)
	}
	// Mutations buffered before flush must shadow the immutable reference too.
	buffer := &continuationPointBuffer{ContinuationIO: kv}
	fs.kv = buffer
	var changed store.PointBlock
	changed.Present[0] = 1
	changed.Points[0] = store.Point{Timestamp: points[0].Timestamp, Value: 999}
	if err := f.setPointBlock(continuationBodyKey(d.id, 'p', 0, 0, 1), &changed); err != nil {
		t.Fatal(err)
	}
	after, err = f.classicGroup(0, d.layout.Archives[0], 1, false)
	if err != nil || after.Points[0].Value != 999 {
		t.Fatal("unflushed write lost", after, err)
	}
	for _, index := range []int{2, 3} {
		if err := f.setPointBlock(continuationBodyKey(d.id, 'p', 0, 0, index), &store.PointBlock{}); err != nil {
			t.Fatal(err)
		}
	}
	for _, kind := range []byte{'r', 'R'} {
		if _, err := kv.Get(continuationBodyKey(d.id, kind, 0, 0, 1)); !errors.Is(err, store.ErrNotFound) {
			t.Fatal("last mapping retained payload", kind, err)
		}
	}
}

func TestPointReferencesRejectOverlapAndBounds(t *testing.T) {
	for _, refs := range [][]pointReference{
		{{ID: 1, Count: 128}, {ID: 1, Count: 1}},
		{{ID: 1, Count: 129}}, {{ID: 1, Source: 127, Count: 2}}, {{ID: 0, Count: 1}}, {{ID: 1, Sealed: true, Source: 8191, Count: 2}},
	} {
		if _, err := decodePointReferences(encodePointReferences(refs)); err == nil {
			t.Fatal("invalid references accepted", refs)
		}
	}
}
