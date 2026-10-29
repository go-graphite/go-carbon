package inputlog

import (
	"bytes"
	"fmt"
	"math"
	"slices"
	"testing"

	"github.com/go-graphite/go-carbon/points"
)

// encodeStream encodes batches of entries as one partition's frames in
// segment seq, flushing after each batch as a cycle does.
func encodeStream(enc *streamEncoder, seq uint64, batches ...[]Entry) [][]byte {
	var w streamScratch
	var frames [][]byte
	enc.begin(seq)
	for _, batch := range batches {
		for _, e := range batch {
			enc.add(&w, e)
		}
		enc.flush(&w, nil, func(raw []byte, rows int) []byte {
			frames = append(frames, slices.Clone(raw))
			return raw
		})
	}
	return frames
}

// decodeStream decodes frames in order, collecting each metric's points.
func decodeStream(t *testing.T, d *streamDecoder, seq uint64, frames [][]byte) map[string][]points.Point {
	t.Helper()
	got := map[string][]points.Point{}
	d.begin(seq)
	for i, raw := range frames {
		if _, err := d.decode(raw, func(p *points.Points) { got[p.Metric] = append(got[p.Metric], p.Data...) }); err != nil {
			t.Fatalf("frame %d: %v", i, err)
		}
	}
	return got
}

func wantPoints(batches ...[]Entry) map[string][]points.Point {
	want := map[string][]points.Point{}
	for _, batch := range batches {
		for _, e := range batch {
			want[e.Metric()] = append(want[e.Metric()], points.Point{Value: e.Value, Timestamp: e.Timestamp})
		}
	}
	return want
}

func equalPoints(t *testing.T, got, want map[string][]points.Point) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("decoded %d metrics, want %d", len(got), len(want))
	}
	for metric, w := range want {
		g := got[metric]
		if len(g) != len(w) {
			t.Fatalf("%s: decoded %d points, want %d", metric, len(g), len(w))
		}
		for i := range w {
			// Compare bits: NaN must survive too.
			if g[i].Timestamp != w[i].Timestamp || math.Float64bits(g[i].Value) != math.Float64bits(w[i].Value) {
				t.Fatalf("%s point %d: decoded %v, want %v", metric, i, g[i], w[i])
			}
		}
	}
}

// Names first seen in later cycles, repeated points of one name in a frame,
// out-of-order arrival and awkward values all round-trip, over many frames.
func TestStreamRoundTrip(t *testing.T) {
	var batches [][]Entry
	special := []float64{0, math.Copysign(0, -1), math.NaN(), math.Inf(1), math.Inf(-1), math.MaxFloat64, math.SmallestNonzeroFloat64, -1.5}
	for cycle := range 6 {
		var batch []Entry
		// Each cycle sends the metrics seen before, in reverse, and new ones.
		for i := (cycle + 1) * 3000; i > 0; i-- {
			metric := fmt.Sprintf("servers.host-%05d.%s.value", i, string(rune('a'+i%26)))
			value := float64(cycle*7 + i)
			if i%10 == 0 {
				value = special[(i/10)%len(special)]
			}
			batch = append(batch, NewStreamRef(metric).Entry(value, int64(1700000000+60*cycle-i%3)))
			if i%500 == 0 { // a second point of the same metric, older
				batch = append(batch, NewStreamRef(metric).Entry(value+1, -int64(i)))
			}
		}
		batches = append(batches, batch)
	}
	// A batch larger than one frame.
	var big []Entry
	for i := range 3 * maxFrameRaw / maxRowBytes {
		big = append(big, NewStreamRef(fmt.Sprintf("big.%d", i%70000)).Entry(float64(i), int64(i)))
	}
	batches = append(batches, big)

	var enc streamEncoder
	frames := encodeStream(&enc, 1, batches...)
	if len(frames) < len(batches)+2 {
		t.Fatalf("%d frames for %d batches, want the large one split", len(frames), len(batches))
	}
	for i, raw := range frames {
		if len(raw) > maxFrameRaw+maxRowBytes+256 {
			t.Fatalf("frame %d holds %d bytes", i, len(raw))
		}
	}
	var d streamDecoder
	equalPoints(t, decodeStream(t, &d, 1, frames), wantPoints(batches...))

	// A new segment starts a new dictionary, on both sides.
	next := []Entry{NewStreamRef("big.5").Entry(1, 2), NewStreamRef("fresh").Entry(3, 4)}
	frames = encodeStream(&enc, 2, next)
	equalPoints(t, decodeStream(t, &d, 2, frames), wantPoints(next))
}

// A stream decodes only in order from its first frame: a frame that does not
// continue it, or does not verify, is rejected without being applied.
func TestStreamRejectsGapsAndDamage(t *testing.T) {
	cycle := func(c int) []Entry {
		var batch []Entry
		for i := range 50 + 10*c {
			batch = append(batch, NewStreamRef(fmt.Sprintf("metric.%d", i)).Entry(float64(c*i), int64(c)))
		}
		return batch
	}
	var enc streamEncoder
	frames := encodeStream(&enc, 1, cycle(0), cycle(1), cycle(2))
	if len(frames) != 3 {
		t.Fatalf("%d frames", len(frames))
	}
	applied := 0
	visit := func(*points.Points) { applied++ }

	var d streamDecoder
	d.begin(1)
	if _, err := d.decode(frames[1], visit); err == nil {
		t.Fatal("a stream's second frame decoded without its first")
	}
	d = streamDecoder{}
	d.begin(1)
	if _, err := d.decode(frames[0], nil); err != nil {
		t.Fatal(err)
	}
	if _, err := d.decode(frames[2], visit); err == nil {
		t.Fatal("a frame decoded after a missing one")
	}
	for n := range len(frames[0]) {
		d = streamDecoder{}
		d.begin(1)
		if _, err := d.decode(frames[0][:n], visit); err == nil {
			t.Fatalf("frame truncated to %d bytes decoded", n)
		}
	}
	if applied != 0 {
		t.Fatalf("rejected frames applied %d metrics", applied)
	}
}

// Entries carrying their ref's stream id replay exactly like entries looked
// up by name, across segment rolls, and across log instances whose segment
// numbers restart: a ref's id is used only in the stream that assigned it.
func TestStreamRefsAcrossSegmentsAndLogs(t *testing.T) {
	dir := t.TempDir()
	refs := map[string]*StreamRef{}
	opts := Options{SegmentSize: 16 << 10}
	want := recorded{}
	run := func(l *Log, src *fakeSource, cycles, offset int) {
		for c := range cycles {
			for i := range 300 {
				// Metrics come and go, so later streams define names in
				// another order than earlier ones.
				m := fmt.Sprintf("metric.%d", (i*7+c*13+offset)%500)
				v, ts := float64(c*1000+i), int64(offset+c)
				src.add(m, v, ts)
				want.add(m, v, ts)
			}
			if err := l.Sync(); err != nil {
				t.Fatal(err)
			}
		}
	}
	src := newFakeSource()
	src.refs = refs
	l := openStarted(t, dir, opts, src)
	run(l, src, 40, 0)
	if files(t, dir, logSuffix) < 2 {
		t.Fatal("the test needs several segments")
	}
	l.Kill()
	l2, got, _ := reopen(t, dir, opts, newFakeSource())
	equalRecorded(t, got, want)
	if err := l2.Retire(); err != nil {
		t.Fatal(err)
	}

	// A new log numbers segments from one again; the refs keep their ids.
	want = recorded{}
	src = newFakeSource()
	src.refs = refs
	l3 := openStarted(t, dir, opts, src)
	run(l3, src, 10, 7)
	l3.Kill()
	l4, got, _ := reopen(t, dir, opts, newFakeSource())
	defer l4.Close()
	equalRecorded(t, got, want)
}

// A cycle takes its entries before the encoder assigns stream ids. Repeated
// points of a metric therefore carry the same old cached id within the cycle.
func BenchmarkStreamCycle(b *testing.B) {
	const metrics, perMetric = 4096, 32
	var enc streamEncoder
	enc.begin(1)
	var w streamScratch
	refs := make([]*StreamRef, metrics)
	for i := range refs {
		refs[i] = NewStreamRef(fmt.Sprintf("services.namespace.host-%08d.component.requests.region.zone.long-metric-name.value", i))
	}
	entries := make([]Entry, 0, metrics*perMetric)
	for range perMetric {
		for _, ref := range refs {
			entries = append(entries, ref.Entry(1, 1790000000))
		}
	}
	var raw []byte
	emit := func(buf []byte, _ int) []byte { return buf }
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, e := range entries {
			enc.add(&w, e)
		}
		raw = enc.flush(&w, raw, emit)
	}
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*len(entries)), "ns/point")
}

func TestStreamStaleSnapshotsMatchDictionary(t *testing.T) {
	var fast, reference streamEncoder
	var a, b streamScratch
	var ar, br []byte
	refs := make([]*StreamRef, 128)
	for cycle := range 6 {
		seq := uint64(1 + cycle/3)
		fast.begin(seq)
		reference.begin(seq)
		var entries []Entry
		for i := range refs {
			if refs[i] == nil || cycle%2 == 0 {
				refs[i] = NewStreamRef(fmt.Sprintf("metric.%d", i))
			}
		}
		for round := range 32 {
			for i, ref := range refs {
				entries = append(entries, ref.Entry(float64(cycle+i+round), int64(cycle*100+round)))
			}
		}
		// Leave snapshots stale while the live ref is assigned within this cycle.
		for _, e := range entries {
			fast.add(&a, e)
		}
		for _, e := range entries {
			id := reference.lookup(&b, e.Ref)
			b.rows = append(b.rows, row{id: id, ts: e.Timestamp, bits: math.Float64bits(e.Value)})
		}
		var got, want [][]byte
		ar = fast.flush(&a, ar, func(raw []byte, _ int) []byte { got = append(got, bytes.Clone(raw)); return raw })
		br = reference.flush(&b, br, func(raw []byte, _ int) []byte { want = append(want, bytes.Clone(raw)); return raw })
		if len(got) != len(want) {
			t.Fatal("frame counts differ")
		}
		for i := range got {
			if !bytes.Equal(got[i], want[i]) {
				t.Fatalf("cycle %d frame %d differs from dictionary encoding", cycle, i)
			}
		}
	}
}
