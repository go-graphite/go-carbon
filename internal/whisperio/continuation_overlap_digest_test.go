package whisperio

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"io"
	"math"
	"math/rand"
	"os"
	"sort"
	"testing"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	whisper "github.com/go-graphite/go-whisper"
)

// This test uses only APIs that predate the overlap replay changes: run
//
//	OVERLAP_DIGEST=1 go test -run TestContinuationOverlapDigest -v
//
// on the old and new code and compare the digests. Each digest covers the
// main file and sidecar bytes as the continuation store presents them, and
// every archive's fetch, after every update of a realistic emitter.

type digestEmitter struct {
	cadence, jitter, skew int
	dupRate, lateRate     float64
}

type digestArrival struct {
	at int
	p  store.Point
}

func (e digestEmitter) batches(rng *rand.Rand, start int) (flushAt []int, batches [][]store.Point) {
	const duration = 86400
	var arrivals []digestArrival
	for ts := start + rng.Intn(e.cadence); ts < start+duration; ts += e.cadence {
		stamp := ts + e.skew
		if e.jitter > 0 {
			stamp += rng.Intn(2*e.jitter+1) - e.jitter
		}
		p := store.Point{Timestamp: int64(stamp), Value: float64(rng.Intn(1000))}
		delay := rng.Intn(5)
		if rng.Float64() < e.lateRate {
			delay = 120 + rng.Intn(1680)
		}
		arrivals = append(arrivals, digestArrival{ts + delay, p})
		if rng.Float64() < e.dupRate {
			arrivals = append(arrivals, digestArrival{ts + delay + 1 + rng.Intn(600), p})
		}
	}
	sort.SliceStable(arrivals, func(i, j int) bool { return arrivals[i].at < arrivals[j].at })
	var cur []store.Point
	first := 0
	for _, a := range arrivals {
		if len(cur) > 0 && a.at-first >= 960 {
			flushAt, batches, cur = append(flushAt, first+960), append(batches, cur), nil
		}
		if len(cur) == 0 {
			first = a.at
		}
		if cur = append(cur, a.p); len(cur) >= 960 {
			flushAt, batches, cur = append(flushAt, a.at), append(batches, cur), nil
		}
	}
	return flushAt, batches
}

func continuationDigestState(t testing.TB, codec *ContinuationCodec, kv mapContinuationIO, m store.Metadata, now int) []byte {
	t.Helper()
	clock := func() time.Time { return time.Unix(int64(now), 0) }
	fs := newContinuationFiles(kv, true).withClock(clock)
	var out []byte
	read := func(path string) []byte {
		info, err := fs.Stat(path)
		if err != nil {
			t.Fatal(err)
		}
		f, err := fs.OpenFile(path, os.O_RDONLY, 0)
		if err != nil {
			t.Fatal(err)
		}
		defer f.Close()
		data := make([]byte, info.Size())
		if _, err := f.ReadAt(data, 0); err != nil && !errors.Is(err, io.EOF) {
			t.Fatal(err)
		}
		return data
	}
	out = append(out, read(fs.main())...)
	w, err := whisper.OpenWithOptions(fs.main(), continuationOptions(fs, clock))
	if err != nil {
		t.Fatal(err)
	}
	if path := w.OutOfOrderPath(); path != "" {
		out = append(out, "sidecar"...)
		out = append(out, read(path)...)
	}
	w.Close()
	for _, r := range m.Retentions {
		series, err := codec.Fetch(context.Background(), kv, m, now-r.Step*r.Count+r.Step, now, clock)
		if err != nil {
			t.Fatal(err)
		}
		out = binary.LittleEndian.AppendUint64(out, uint64(series.FromTime))
		for _, v := range series.Values {
			out = binary.LittleEndian.AppendUint64(out, math.Float64bits(v))
		}
	}
	return out
}

// digestBatchHook, when set, runs around every emitter batch update.
var digestBatchHook func(before bool, kv mapContinuationIO, now int)

func TestContinuationOverlapDigest(t *testing.T) {
	if os.Getenv("OVERLAP_DIGEST") == "" && digestBatchHook == nil {
		t.Skip("set OVERLAP_DIGEST=1 to print old/new comparable digests")
	}
	schemas := [][]store.Retention{
		{{Step: 60, Count: 43200}, {Step: 3600, Count: 8760}, {Step: 86400, Count: 3650}},
		{{Step: 60, Count: 20160}, {Step: 1800, Count: 35040}},
		{{Step: 10, Count: 2160}, {Step: 60, Count: 1440}, {Step: 600, Count: 1008}},
		{{Step: 1, Count: 600}, {Step: 60, Count: 60}, {Step: 600, Count: 144}},
		{{Step: 300, Count: 105120}},
	}
	emitters := []digestEmitter{{cadence: 10}, {cadence: 10, jitter: 3, dupRate: .02, lateRate: .01}, {cadence: 60, skew: 30}, {cadence: 7, jitter: 2, dupRate: .05, lateRate: .02, skew: 2}}
	methods := []store.AggregationMethod{store.Average, store.Sum, store.Max, store.Last}
	for si, rets := range schemas {
		for ei, e := range emitters {
			ctx := context.Background()
			codec := &ContinuationCodec{ReuseBlockKeys: true}
			kv := mapContinuationIO{}
			now := 1_700_000_000
			clock := func() time.Time { return time.Unix(int64(now), 0) }
			m := store.Metadata{MetricConfig: store.MetricConfig{Name: "digest", Retentions: rets, AggregationMethod: methods[(si+ei)%len(methods)], XFilesFactor: []float32{0, .5}[(si+ei)%2]}}
			if err := codec.Create(ctx, kv, m, clock); err != nil {
				t.Fatal(err)
			}
			rng := rand.New(rand.NewSource(int64(si*10 + ei)))
			maintenance := &store.ContinuationMaintenance{MinBytes: 65536, MaxPointAge: 5 * time.Minute, RetentionMargin: time.Minute,
				Allow: func() bool { return rng.Intn(4) == 0 }}
			update := func(at int, pts []store.Point) {
				now = at
				if err := codec.Update(ctx, kv, m, pts, -1, clock, maintenance); err != nil {
					t.Fatal(err)
				}
			}
			start := now
			step := rets[0].Step
			history := step*rets[0].Count + 2*step
			if history > 35*86400 {
				history = 35 * 86400
			}
			chunk := 86400
			if step*rets[0].Count < chunk {
				chunk = step * rets[0].Count / 3
			}
			from := start - history
			for ts := from - from%step; ts < start; ts += chunk {
				var pts []store.Point
				for x := ts; x < ts+chunk && x < start; x += step {
					pts = append(pts, store.Point{Timestamp: int64(x), Value: float64(x % 997)})
				}
				update(min(ts+chunk, start), pts)
			}
			flushAt, batches := e.batches(rng, start)
			h := sha256.New()
			for i, b := range batches {
				if digestBatchHook != nil {
					digestBatchHook(true, kv, flushAt[i])
				}
				update(flushAt[i], b)
				if digestBatchHook != nil {
					digestBatchHook(false, kv, now)
				}
				h.Write(continuationDigestState(t, codec, kv, m, now))
			}
			t.Logf("digest schema=%d emitter=%v steps=%d %x", si, e, len(batches), h.Sum(nil)[:12])
		}
	}
}
