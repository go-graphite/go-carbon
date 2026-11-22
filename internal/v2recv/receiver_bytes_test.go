package v2recv

import (
	"fmt"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/internal/namefp"
	"github.com/go-graphite/go-carbon/points"
)

type recordingByteOwner struct {
	coalescingOwner
	byteCalls int
	sequence  []uint64
}

func (o *recordingByteOwner) Append(names []string, hashes []uint64, data []points.Point, sequence []uint64, clock int64) error {
	o.sequence = append(o.sequence, sequence...)
	return o.coalescingOwner.Append(names, hashes, data, sequence, clock)
}

func (o *recordingByteOwner) AppendBytes(text []byte, ends []int, hashes []uint64, data []points.Point, sequence []uint64, clock int64) error {
	o.byteCalls++
	names := make([]string, len(ends))
	start := 0
	for i, end := range ends {
		names[i] = string(text[start:end])
		start = end
	}
	return o.Append(names, hashes, data, sequence, clock)
}

// Fragments cross inbox boundaries and alternate with codec names. Both
// layouts share a byte owner's inbox; the copy must preserve names and hashes.
func TestOwnerByteFragmentsAndCodec(t *testing.T) {
	o := &recordingByteOwner{}
	s := ownerOnly(o, Config{BatchSize: 8, SyncInterval: time.Hour})
	index := 0
	for k, n := range []int{3, 2, 4, 8, 1} {
		p := &batch{}
		for range n {
			name := fmt.Sprintf("metric.%d.%s", index, string(make([]byte, index%5+1)))
			if k%2 == 0 {
				p.text = append(p.text, name...)
				p.ends = append(p.ends, len(p.text))
			} else {
				p.names = append(p.names, name)
			}
			p.hashes = append(p.hashes, namefp.Route(name))
			p.data = append(p.data, points.Point{Timestamp: int64(index), Value: float64(index) + .125})
			index++
		}
		if err := s.handOff(0, p); err != nil {
			t.Fatal(err)
		}
		clear(p.text[:cap(p.text)]) // the inbox holds a copy
	}
	close(s.closing)
	s.workers.Wait()
	// Hand-offs of 3, 2 (codec), 4, 8 (codec) and 1 points: the inbox is
	// taken whole once it reaches 8, one call each: 9, 8, and 1 at close.
	if o.byteCalls != 3 {
		t.Fatalf("byte calls=%d, want 3", o.byteCalls)
	}
	if len(o.names) != index || s.Stats().Accepted != uint64(index) {
		t.Fatal("lost points", len(o.names), s.Stats())
	}
	for i, name := range o.names {
		if o.sequence[i] != uint64(i+1) {
			t.Fatalf("sequence[%d]=%d", i, o.sequence[i])
		}
		want := fmt.Sprintf("metric.%d.%s", i, string(make([]byte, i%5+1)))
		if name != want || o.hashes[i] != namefp.Route(want) || o.data[i] != (points.Point{Timestamp: int64(i), Value: float64(i) + .125}) {
			t.Fatalf("point %d: %q / %q", i, name, want)
		}
	}
}
