package v2backend

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"reflect"
	"testing"

	"github.com/go-graphite/go-carbon/internal/daemonstore"
	"github.com/go-graphite/go-carbon/internal/partsformat"
	"github.com/go-graphite/go-carbon/internal/sealrollup"
)

func TestSamePolicyMatchesLegacyComparison(t *testing.T) {
	base := daemonstore.Metric{Archives: []daemonstore.Archive{{Step: 1, Points: 60}}, AggregationMethod: "average"}
	for _, xff := range []float32{0, float32(math.Copysign(0, -1)), .5, 1} {
		for _, other := range []float32{0, float32(math.Copysign(0, -1)), .5, 1} {
			a, b := base, base
			a.XFilesFactor, b.XFilesFactor = xff, other
			if got, want := samePolicy(a, b), reflect.DeepEqual(a, b); got != want {
				t.Fatalf("policy equality differs for %v, %v", xff, other)
			}
		}
	}
}

func TestSealScratchGroupingPreservesOriginalApplyOrder(t *testing.T) {
	var s sealScratch
	for run := 0; run < 4; run++ {
		fs := make([]frame, 8193-run*197)
		want := make(map[string][]frame)
		for i := range fs {
			fs[i] = frame{name: fmt.Sprintf("metric.%d", (i*17)%131), order: uint64(i + 1), clock: int64(i % 7)}
			want[fs[i].name] = append(want[fs[i].name], fs[i])
		}
		got := s.group(fs)
		if len(got) != len(want) {
			t.Fatal("group count")
		}
		for name, span := range got {
			if !reflect.DeepEqual(s.frames[span.start:span.end], want[name]) {
				t.Fatalf("apply order changed: %s", name)
			}
		}
		s.reset()
		for _, f := range s.frames {
			if f.name != "" || f.points != nil {
				t.Fatal("scratch retains input")
			}
		}
	}
}
func TestSealArenaPortableRowsMatchCodecs(t *testing.T) {
	var a sealBytes
	var saved [][]byte
	var expected [][]byte
	for i := 0; i < 5000; i++ {
		name := fmt.Sprintf("namespace.host.%d.metric.\xff", i)
		base := partsformat.Key{Group: "placement", Metric: partsformat.ID(name), VShard: partsformat.Route(partsformat.ID(name)), Partition: int64(i) - 2500}
		binary.BigEndian.PutUint64(base.Part[:], uint64(i+1))
		template, e := partsformat.Encode(base)
		if e != nil {
			t.Fatal(e)
		}
		for archive := 0; archive < 4; archive++ {
			key := a.copy(template)
			tail := key[len(key)-16:]
			binary.BigEndian.PutUint32(tail, uint32(archive))
			binary.BigEndian.PutUint64(tail[4:], uint64(int64(i)-2500)^(1<<63))
			base.Archive = uint32(archive)
			base.BlockStart = int64(i) - 2500
			want, e := partsformat.Encode(base)
			if e != nil || !bytes.Equal(key, want) {
				t.Fatal("key codec differs", e)
			}
			record := sealrollup.Record{Correction: i%2 == 0}
			record.Sample.Time = int64(i + 1)
			record.Sample.Bits = math.Float64bits(math.Copysign(float64(i), -1))
			record.Order = uint64(i + 1)
			value := a.copy(sealrollup.EncodeRecord(record))
			saved = append(saved, key, value)
			expected = append(expected, want, sealrollup.EncodeRecord(record))
		}
	}
	for i := range saved {
		if !bytes.Equal(saved[i], expected[i]) {
			t.Fatal("growth invalidated prior bytes", i)
		}
	}
	a.reset()
	if allocs := testing.AllocsPerRun(100, func() {
		a.reset()
		for i := 0; i < 10000; i++ {
			a.copy(expected[i%len(expected)])
		}
	}); allocs != 0 {
		t.Fatalf("warm arena allocs=%g", allocs)
	}
}
func BenchmarkSealGrouping(b *testing.B) {
	fs := make([]frame, 16384)
	for i := range fs {
		fs[i] = frame{name: fmt.Sprintf("metric.%d", i%4096), order: uint64(i + 1)}
	}
	b.Run("previous", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			m := make(map[string][]frame)
			for _, f := range fs {
				m[f.name] = append(m[f.name], f)
			}
		}
	})
	b.Run("reused", func(b *testing.B) {
		var s sealScratch
		s.group(fs)
		s.reset()
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			s.group(fs)
			s.reset()
		}
	})
}
