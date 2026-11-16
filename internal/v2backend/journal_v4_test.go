package v2backend

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/points"
)

// Writer switches keep the journal suffix and reconstruct each codec epoch.
func TestCatalogMixedBackendReopen(t *testing.T) {
	const clock = int64(1794700800)
	opts := testOptions(t.TempDir(), simplePolicy())
	codecs := []string{"pdx1", "pdc3", "pdc4", "pdx1", "pdc4"}
	for epoch, codec := range codecs {
		opts.JournalCodec = codec
		s, e := openAt(opts, func() time.Time { return time.Unix(clock, 0) })
		if e != nil {
			t.Fatal(epoch, e)
		}
		for i := 0; i < epoch; i++ {
			got, e := s.Fetch(context.Background(), "mixed.binary.\xff", int(clock-301), int(clock))
			if e != nil {
				t.Fatal(e)
			}
			want := uint64(0x3ff0000000000000) + uint64(i)
			if i == 0 {
				want = 0x8000000000000000
			}
			if math.Float64bits(got.Values[i]) != want {
				t.Fatalf("epoch%d point%d got%x want%x", epoch, i, math.Float64bits(got.Values[i]), want)
			}
			got.Release()
		}
		bits := uint64(0x3ff0000000000000) + uint64(epoch)
		if epoch == 0 {
			bits = 0x8000000000000000
		}
		if e = s.Append(&points.Points{Metric: "mixed.binary.\xff", Data: []points.Point{{Timestamp: clock - 300 + int64(epoch), Value: math.Float64frombits(bits)}}}); e != nil {
			t.Fatal(e)
		}
		requireClose(t, s)
	}
}
