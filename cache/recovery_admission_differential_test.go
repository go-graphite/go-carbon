package cache

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"math"
	"sort"
	"testing"

	"github.com/go-graphite/go-carbon/points"
)

// This drives the real cache admission/add/pop path across multiple capacity
// boundaries. The old-source overlay runs the identical trace with legacy
// admission; only drain timing and batch boundaries may differ.
func TestRecoveryAdmissionBitDifferential(t *testing.T) {
	c := New()
	c.SetMaxSize(20000)
	configureRecoveryOracle(c)
	names := make([]string, 512)
	for i := range names {
		names[i] = fmt.Sprintf("recovery.bits.%04d", i)
	}
	got, want := map[string][]points.Point{}, map[string][]points.Point{}
	drain := func() {
		for _, name := range names {
			if batch, ok := c.Pop(name); ok {
				got[name] = append(got[name], batch.Data...)
			}
		}
	}
	bits := []uint64{0, 1 << 63, 0x7ff8000000000042, 0x7ff0000000000000, 0xfff0000000000000, 1, 0x7fefffffffffffff}
	count, waits := 0, 0
	for round := range 64 {
		for i, name := range names {
			batch := &points.Points{Metric: name}
			for j := range 8 {
				value := math.Float64frombits(bits[(round+i+j)%len(bits)])
				if j == 0 {
					value = math.Float64frombits(uint64((round*512+i)*8+j) * 0x9e3779b97f4a7c15)
				}
				batch.Data = append(batch.Data, points.Point{Value: value, Timestamp: int64(1700000000 + round - j%3)})
			}
			want[name] = append(want[name], batch.Data...)
			count += len(batch.Data)
			if !recoveryOracleAdmit(c, name, len(batch.Data)) {
				waits++
				drain()
				if !recoveryOracleAdmit(c, name, len(batch.Data)) {
					t.Fatal("recovery cannot progress after draining")
				}
			}
			c.AddRecovered(batch)
		}
	}
	drain()
	digest := func(values map[string][]points.Point) string {
		keys := make([]string, 0, len(values))
		for key := range values {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		h := sha256.New()
		var buf [16]byte
		for _, name := range keys {
			fmt.Fprintf(h, "%d:%s:%d:", len(name), name, len(values[name]))
			for _, p := range values[name] {
				binary.LittleEndian.PutUint64(buf[:8], uint64(p.Timestamp))
				binary.LittleEndian.PutUint64(buf[8:], math.Float64bits(p.Value))
				h.Write(buf[:])
			}
		}
		return fmt.Sprintf("%x", h.Sum(nil))
	}
	if digest(got) != digest(want) {
		t.Fatal("points/routes/order differ from input", digest(got), digest(want))
	}
	t.Logf("points=%d routes=%d digest=%s capacity_drains=%d", count, len(names), digest(got), waits)
}
