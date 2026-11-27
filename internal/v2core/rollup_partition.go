package v2core

import (
	"math"
	"time"

	"github.com/go-graphite/go-carbon/internal/sealrollup"
)

// Keep source samples through the last possible coarse window that can use
// them. This is deliberately conservative at the archive retention boundary.
func rollupRecordExpiry(c sealrollup.Config, r sealrollup.Record) int64 {
	a := c.Archives[r.Archive]
	extra := int64(0)
	if r.Archive+1 < len(c.Archives) {
		extra = c.Archives[r.Archive+1].Step
	}
	return saturatingAdd(r.Sample.Time, a.Step*a.Count+extra)
}

// A metric envelope outlives every record it can mask: no record at or before
// the frontier's newest sample can expire later than this.
func rollupMetricExpiry(c sealrollup.Config, maxTime int64) int64 {
	var retention int64
	for i, a := range c.Archives {
		extra := int64(0)
		if i+1 < len(c.Archives) {
			extra = c.Archives[i+1].Step
		}
		retention = max(retention, a.Step*a.Count+extra)
	}
	return saturatingAdd(maxTime, retention)
}

func saturatingAdd(t, d int64) int64 {
	if t > math.MaxInt64-d {
		return math.MaxInt64
	}
	return t + d
}

// Partitions are coarse UTC calendar months of sample time, as in ClickHouse
// (toYYYYMM) and VictoriaMetrics. Live intake writes one partition, two briefly
// at a month boundary. Retention never splits partitions: each part records its
// own expiry bounds for whole-part drops and bounded TTL rewrites.
func rollupMonthPartition(t int64) int64 {
	y, m, _ := time.Unix(t, 0).UTC().Date()
	return time.Date(y, m, 1, 0, 0, 0, 0, time.UTC).Unix()
}

// RollupMonthPartition is the live seal's partition of a sample time.
// Converters must use it so a baseline and a live seal stay byte-identical.
func RollupMonthPartition(t int64) int64 { return rollupMonthPartition(t) }

// expiryBounds accumulates Coverage.MinExpiry (data) and MaxExpiry (data and
// envelopes); zero means none seen.
type expiryBounds struct{ min, max int64 }

func (b *expiryBounds) data(e int64) {
	if b.min == 0 || e < b.min {
		b.min = max(e, 1)
	}
	b.envelope(e)
}

func (b *expiryBounds) envelope(e int64) { b.max = max(b.max, e, 1) }

func (b *expiryBounds) join(o expiryBounds) {
	if o.min != 0 {
		b.data(o.min)
	}
	if o.max != 0 {
		b.envelope(o.max)
	}
}
