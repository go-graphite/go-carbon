package cache

import (
	"sync/atomic"
	"time"

	"github.com/lomik/zapwriter"
	"go.uber.org/zap"

	"github.com/go-graphite/go-carbon/points"
)

var nameTooLongLogged atomic.Int64

// rejectNameTooLong counts p's points and warns at most once a minute.
func (c *Cache) rejectNameTooLong(p *points.Points) {
	c.stat.nameTooLong.Add(uint64(len(p.Data)))
	now := time.Now().UnixNano()
	last := nameTooLongLogged.Load()
	if now-last < int64(time.Minute) || !nameTooLongLogged.CompareAndSwap(last, now) {
		return
	}
	zapwriter.Logger("cache").Warn("refused points: metric name too long",
		zap.Int("name_bytes", len(p.Metric)), zap.Int("max_bytes", points.MaxMetricNameBytes),
		zap.String("name_prefix", p.Metric[:200]), zap.Uint64("rejected_points_total", c.stat.nameTooLong.Load()))
}

// AddNameTooLongRejected counts points another intake (v2) refused by the same
// name limits, so receiver_name_too_long_rejected_points_total covers both.
func (c *Cache) AddNameTooLongRejected(n uint64) { c.stat.nameTooLong.Add(n) }

// NameTooLongRejectedPoints returns points refused at intake because their
// name exceeds points.MaxMetricNameBytes.
func (c *Cache) NameTooLongRejectedPoints() uint64 { return c.stat.nameTooLong.Load() }
