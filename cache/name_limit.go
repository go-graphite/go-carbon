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

// NameTooLongRejectedPoints returns points refused at intake because their
// name exceeds points.MaxMetricNameBytes.
func (c *Cache) NameTooLongRejectedPoints() uint64 { return c.stat.nameTooLong.Load() }
