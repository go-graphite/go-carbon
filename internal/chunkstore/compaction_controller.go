package chunkstore

import (
	"math"
	"sync"
	"time"
)

const (
	compactionPacingWindow   = time.Hour
	compactionPacingFloor    = 8 << 20
	compactionStallHold      = 5 * time.Minute
	compactionConversionHold = 5 * time.Minute
)

type compactionSample struct {
	// Physical SST ingress avoids counting a write both in the WAL and again
	// when flushed, and keeps compression out of the amplification ratio.
	ingress, output, debt uint64
	l0                    int64
	memtablePressure      bool
}

type compactionPacingStats struct {
	rate, target int64
	auto         bool
	bypass       string
	bypasses     map[string]uint64
}

// compactionController uses only local observations. The output-rate guard
// covers work that lags ingress; debt growth adds catch-up budget even for
// delete waves, which produce little ingress. Pressure always wins over rate.
type compactionController struct {
	mu                      sync.Mutex
	pacer                   *compactionPacer
	mode, target, l0Limit   int64
	started, last           time.Time
	previous                compactionSample
	weight, ingress, output float64
	amplification, debtMean float64
	debtWeight, debtTrend   float64
	debtAlarm               bool
	stallActive             bool
	stallUntil              time.Time
	conversion              func() (int64, uint64)
	converted               uint64
	conversionUntil         time.Time
	stopping                bool
	bypass                  string
	bypasses                map[string]uint64
}

func newCompactionController(p *compactionPacer, mode int64, now time.Time) *compactionController {
	c := &compactionController{pacer: p, mode: mode, l0Limit: 6, started: now, last: now,
		amplification: 1, bypasses: make(map[string]uint64)}
	c.apply(now)
	return c
}

func (c *compactionController) sample(now time.Time, s compactionSample) {
	c.mu.Lock()
	defer c.mu.Unlock()
	dt := now.Sub(c.last)
	if dt <= 0 {
		return
	}
	a := -math.Expm1(-dt.Seconds() / compactionPacingWindow.Seconds())
	c.weight += a * (1 - c.weight)
	c.ingress += a * (float64(s.ingress-c.previous.ingress)/dt.Seconds() - c.ingress)
	c.output += a * (float64(s.output-c.previous.output)/dt.Seconds() - c.output)
	// Smooth the measured ratio too: a compaction may rewrite older flushes,
	// rather than the data that arrived in this sampling interval.
	if c.ingress > 0 {
		c.amplification = max(1, c.amplification+a*(c.output/c.ingress-c.amplification))
	}
	trendAlpha := -math.Expm1(-dt.Seconds() / (5 * time.Minute).Seconds())
	c.debtTrend += trendAlpha * ((float64(s.debt)-float64(c.previous.debt))/dt.Seconds() - c.debtTrend)
	c.last, c.previous = now, s

	// Compare before learning the sample. Freeze the normal debt baseline
	// during an alarm, so a sustained backlog cannot become the new normal.
	debt := float64(s.debt)
	normal := c.debtMean
	if c.debtWeight > 0 {
		normal /= c.debtWeight
	}
	if c.debtAlarm {
		c.debtAlarm = debt > max(2<<30, 1.5*normal)
	} else {
		c.debtAlarm = debt > max(4<<30, 2*normal)
	}
	if !c.debtAlarm || now.Sub(c.started) < compactionPacingWindow {
		c.debtMean += a * (debt - c.debtMean)
		c.debtWeight += a * (1 - c.debtWeight)
	}
	c.apply(now)
}

// apply is called under mu, except during construction. It never calls Pebble:
// stall callbacks hold Pebble's DB mutex and must be able to wake writers.
func (c *compactionController) apply(now time.Time) {
	var active int64
	if c.conversion != nil {
		var converted uint64
		active, converted = c.conversion()
		if active > 0 || converted != c.converted {
			c.conversionUntil = now.Add(compactionConversionHold)
		}
		c.converted = converted
	}
	rate, target, reason := c.mode, c.mode, ""
	if c.mode == -1 {
		budget := float64(compactionPacingFloor)
		if c.weight > 0 {
			budget = max(budget, 2*max(c.ingress*c.amplification, c.output)/c.weight+max(0, c.debtTrend))
		}
		target = math.MaxInt64
		if budget < float64(math.MaxInt64) {
			target = int64(math.Ceil(budget))
		}
		rate = target
		switch {
		case active > 0 || now.Before(c.conversionUntil):
			reason = "conversion"
		case c.stallActive || now.Before(c.stallUntil):
			reason = "stall"
		case c.previous.l0 >= c.l0Limit:
			reason = "l0"
		case c.previous.memtablePressure:
			reason = "memtable"
		case c.debtAlarm:
			reason = "debt"
		case now.Sub(c.started) < compactionPacingWindow:
			reason = "warmup"
		}
	}
	if c.stopping {
		reason = "shutdown"
	}
	if reason != "" {
		rate = 0
		if reason != c.bypass {
			c.bypasses[reason]++
		}
	}
	c.target, c.bypass = target, reason
	if c.pacer.rateBytes() != rate {
		c.pacer.setRate(rate)
	}
}

func (c *compactionController) setMode(mode int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.mode = mode
	c.apply(time.Now())
}

func (c *compactionController) stall(active bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.stallActive, c.stallUntil = active, time.Now().Add(compactionStallHold)
	c.apply(time.Now())
}

func (c *compactionController) setConversionSource(source func() (int64, uint64)) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.conversion = source
	// Existing completed work may be days old. Only new completions or
	// in-flight work extend the hold; a registered idle source never does.
	if source != nil {
		_, c.converted = source()
	}
	c.apply(time.Now())
}

func (c *compactionController) stop() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.stopping = true
	c.apply(time.Now())
}

func (c *compactionController) stats() compactionPacingStats {
	c.mu.Lock()
	defer c.mu.Unlock()
	s := compactionPacingStats{rate: c.pacer.rateBytes(), target: c.target, auto: c.mode == -1,
		bypass: c.bypass, bypasses: make(map[string]uint64, len(c.bypasses))}
	for reason, count := range c.bypasses {
		s.bypasses[reason] = count
	}
	return s
}

func (s *Store) startCompactionController() {
	c := s.compactionControl
	initial := s.compactionSample()
	c.mu.Lock()
	c.l0Limit = int64(max(1, min(6, s.pebbleOptions.L0StopWritesThreshold/2)))
	c.started, c.last, c.previous = time.Now(), time.Now(), initial
	c.apply(c.last)
	c.mu.Unlock()
	s.compactionStop, s.compactionDone = make(chan struct{}), make(chan struct{})
	go func() {
		defer close(s.compactionDone)
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-s.compactionStop:
				return
			case <-ticker.C:
				v := s.compactionSample()
				c.sample(time.Now(), v)
			}
		}
	}()
}

func (s *Store) compactionSample() compactionSample {
	m := s.db.Metrics()
	var ingress uint64
	for _, level := range m.Levels {
		ingress += level.BytesFlushed + level.BytesIngested
	}
	limit := uint64(s.pebbleOptions.MemTableStopWritesThreshold) * s.pebbleOptions.MemTableSize
	return compactionSample{ingress: ingress, output: s.compactionPacing.output.Load(),
		debt: m.Compact.EstimatedDebt, l0: int64(m.Levels[0].Sublevels), memtablePressure: m.MemTable.Size >= limit-limit/4}
}

// SetCompactionPacingConversionSource supplies in-flight and completed conversion
// counters, sampled each second. The source must only read concurrent counters:
// it runs under the controller lock, including from Pebble's stall callbacks.
// Auto bypasses during activity and for five minutes after the last observation;
// an idle migration may remain registered indefinitely. Fixed budgets stay under
// the operator's control. A nil source detaches it without clearing a recent hold.
func (s *Store) SetCompactionPacingConversionSource(source func() (inFlight int64, completed uint64)) {
	s.compactionControl.setConversionSource(source)
}

// StopCompactionPacing releases writers before shutdown starts draining work.
// It is permanent, including across a concurrent reload or migration completion.
func (s *Store) StopCompactionPacing() { s.compactionControl.stop() }
