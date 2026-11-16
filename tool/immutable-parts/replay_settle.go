package main

import (
	"math"
	"time"
)

type debtObservation struct{ seconds, bytes float64 }
type replaySettlement struct{ observations []debtObservation }

// Observe retains a ten-minute window plus the immediately preceding sample.
// That bracket prevents a five-second scrape cadence from silently shortening
// the required observation. Nothing is subtracted from reported debt.
func (s *replaySettlement) observe(now time.Duration, sample map[string]any) {
	s.observations = append(s.observations, debtObservation{now.Seconds(), float64(sample["estimated_debt_bytes"].(uint64))})
	cutoff := now.Seconds() - 600
	for len(s.observations) > 2 && s.observations[1].seconds <= cutoff {
		s.observations = s.observations[1:]
	}
}

func (s *replaySettlement) assess(sample map[string]any, continuous bool) bool {
	if !continuous {
		return sample["estimated_debt_bytes"].(uint64) == 0 && sample["compactions_in_progress"].(int64) == 0 && sample["flushes_in_progress"].(int64) == 0
	}
	n := len(s.observations)
	span, slope := 0.0, math.Inf(1)
	if n >= 2 {
		first := s.observations[0].seconds
		span = s.observations[n-1].seconds - first
		var sx, sy, sxx, sxy float64
		for _, v := range s.observations {
			x := v.seconds - first
			sx += x
			sy += v.bytes
			sxx += x * x
			sxy += x * v.bytes
		}
		denom := float64(n)*sxx - sx*sx
		if denom > 0 {
			slope = (float64(n)*sxy - sx*sy) / denom
		}
	}
	target := sample["settlement_target_file_bytes"].(uint64)
	// Noise budget is at most 1% of one target SST over ten minutes, capped
	// at 64KiB. Declare it before the run; report both signed slope and budget.
	noise := math.Min(float64(target)/100, 65536) / 600
	sample["settlement"] = "automatic_pebble_thresholds_600s"
	sample["debt_window_seconds"] = span
	if math.IsInf(slope, 0) {
		sample["debt_slope_bytes_per_second"] = nil
	} else {
		sample["debt_slope_bytes_per_second"] = slope
	}
	sample["debt_slope_noise_bytes_per_second"] = noise
	return n >= 2 && span >= 600 && math.Abs(slope) <= noise && target > 0 && sample["estimated_debt_bytes"].(uint64) < target && sample["compactions_in_progress"].(int64) == 0 && sample["flushes_in_progress"].(int64) == 0 && sample["l0_below_compaction_threshold"].(bool)
}
