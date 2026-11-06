package persister

import (
	"math"
	"time"
)

// Metrics retain every adjustment. INFO reports material changes relative to
// the last logged value, at most once per minute for a stable reason. A reason
// transition is immediate so pressure/rollback does not disappear in sampling.
type autoAdjustmentLog struct {
	at     time.Time
	reason string
	value  int
}

func (l *autoAdjustmentLog) allow(now time.Time, reason string, value int, fraction float64) bool {
	if !l.at.IsZero() && reason == l.reason {
		if now.Sub(l.at) < time.Minute || math.Abs(float64(value-l.value)) < max(1, math.Abs(float64(l.value))*fraction) {
			return false
		}
	}
	l.at, l.reason, l.value = now, reason, value
	return true
}

func capacityLogReason(reason string) string {
	// Observing and growing are the same healthy regime. Do not manufacture
	// reason transitions every other sample just because AIMD uses hysteresis.
	if reason == "observing" {
		return "token wait with idle hardware"
	}
	return reason
}
