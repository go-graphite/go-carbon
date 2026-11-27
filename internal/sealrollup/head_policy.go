package sealrollup

import (
	"fmt"
	"math"
	"time"
)

// HeadPolicy limits one partition head, owned by one writer. Journal sync is
// independent: sealing bounds memory and consolidation latency, not durability.
// Callers must keep unsealed heads visible to readers and replay the journal
// after the committed frontier. It never changes original batch boundaries.
type HeadPolicy struct {
	MaxAge    time.Duration
	MaxPoints uint64
}

// NewHeadPolicy derives identical per-owner limits from host intake and a total
// resident head budget. The default holds at most sixteen minutes of intake;
// memory pressure shortens that interval. No cluster names enter the policy.
// residentBytesPerPoint must include frozen/in-flight copies and row metadata;
// the budget must exclude memory reserved for sealed history, indexes and reads.
// A skewed partition may reach the size bound earlier than the estimated age.
func NewHeadPolicy(intakePointsPerSecond, budgetBytes, residentBytesPerPoint uint64, owners int) (HeadPolicy, error) {
	if intakePointsPerSecond == 0 || residentBytesPerPoint == 0 || owners < 1 {
		return HeadPolicy{}, fmt.Errorf("positive intake, resident point size and owner count required")
	}
	points := budgetBytes / residentBytesPerPoint / uint64(owners)
	if points == 0 {
		return HeadPolicy{}, fmt.Errorf("head budget cannot hold one point per owner")
	}
	const ceiling = 16 * time.Minute
	// Float conversion only chooses a scheduling limit; it never touches data.
	age := min(float64(ceiling), float64(points)*float64(owners)*float64(time.Second)/float64(intakePointsPerSecond))
	if age < 1 || age > math.MaxInt64 {
		return HeadPolicy{}, fmt.Errorf("head age outside duration range")
	}
	maxAge := time.Duration(age)
	// Do not reserve more rows than the time bound needs at balanced intake.
	timePoints := math.Ceil(float64(intakePointsPerSecond) * float64(maxAge) / float64(time.Second) / float64(owners))
	if timePoints < float64(points) {
		points = max(1, uint64(timePoints))
	}
	return HeadPolicy{MaxAge: maxAge, MaxPoints: points}, nil
}

// Due is pure and lock-free. opened is the oldest point's admission time, not
// its sample timestamp. The caller seals before admitting a batch that would
// exceed MaxPoints; an indivisible oversized original batch requires explicit
// admission backpressure or a separately budgeted allowance.
func (p HeadPolicy) Due(opened, now time.Time, points uint64) bool {
	return points > 0 && (points >= p.MaxPoints || now.Sub(opened) >= p.MaxAge)
}
