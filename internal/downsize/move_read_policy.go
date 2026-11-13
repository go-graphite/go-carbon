package downsize

import (
	"errors"
	"fmt"
	"math"
	"os"
	"slices"
	"time"
)

const (
	defaultReadCeilingMS = 100
	defaultReadFloorMS   = 50
	defaultReadRatio     = 1.5
	// Journals written before the read policy was persisted used this fixed
	// floor and ratio with a per-run --read-p99-ms ceiling.
	legacyReadFloorMS = 2
	legacyReadRatio   = 1.5
)

// readPolicy bounds the importer's read-p99 pause:
// limit = min(CeilingMS, max(FloorMS, Ratio × pre-copy median read p99)).
// A one-millisecond baseline cannot pause a copy on normal 1-30 ms swings;
// the absolute ceiling still applies, and CeilingMS 0 leaves only the
// relative limit.
type readPolicy struct {
	CeilingMS float64 `json:"ceiling_ms"`
	FloorMS   float64 `json:"floor_ms"`
	Ratio     float64 `json:"ratio"`
}

func defaultReadPolicy() readPolicy {
	return readPolicy{defaultReadCeilingMS, defaultReadFloorMS, defaultReadRatio}
}

func (p readPolicy) validate() error {
	for _, v := range []float64{p.CeilingMS, p.FloorMS, p.Ratio} {
		if math.IsNaN(v) || math.IsInf(v, 0) {
			return errors.New("invalid read p99 policy: values must be finite")
		}
	}
	if p.CeilingMS < 0 || p.FloorMS < 0 || p.Ratio < 1 {
		return errors.New("invalid read p99 policy: ceiling and floor must be >= 0 ms, ratio >= 1")
	}
	return nil
}

// limit returns the read-p99 pause threshold in milliseconds; zero disables it.
func (p readPolicy) limit(baseline float64) float64 {
	limit := p.CeilingMS
	if baseline > 0 {
		relative := max(p.FloorMS, baseline*p.Ratio)
		if limit <= 0 || relative < limit {
			limit = relative
		}
	}
	return limit
}

// policyOverride records a resume that changed a governor threshold persisted
// in the journal. The list is saved with the admission state and printed by
// status, so a relaxed or tightened envelope is never silent.
type policyOverride struct {
	At   string  `json:"at"`
	Flag string  `json:"flag"`
	From float64 `json:"from"`
	To   float64 `json:"to"`
	Note string  `json:"note,omitempty"`
}

// resumePolicy merges the saved admission policy with this run's flags.
// Thresholds named in explicit (nil means every option counts as given)
// override the saved values; others keep the journal's values. The backlog
// baseline window describes how the saved baseline was learned and is fixed.
// Journals without a saved read policy adopt this run's read flags.
func resumePolicy(saved admissionState, backlog backlogPolicy, read readPolicy, explicit map[string]bool, now time.Time) (admissionState, []policyOverride, error) {
	given := func(flag string) bool { return explicit == nil || explicit[flag] }
	at := now.UTC().Format(time.RFC3339)
	var changes []policyOverride
	pick := func(flag string, old, requested float64) float64 {
		if !given(flag) || old == requested {
			return old
		}
		changes = append(changes, policyOverride{At: at, Flag: flag, From: old, To: requested})
		return requested
	}
	if given("backlog-baseline-window") && backlog.Window != saved.Policy.Window {
		return saved, nil, fmt.Errorf("--backlog-baseline-window %s differs from the journal's learned baseline window %s; omit it or pass the saved value", backlog.Window, saved.Policy.Window)
	}
	p := saved.Policy
	p.SoftRatio = pick("backlog-soft-ratio", p.SoftRatio, backlog.SoftRatio)
	p.HardRatio = pick("backlog-hard-ratio", p.HardRatio, backlog.HardRatio)
	p.GrowthSamples = int(pick("backlog-growth-samples", float64(p.GrowthSamples), float64(backlog.GrowthSamples)))
	p.BytesPerPoint = pick("cache-bytes-per-point", p.BytesPerPoint, backlog.BytesPerPoint)
	if err := p.validate(); err != nil {
		return saved, nil, fmt.Errorf("resume policy override: %w", err)
	}
	var r readPolicy
	if saved.Read == nil {
		// Record the implicit legacy floor and ratio being replaced. The legacy
		// ceiling was never saved: it always came from this run's flag.
		const note = "journal predates the saved read policy"
		r = readPolicy{CeilingMS: read.CeilingMS, FloorMS: legacyReadFloorMS, Ratio: legacyReadRatio}
		if r.FloorMS != read.FloorMS {
			changes = append(changes, policyOverride{At: at, Flag: "read-p99-floor-ms", From: r.FloorMS, To: read.FloorMS, Note: note})
		}
		if r.Ratio != read.Ratio {
			changes = append(changes, policyOverride{At: at, Flag: "read-p99-ratio", From: r.Ratio, To: read.Ratio, Note: note})
		}
		r = read
	} else {
		r = *saved.Read
		r.CeilingMS = pick("read-p99-ms", r.CeilingMS, read.CeilingMS)
		r.FloorMS = pick("read-p99-floor-ms", r.FloorMS, read.FloorMS)
		r.Ratio = pick("read-p99-ratio", r.Ratio, read.Ratio)
	}
	if err := r.validate(); err != nil {
		return saved, nil, fmt.Errorf("resume policy override: %w", err)
	}
	merged := saved
	merged.Policy = p
	merged.Read = &r
	merged.Overrides = append(slices.Clone(saved.Overrides), changes...)
	return merged, changes, nil
}

// reportOverrides makes every resume override visible in the event stream and
// on stderr, which launchers keep in their logs.
func reportOverrides(changes []policyOverride) {
	for _, c := range changes {
		event("policy_override", map[string]any{"flag": c.Flag, "from": c.From, "to": c.To, "note": c.Note, "warning": "resume changed a governor threshold saved in the journal"})
		fmt.Fprintf(os.Stderr, "WARNING: resume overrides journal policy --%s %g -> %g %s\n", c.Flag, c.From, c.To, c.Note)
	}
}

func medianFloat(values []float64) float64 {
	sorted := slices.Clone(values)
	slices.Sort(sorted)
	mid := len(sorted) / 2
	if len(sorted)%2 == 0 {
		return (sorted[mid-1] + sorted[mid]) / 2
	}
	return sorted[mid]
}
