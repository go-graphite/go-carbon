package downsize

import (
	"testing"
	"time"
)

func TestReadyETAIncludesConvergenceAndKeepsUnknownWork(t *testing.T) {
	t.Parallel()
	var m readyETAModel
	now := time.Unix(1000, 0)
	s := progressSnapshot{Phase: "verify round 1", Round: 1, Status: "running", Total: 1000, FreshDoneAvailable: true}
	m.estimate(now, "run", s)
	s.Done, s.FreshDone = 600, 600
	e := m.estimate(now.Add(time.Minute), "run", s)
	if e.known != 40 || e.seconds != -1 || e.unknown != 2 || e.phases != 2 {
		t.Fatalf("verify: %+v", e)
	}
	next := int64(100)
	s = progressSnapshot{Phase: "fill", Round: 1, Status: "running", Total: 200, FreshDoneAvailable: true, NextVerifyNames: &next}
	m.estimate(now.Add(2*time.Minute), "run", s)
	s.Done, s.FreshDone = 100, 100
	e = m.estimate(now.Add(3*time.Minute), "run", s)
	// Fill: 100 remaining at 100/min = 60s. Verify: 100 at 10/s = 10s.
	if e.known != 70 || e.seconds != -1 || e.unknown != 2 || e.phases != 3 {
		t.Fatalf("fill+verify: %+v", e)
	}
	s.NextVerifyNames = nil
	e = m.estimate(now.Add(3*time.Minute), "run", s)
	if e.unknown != 3 || e.known != 60 {
		t.Fatalf("missing denominator: %+v", e)
	}
}

func TestReadyETADoesNotTreatFinalChecksAsReady(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		status  string
		seconds float64
	}{{"running", -1}, {"incomplete", -1}, {"failed", -1}, {"interrupted", -1}, {"complete", 0}} {
		var m readyETAModel
		e := m.estimate(time.Now(), "run", progressSnapshot{Phase: "ready", Status: tc.status})
		if e.seconds != tc.seconds {
			t.Fatalf("%s: %+v", tc.status, e)
		}
	}
}

func TestReadyETAFreshRatesResetOnReplayRoundPathAndStall(t *testing.T) {
	t.Parallel()
	for _, change := range []string{"replay", "round", "path", "waiting", "stall"} {
		t.Run(change, func(t *testing.T) {
			var m readyETAModel
			now := time.Unix(1000, 0)
			path := "run"
			s := progressSnapshot{Phase: "verify round 1", Round: 1, Status: "running", Total: 2000000, Done: 1000000, FreshDoneAvailable: true}
			m.estimate(now, path, s)
			s.Done += 100
			s.FreshDone = 100
			if e := m.estimate(now.Add(time.Minute), path, s); e.known < 500000 {
				t.Fatalf("fresh-rate projection: %+v", e)
			}
			at := now.Add(2 * time.Minute)
			switch change {
			case "replay":
				s.Done += 500000
				s.FreshDoneAvailable = false
			case "round":
				s.Round = 2
				s.Phase = "verify round 2"
			case "path":
				path = "recovery"
			case "waiting":
				s.WaitingForStore = true
			case "stall":
				at = now.Add(6 * time.Minute)
			}
			e := m.estimate(at, path, s)
			if e.known != 0 || e.seconds != -1 || e.unknown == 0 {
				t.Fatalf("%s: %+v", change, e)
			}
		})
	}
}

func TestReadyETAPlannedVerificationAfterCopy(t *testing.T) {
	t.Parallel()
	var m readyETAModel
	now := time.Unix(1000, 0)
	verify := progressSnapshot{Phase: "verify round 1", Round: 1, Status: "running", Total: 100, FreshDoneAvailable: true}
	m.estimate(now, "run", verify)
	verify.Done, verify.FreshDone = 100, 100
	m.estimate(now.Add(time.Minute), "run", verify)
	next := int64(200)
	copy := progressSnapshot{Phase: "copy", Status: "running", Done: 100, Total: 100, NextVerifyNames: &next, FreshDoneAvailable: true}
	e := m.estimate(now.Add(2*time.Minute), "run", copy)
	if e.known != 120 || e.seconds != -1 || e.phases != 3 {
		t.Fatalf("copy+verification: %+v", e)
	}
}

func TestReadyETADoesNotReuseRateForDifferentScope(t *testing.T) {
	t.Parallel()
	var m readyETAModel
	now := time.Unix(1000, 0)
	s := progressSnapshot{Phase: "verify round 1", Round: 1, Status: "running", Total: 100, FreshDoneAvailable: true, Effective: &effectiveSyncPolicy{Engine: "metric", Scope: "full-history"}}
	m.estimate(now, "run", s)
	s.Done, s.FreshDone = 100, 100
	m.estimate(now.Add(time.Minute), "run", s)
	next := int64(100)
	s = progressSnapshot{Phase: "fill", Status: "running", Done: 10, Total: 10, NextVerifyNames: &next, FreshDoneAvailable: true, Effective: &effectiveSyncPolicy{Engine: "native-window", Scope: "windows-only"}}
	e := m.estimate(now.Add(2*time.Minute), "recovery", s)
	if e.known != 0 || e.unknown != 3 {
		t.Fatalf("scope reused an incompatible rate: %+v", e)
	}
}
