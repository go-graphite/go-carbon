//go:build unix

package carbon

import (
	"fmt"
	"net"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/go-graphite/go-carbon/points"
	"github.com/lomik/zapwriter"
)

// Exercise the real capture source construction and both replay/frontier
// transitions. Calling the cache directly would miss an unmarked source.
func TestStartupCapturePreservesCreationAdmission(t *testing.T) {
	if isolatedTestProcess(t) {
		return
	}
	defer zapwriter.Test()()
	s, app, core, address := directCaptureFixture(t)
	var capturedChecks atomic.Int64
	blocked := func(p *points.Points) bool { return strings.HasPrefix(p.Metric, "blocked.") }
	core.SetCreationThrottle(func(p *points.Points, _ bool) bool {
		capturedChecks.Add(1)
		return blocked(p)
	})
	core.SetThrottleWithHash(func(p *points.Points, _ bool, _ uint64, _ *any) bool { return blocked(p) })
	conn, err := net.Dial("tcp", address)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	write := func(phase string) {
		t.Helper()
		if _, err := fmt.Fprintf(conn, "blocked.%s 1 1\nallowed.%s 1 1\n", phase, phase); err != nil {
			t.Fatal(err)
		}
	}
	write("history")
	awaitCapture(t, func() bool { return s.points.Load() == 2 })
	app.finishStartupInput(core)
	awaitCapture(t, func() bool { return len(core.Get("allowed.history")) == 1 && core.AdmissionStats().Throttled == 1 })
	awaitCapture(t, func() bool { return allCaptureLive(s) })
	write("frontier")
	awaitCapture(t, func() bool { return len(core.Get("allowed.frontier")) == 1 && core.AdmissionStats().Throttled == 2 })
	if capturedChecks.Load() != 4 {
		t.Fatalf("raw capture checks=%d, want 4", capturedChecks.Load())
	}
	goDirect(t, s)
	write("direct")
	awaitCapture(t, func() bool { return len(core.Get("allowed.direct")) == 1 && core.AdmissionStats().Throttled == 3 })
	for _, phase := range []string{"history", "frontier", "direct"} {
		if len(core.Get("blocked."+phase)) != 0 {
			t.Fatalf("%s input bypassed creation admission", phase)
		}
	}
}
