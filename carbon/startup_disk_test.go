package carbon

import (
	"strings"
	"testing"

	"github.com/lomik/zapwriter"
	"github.com/prometheus/client_golang/prometheus"
)

func TestStartupCaptureSoftBudget(t *testing.T) {
	defer zapwriter.Test()()
	for _, tc := range []struct {
		name     string
		retained int64
		exceeded bool
	}{
		{"below", 299, false},
		{"equal", 300, false},
		{"above", 301, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := &startupInput{}
			s.size.Store(tc.retained)
			s.updateDiskSpace(900, 2000, 0)
			if got := s.overBudget.Load(); got != tc.exceeded {
				t.Fatalf("soft budget exceeded = %v, want %v", got, tc.exceeded)
			}
			if s.diskDanger(1) || s.paused.Load() || s.fallback.Load() {
				t.Fatal("soft budget paused capture with ample free disk")
			}
			s.updateDiskSpace(1200, 2000, 0)
			if s.overBudget.Load() {
				t.Fatal("soft budget warning did not clear after free-space recovery")
			}
		})
	}
	if !strings.Contains(zapwriter.TestString(), "exceeded soft spool budget") {
		t.Fatal("soft budget crossing did not log an error")
	}
	// Regression: a multi-TB retained spool must keep taking input while the
	// filesystem still has far more free space than its safety reserve.
	s := &startupInput{}
	s.size.Store(3_880_000_000_000)
	s.updateDiskSpace(11_600_000_000_000, 32_000_000_000_000, 0)
	if s.effectiveBytes.Load() != 3_870_000_000_000 || !s.overBudget.Load() || s.diskDanger(2<<20) {
		t.Fatal("soft budget treated as hard reserve")
	}
}

func TestStartupCaptureHardReserve(t *testing.T) {
	defer zapwriter.Test()()
	for _, tc := range []struct {
		name  string
		free  uint64
		frame int64
		pause bool
	}{
		{"below", 49, 0, true},
		{"equal", 50, 0, false},
		{"above", 51, 0, false},
		{"write to reserve", 51, 1, false},
		{"write below reserve", 51, 2, true},
		{"no space for write at reserve", 50, 1, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := &startupInput{}
			s.updateDiskSpace(tc.free, 1000, 0)
			if got := s.diskDanger(tc.frame); got != tc.pause {
				t.Fatalf("diskDanger(%d) = %v, want %v", tc.frame, got, tc.pause)
			}
		})
	}
}

func TestStartupCaptureReserveDebitsAppendsUntilResample(t *testing.T) {
	defer zapwriter.Test()()
	s := &startupInput{}
	s.updateDiskSpace(100, 1000, 0) // 50 bytes of headroom.
	s.size.Store(50)
	s.diskWritten.Store(50)
	if !s.diskDanger(1) {
		t.Fatal("writes since statfs did not consume headroom")
	}
	s.retiredBytes.Store(50)
	if !s.diskDanger(1) {
		t.Fatal("retirement returned bytes to a stale free-space sample")
	}
	s.updateDiskSpace(100, 1000, 50)
	if s.diskDanger(50) || !s.diskDanger(51) {
		t.Fatal("fresh free-space sample did not restore actual headroom")
	}
	s.size.Add(1 << 40) // Opening an older source does not allocate new blocks.
	if s.diskDanger(50) {
		t.Fatal("opening existing source bytes consumed disk headroom")
	}
}

func TestStartupCaptureDiskPressureMetrics(t *testing.T) {
	defer zapwriter.Test()()
	s := &startupInput{}
	s.size.Store(301)
	s.updateDiskSpace(900, 1000, 0)
	app := &App{}
	app.startupCaptureState.Store(s)
	reg := prometheus.NewRegistry()
	reg.MustRegister(&migrationCollector{app: app})
	check := func(over, paused, fallback float64) {
		t.Helper()
		families, err := reg.Gather()
		if err != nil {
			t.Fatal(err)
		}
		want := map[string]float64{
			"startup_capture_spool_over_budget": over,
			"startup_capture_paused":            paused,
			"startup_capture_fallback":          fallback,
		}
		for _, family := range families {
			if value, ok := want[family.GetName()]; ok {
				if got := family.Metric[0].GetGauge().GetValue(); got != value {
					t.Fatalf("%s = %v, want %v", family.GetName(), got, value)
				}
				delete(want, family.GetName())
			}
		}
		if len(want) != 0 {
			t.Fatal("missing disk pressure metrics", want)
		}
	}
	check(1, 0, 0)
	s.updateDiskSpace(49, 1000, 0)
	if !s.diskDanger(1) {
		t.Fatal("expected real disk pressure")
	}
	s.notePause()
	check(1, 1, 1)
	if s.pauseExpected.Load() != -1 || !strings.Contains(zapwriter.TestString(), "reached actual disk safety reserve") {
		t.Fatal("pause did not report a loud disk safety failure")
	}
}

func TestStartupCaptureDiskSampleFailureKeepsReserve(t *testing.T) {
	defer zapwriter.Test()()
	s := &startupInput{path: "/nonexistent-startup-capture-directory/input.tail.bin"}
	s.updateDiskSpace(49, 1000, 0)
	s.sampleDiskSpace()
	if !s.diskDanger(1) || !s.fallback.Load() {
		t.Fatal("failed statfs discarded reserve or did not publish failure")
	}
	if !strings.Contains(zapwriter.TestString(), "cannot sample disk safety reserve") {
		t.Fatal("failed statfs did not log an error")
	}
}
