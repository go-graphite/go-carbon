package carbon

import (
	"errors"
	"fmt"
	"math"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/internal/inputlog"
	"github.com/go-graphite/go-carbon/points"
	"github.com/lomik/zapwriter"
	"github.com/prometheus/client_golang/prometheus"
)

func TestCaptureAdmissionUsesBytesAndWakesOnStop(t *testing.T) {
	s := &startupInput{stop: make(chan struct{})}
	j := &startupJournal{owner: s, budget: captureByteBudget{limit: 200000, changed: make(chan struct{})}}
	release, err := j.acquire(100, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	// A larger batch waits although only one caller currently holds admission.
	done := make(chan error, 1)
	go func() {
		r, err := j.acquire(30000, 1)
		if err == nil {
			r()
		}
		done <- err
	}()
	select {
	case err := <-done:
		t.Fatal("byte budget did not block", err)
	case <-time.After(10 * time.Millisecond):
	}
	close(s.stop)
	if err := <-done; !errors.Is(err, inputlog.ErrClosed) {
		t.Fatal(err)
	}
	if j.budget.peak.Load() > j.budget.limit {
		t.Fatal("budget exceeded")
	}
}

func TestCaptureAdmissionPressureMetrics(t *testing.T) {
	defer zapwriter.Test()()
	s := &startupInput{stop: make(chan struct{})}
	j := &startupJournal{owner: s, budget: captureByteBudget{limit: 200000, changed: make(chan struct{})}}
	s.journal = j
	release, err := j.acquire(100, 1)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := j.acquire(200000, 1); err == nil {
		t.Fatal("oversized batch admitted")
	}
	done := make(chan error, 1)
	go func() {
		r, err := j.acquire(30000, 1)
		if err == nil {
			r()
		}
		done <- err
	}()
	deadline := time.Now().Add(time.Second)
	for j.budget.waits.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	release()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	app := New("")
	app.startupCaptureState.Store(s)
	reg := prometheus.NewRegistry()
	reg.MustRegister(&migrationCollector{app: app})
	families, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]float64{"startup_capture_admission_limit_bytes": 200000, "startup_capture_admission_used_bytes": 0, "startup_capture_admission_waits_total": 1, "startup_capture_admission_rejections_total": 1, "startup_capture_admission_calls_total": 2}
	for _, f := range families {
		if value, ok := want[f.GetName()]; ok {
			m := f.Metric[0]
			got := m.GetGauge().GetValue() + m.GetCounter().GetValue()
			if got != value {
				t.Errorf("%s = %v, want %v", f.GetName(), got, value)
			}
			delete(want, f.GetName())
		}
	}
	if len(want) > 0 {
		t.Fatal("missing metrics", want)
	}
	logs := zapwriter.TestString()
	if !strings.Contains(logs, "admission byte budget exhausted") || !strings.Contains(logs, "exceeds admission byte budget") {
		t.Fatal("budget pressure was not logged", logs)
	}
}

func TestCapturePoolAccountsSkewAndNameHighWater(t *testing.T) {
	app := journalStartupFixture(t, t.TempDir())
	defer closeJournalFixture(app)
	j := app.startupInput.journal
	var raw []byte
	for i := 0; i < 2048; i++ {
		p := points.OnePoint(fmt.Sprintf("skew.unique.%d", i), 1, 1700000000)
		raw = p.AppendBinary(raw)
	}
	if err := j.write(raw, 2048); err != nil {
		t.Fatal(err)
	}
	buffer := <-j.buffers
	if buffer.peakNames != 2048 || buffer.retained < 2048*256 {
		t.Fatal("names not accounted", buffer.peakNames, buffer.retained)
	}
	j.buffers <- buffer
	// One large multi-point record exercises the decoder arena independently
	// of the many-small-record fixture. The arena must not enter the idle pool.
	p := points.Points{Metric: "skew.one", Data: make([]points.Point, 16384)}
	for i := range p.Data {
		p.Data[i] = points.Point{Value: float64(i), Timestamp: int64(1700000000 + i)}
	}
	if err := j.write(p.AppendBinary(nil), len(p.Data)); err != nil {
		t.Fatal(err)
	}
	var retained int64
	for len(j.buffers) > 0 {
		b := <-j.buffers
		if !reflect.DeepEqual(b.decoder, points.FrameDecoder{}) {
			t.Fatal("idle decoder arena retained")
		}
		if b.peakNames < 2048 {
			t.Fatal("map high water lost")
		}
		retained += b.retained
	}
	if got := j.pooledBytes.Load(); got != retained || got > 32<<20 {
		t.Fatal("idle pool accounting", got, retained)
	}
}

func TestCaptureAdmissionRejectsOverflow(t *testing.T) {
	j := &startupJournal{owner: &startupInput{stop: make(chan struct{})}, budget: captureByteBudget{limit: 128 << 20, changed: make(chan struct{})}}
	for _, input := range [][2]int{{math.MaxInt, 1}, {1, math.MaxInt}} {
		if r, err := j.acquire(input[0], input[1]); err == nil {
			r()
			t.Fatal("overflow admitted", input)
		}
	}
	if j.budget.used != 0 || j.budget.rejected.Load() != 2 {
		t.Fatal("overflow accounting changed budget")
	}
}
