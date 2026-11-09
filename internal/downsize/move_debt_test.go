package downsize

import (
	"context"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestDebtTuningEnvelope(t *testing.T) {
	for _, tc := range []struct {
		name   string
		values []uint64
		want   string
	}{
		{"foreground-swings", []uint64{3e9, 12.7e9, 5e9, 11e9, 4e9, 12.7e9}, ""},
		{"two-high-samples", []uint64{16e9, 16e9}, ""},
		{"sustained-high", []uint64{16e9, 16e9, 16e9}, "compaction debt ratio"},
		{"spike-recovers", []uint64{16e9, 16e9, 4e9, 16e9}, ""},
		{"strictly-rising", []uint64{22.5e9, 22.6e9, 22.7e9, 22.8e9, 22.9e9}, "compaction debt ratio"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := hostLimit{debtBaseline: &backlogBaseline{Min: 3e9, Median: 7e9, Max: 12.7e9}}
			var status debtStatus
			for _, v := range tc.values {
				s := storageSample{CompactionDebt: v}
				status = h.observeDebt(hostSample{}, s, 5, false)
				h.storePrevious = &s
			}
			if status.Soft != tc.want || status.Limit != 15.875e9 {
				t.Fatalf("%+v", status)
			}
		})
	}
	// A narrow observed swing can bind before the 25% ratio threshold.
	h := hostLimit{debtBaseline: &backlogBaseline{Min: 9e9, Median: 9.5e9, Max: 10e9}}
	for i, v := range []uint64{11.1e9, 11.2e9, 11.3e9, 11.4e9, 11.5e9} {
		s := storageSample{CompactionDebt: v}
		status := h.observeDebt(hostSample{}, s, 5, false)
		h.storePrevious = &s
		if (status.Soft != "") != (i == 4) || i == 4 && status.Soft != "compaction debt growth" {
			t.Fatalf("sample %d: %+v", i, status)
		}
	}
}

func TestDebtAbsoluteFloorAndHardStops(t *testing.T) {
	h := hostLimit{debtBaseline: &backlogBaseline{Min: 1e9, Median: 2e9, Max: 3e9}}
	for range 3 {
		if s := h.observeDebt(hostSample{}, storageSample{CompactionDebt: 8 << 30}, 5, false); s.Soft != "" {
			t.Fatal(s)
		}
	}
	for _, s := range []storageSample{{CompactionDebt: (64 << 30) + 1}, {L0StopWrites: 32, L0Sublevels: 31}, {WriteStallActive: true}, {WriteStalls: 1}} {
		hard, _ := guard(backlogTestSample(240e6), s, nil, &storageSample{}, 0)
		if hard == "" {
			t.Fatalf("hard stop missed: %+v", s)
		}
	}
	hard, soft := guard(backlogTestSample(240e6), storageSample{L0StopWrites: 32, L0Sublevels: 16}, nil, nil, 0)
	if hard != "" || soft != "compaction pressure" {
		t.Fatal(hard, soft)
	}
}

func TestDebtJournalResumeAndLegacyObservation(t *testing.T) {
	tune := newTestTuner([]string{"a"}, capacity{8, 32, 1 << 20, 100}, true, 0, 0)
	sample := backlogTestSample(240e6)
	tune.hosts["a"].baseline = &backlogBaseline{Min: 236e6, Median: 242e6, Max: 248e6, Samples: 10, Start: sample.Time - 300, End: sample.Time}
	tune.hosts["a"].debtBaseline = &backlogBaseline{Min: 3e9, Median: 7e9, Max: 12.7e9, Samples: 10, Start: sample.Time - 300, End: sample.Time}
	for range 2 {
		sample.Time += .01
		if err := tune.observe("a", sample, storageSample{CompactionDebt: 16e9}); err != nil {
			t.Fatal(err)
		}
	}
	j, err := openJournal(filepath.Join(t.TempDir(), "state"), "plan")
	if err != nil {
		t.Fatal(err)
	}
	if err = j.saveAdmission(tune.snapshotAdmission(false)); err != nil {
		t.Fatal(err)
	}
	if err = j.compact(); err != nil {
		t.Fatal(err)
	}
	path := j.path
	j.close()
	j, err = openJournal(path, "plan")
	if err != nil {
		t.Fatal(err)
	}
	defer j.close()
	restored := newTuner([]string{"a"}, tune.maximum, true, 0, 0)
	if err = restored.restoreAdmission(j.admission, false); err != nil {
		t.Fatal(err)
	}
	sample.Time += .01
	if err = restored.observe("a", sample, storageSample{CompactionDebt: 16e9}); err != nil {
		t.Fatal(err)
	}
	if restored.hosts["a"].debtHigh != 3 || restored.hosts["a"].Streams != 0 {
		t.Fatal("lost pending debt breach")
	}
	saved := j.admission.Hosts["a"]
	saved.DebtBaseline = nil
	j.admission.Hosts["a"] = saved
	legacy := newTuner([]string{"a"}, tune.maximum, true, 0, 0)
	if err = legacy.restoreAdmission(j.admission, false); err == nil || !strings.Contains(err.Error(), "observe-only") {
		t.Fatal("legacy copied without debt baseline", err)
	}
	legacy.collectingDebt = true
	if err = legacy.restoreAdmission(j.admission, false); err != nil {
		t.Fatal(err)
	}
	if _, err = legacy.admit(context.Background(), "a", "a"); err == nil {
		t.Fatal("observation admitted copy")
	}
}

func TestObserveOnlyIssuesNoTransferRequests(t *testing.T) {
	source, target := localStore(t), localStore(t)
	var transfers atomic.Int32
	wrap := func(h http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodGet {
				transfers.Add(1)
			}
			h.ServeHTTP(w, r)
		})
	}
	src := httptest.NewServer(wrap(localService(t, source)))
	defer src.Close()
	dst := httptest.NewServer(wrap(localService(t, target)))
	defer dst.Close()
	root := fixturePlan(t, src.URL, dst.URL, []string{"must-not-copy"})
	o := testOptions(root)
	o.delete = false
	o.observeOnly = 20 * time.Millisecond
	o.tick = time.Second
	if err := run(context.Background(), o); err != nil {
		t.Fatal(err)
	}
	if transfers.Load() != 0 {
		t.Fatal("observation sent transfer request")
	}
}
