package downsize

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/go-graphite/go-carbon/internal/chunkstore"
)

// --debt-guard=false removes the compaction-debt governor (soft pause and the
// baseline requirement) but keeps hard storage stops.
func TestDebtGuardOffSkipsDebtPauseKeepsHardStops(t *testing.T) {
	run := func(off bool) (*tuner, error) {
		tune := newTestTuner([]string{"a"}, capacity{64, 8192, 64 << 20, 65536}, false, 0, 0)
		tune.debtGuardOff = off
		h := tune.hosts["a"]
		h.debtBaseline = &backlogBaseline{Samples: 10, Start: 1, End: 301, Min: 3e9, Median: 7e9, Max: 12.7e9}
		sample := backlogTestSample(1)
		var err error
		for range 4 {
			sample.Time += .01
			if err = tune.observe("a", sample, storageSample{CompactionDebt: 16e9, MaxTransfers: 64}); err != nil {
				break
			}
		}
		return tune, err
	}
	on, err := run(false)
	if err != nil {
		t.Fatal(err)
	}
	if on.hosts["a"].Streams >= 64 {
		t.Fatal("debt guard on did not pause on sustained compaction debt", on.hosts["a"].capacity)
	}
	off, err := run(true)
	if err != nil {
		t.Fatal(err)
	}
	if off.hosts["a"].Streams != 64 {
		t.Fatal("debt guard off still paused admission", off.hosts["a"].capacity)
	}

	// A missing debt baseline is a hard stop only while the guard is on.
	for _, guardOff := range []bool{false, true} {
		tune := newTestTuner([]string{"a"}, capacity{64, 8192, 64 << 20, 65536}, false, 0, 0)
		tune.debtGuardOff = guardOff
		tune.hosts["a"].debtBaseline = nil
		err := tune.observe("a", backlogTestSample(1), storageSample{MaxTransfers: 64})
		if (err != nil) == guardOff {
			t.Fatalf("debt guard off=%v: missing baseline error %v", guardOff, err)
		}
	}

	// Hard limits are not waived.
	tune := newTestTuner([]string{"a"}, capacity{64, 8192, 64 << 20, 65536}, false, 0, 0)
	tune.debtGuardOff = true
	if err := tune.observe("a", backlogTestSample(1), storageSample{WriteStallActive: true}); err == nil {
		t.Fatal("write stall admitted with debt guard off")
	}
}

func TestDebtGuardOffSendsHeader(t *testing.T) {
	for _, off := range []bool{false, true} {
		var got string
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			got = r.Header.Get(chunkstore.TransferDebtGuardHeader)
			_, _ = w.Write([]byte("{}"))
		}))
		m := &mover{options: options{engine: "metric", debtGuardOff: off}, client: server.Client()}
		var out map[string]any
		if _, err := m.requestOnce(t.Context(), server.URL, "/v1/transfer/status", nil, &out); err != nil {
			t.Fatal(err)
		}
		server.Close()
		if (got == "off") != off {
			t.Fatalf("debtGuardOff=%v sent %q", off, got)
		}
	}
}
