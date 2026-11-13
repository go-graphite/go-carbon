package buckyd

import (
	"net/http/httptest"
	"runtime"
	"testing"

	"github.com/go-graphite/go-carbon/internal/chunkstore"
)

func TestTransferPressureDebtWaiverKeepsHardStops(t *testing.T) {
	const l0Stop, debt = 20, 64 << 30
	for _, tc := range []struct {
		name        string
		stats       chunkstore.Stats
		guard, free bool // 429 with the guard on / with the debt guard waived
	}{
		{"idle", chunkstore.Stats{}, false, false},
		{"debt-over-cap", chunkstore.Stats{CompactionDebt: debt + 1}, true, false},
		{"write-stall", chunkstore.Stats{WriteStallActive: true}, true, true},
		{"l0-near-stop", chunkstore.Stats{L0Sublevels: 15}, true, true},
		{"debt-and-l0", chunkstore.Stats{CompactionDebt: debt + 1, L0Sublevels: 15}, true, true},
	} {
		if got := transferPressure(tc.stats, l0Stop, debt, false); got != tc.guard {
			t.Errorf("%s: guarded pressure %v want %v", tc.name, got, tc.guard)
		}
		if got := transferPressure(tc.stats, l0Stop, debt, true); got != tc.free {
			t.Errorf("%s: waived pressure %v want %v", tc.name, got, tc.free)
		}
	}
	r := httptest.NewRequest("POST", "/v1/transfer/pull", nil)
	if transferDebtGuardOff(r) {
		t.Fatal("waiver without header")
	}
	r.Header.Set(chunkstore.TransferDebtGuardHeader, "off")
	if !transferDebtGuardOff(r) {
		t.Fatal("header not honoured")
	}
}

// max_transfers applies on SIGHUP: raising it admits more transfers at once,
// lowering it stops new admissions until the active count drains.
func TestSetMaxTransfersLive(t *testing.T) {
	s := &Service{}
	s.SetMaxTransfers(2)
	if !s.acquireTransfer() || !s.acquireTransfer() || s.acquireTransfer() {
		t.Fatal("cap 2 not enforced")
	}
	s.SetMaxTransfers(3)
	if s.MaxTransfers() != 3 || !s.acquireTransfer() || s.acquireTransfer() {
		t.Fatal("raised cap not applied live")
	}
	s.SetMaxTransfers(1)
	s.releaseTransfer()
	if s.acquireTransfer() {
		t.Fatal("lowered cap admitted while 2 transfers still run")
	}
	s.releaseTransfer()
	s.releaseTransfer()
	if !s.acquireTransfer() || s.acquireTransfer() {
		t.Fatal("lowered cap wrong after drain")
	}
	s.SetMaxTransfers(0)
	if s.MaxTransfers() != defaultMaxTransfers() {
		t.Fatal("zero did not select the CPU default")
	}
}

// The default never throttles copies below 128 concurrent transfers and
// grows with the CPU count past that.
func TestMaxTransfersDefault(t *testing.T) {
	for cpus, want := range map[int]int{1: 128, 8: 128, 32: 128, 64: 128, 128: 128, 192: 192, 384: 384} {
		if got := maxTransfersFor(cpus); got != want {
			t.Errorf("maxTransfersFor(%d) = %d, want %d", cpus, got, want)
		}
	}
	if got := defaultMaxTransfers(); got < 128 || got < runtime.NumCPU() {
		t.Fatalf("defaultMaxTransfers() = %d on %d CPUs", got, runtime.NumCPU())
	}
}
