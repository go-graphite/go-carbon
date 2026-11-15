package downsize

import (
	"context"
	"fmt"
	"testing"

	"github.com/go-graphite/go-carbon/buckyd"
	"github.com/go-graphite/go-carbon/internal/chunkstore"
)

type expiryFixture struct {
	*readyFixture
	evidence map[string]map[string]buckyd.ExpirationEvidence
}

func (f *expiryFixture) ExpirationEvidence(_ context.Context, store string, names []string, _ bool) (map[string]buckyd.ExpirationEvidence, error) {
	out := map[string]buckyd.ExpirationEvidence{}
	for _, n := range names {
		if e, ok := f.evidence[store][n]; ok {
			out[n] = e
		}
	}
	return out, nil
}
func newExpiryFixture(t *testing.T) (*expiryFixture, syncOptions, syncReadyOptions) {
	base, o, r := newReadyFixture(t, 120)
	f := &expiryFixture{base, map[string]map[string]buckyd.ExpirationEvidence{}}
	name := "expired.old"
	for _, source := range o.from {
		f.store(source).archives[name] = []syncArchive{{Step: 3600, Points: 240}}
		f.store(source).points[name] = map[int64]map[int64]float64{3600: {syncTestNow - 6*86400: 42}}
		f.evidence[source] = map[string]buckyd.ExpirationEvidence{name: {Latest: syncTestNow - 6*86400}}
	}
	f.evidence[o.to] = map[string]buckyd.ExpirationEvidence{name: {Policy: "exact-policy-v1", TTLSeconds: 4 * 86400, Receipt: &chunkstore.ExpirationReceipt{Policy: "exact-policy-v1", DeletedAt: syncTestNow - 60, LastUpdate: syncTestNow - 6*86400}}}
	return f, o, r
}
func TestExpectedAbsencePreservesReadyWithoutRefill(t *testing.T) {
	f, o, r := newExpiryFixture(t)
	if err := runSyncReady(context.Background(), &o, r, f, func() error { return runSync(context.Background(), o, f) }); err != nil {
		t.Fatal(err)
	}
	report := readReadyReport(t, o.state)
	if report.Verdict != "READY" || report.ExpectedAbsent != 1 {
		t.Fatalf("%+v", report)
	}
	for _, copy := range f.copies {
		if copy != "" {
			t.Fatal("unexpected copy", copy)
		}
	}
	if _, ok := f.store(o.to).archives["expired.old"]; ok {
		t.Fatal("expired metric refilled")
	}
	// Reviving data on any source immediately removes the exclusion.
	e := f.evidence[o.from[2]]["expired.old"]
	e.Latest = syncTestNow - 60
	f.evidence[o.from[2]]["expired.old"] = e
	got, err := expectedAbsent(context.Background(), f, o, []string{"expired.old"})
	if err != nil || got["expired.old"] {
		t.Fatal("stale proof survived write", got, err)
	}
}
func TestExpectedAbsenceRejectsUnprovenPolicyAndActivity(t *testing.T) {
	for _, variant := range []string{"receipt", "policy", "grace-boundary", "pending", "source-missing", "target-present", "future-receipt", "source-future"} {
		t.Run(variant, func(t *testing.T) {
			f, o, _ := newExpiryFixture(t)
			n := "expired.old"
			e := f.evidence[o.to][n]
			switch variant {
			case "receipt":
				e.Receipt = nil
			case "policy":
				e.Policy = "changed"
			case "grace-boundary":
				e.Latest = 0
				v := f.evidence[o.from[1]][n]
				v.Latest = syncTestNow - 4*86400
				f.evidence[o.from[1]][n] = v
			case "pending":
				v := f.evidence[o.from[1]][n]
				v.Pending = true
				f.evidence[o.from[1]][n] = v
			case "source-missing":
				delete(f.evidence[o.from[2]], n)
			case "target-present":
				e.Latest = syncTestNow - 7*86400
			case "future-receipt":
				e.Receipt.DeletedAt = syncTestNow + 1
			case "source-future":
				v := f.evidence[o.from[2]][n]
				v.Latest = syncTestNow + 86400
				f.evidence[o.from[2]][n] = v
			}
			f.evidence[o.to][n] = e
			got, err := expectedAbsent(context.Background(), f, o, []string{n})
			if err != nil || got[n] {
				t.Fatal(got, err)
			}
		})
	}
}
func TestExpectedAbsenceExistingReadyVerdicts(t *testing.T) {
	for _, hole := range []bool{false, true} {
		t.Run(fmt.Sprint(hole), func(t *testing.T) {
			base, o, r := newReadyFixture(t, 120)
			if hole {
				delete(base.store(o.to).points["metric.001"][3600], syncTestNow-7200)
			}
			f := &expiryFixture{base, map[string]map[string]buckyd.ExpirationEvidence{}}
			if err := runSyncReady(context.Background(), &o, r, f, func() error { return runSync(context.Background(), o, f) }); err != nil {
				t.Fatal(err)
			}
			if readReadyReport(t, o.state).Verdict != "READY" {
				t.Fatal("strict verdict changed")
			}
		})
	}
}

func TestExpectedAbsenceInvalidProofStillRepairs(t *testing.T) {
	f, o, r := newExpiryFixture(t)
	n := "expired.old"
	e := f.evidence[o.from[2]][n]
	e.Latest = syncTestNow - 60
	f.evidence[o.from[2]][n] = e
	if err := runSyncReady(context.Background(), &o, r, f, func() error { return runSync(context.Background(), o, f) }); err != nil {
		t.Fatal(err)
	}
	if len(f.copies) == 0 {
		t.Fatal("recent non-owning source did not require copy")
	}
	if _, ok := f.store(o.to).archives[n]; !ok {
		t.Fatal("invalid exemption hid absent target")
	}
	if readReadyReport(t, o.state).ExpectedAbsent != 0 {
		t.Fatal("invalid exemption counted")
	}
}

func TestExpectedAbsenceNoCopyAuditDoesNotRequireExpiredNames(t *testing.T) {
	f, o, r := newExpiryFixture(t)
	o.noCopy = true
	if err := runSyncReady(context.Background(), &o, r, f, func() error { return runSync(context.Background(), o, f) }); err != nil {
		t.Fatal(err)
	}
	if readReadyReport(t, o.state).ExpectedAbsent != 1 {
		t.Fatal("expiry not accounted")
	}
	if len(f.copies) != 0 {
		t.Fatal("audit copied")
	}
}
