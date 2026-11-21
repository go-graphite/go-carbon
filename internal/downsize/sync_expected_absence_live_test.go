package downsize

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/buckyd"
	"github.com/go-graphite/go-carbon/internal/chunkstore"
)

// expiryStores serves /v1/expiration/evidence for one target and its donors.
// Every name is expired on the target with a valid receipt and old on every
// donor, so each name passes through all three evidence stages.
type expiryStores struct {
	target  *httptest.Server
	donors  []*httptest.Server
	calls   atomic.Int64
	active  atomic.Int64
	peak    atomic.Int64
	overlap atomic.Int64 // requests that started while another donor was busy
	busy    []atomic.Int64
}

func newExpiryStores(t testing.TB, donors int, now time.Time, delay time.Duration) *expiryStores {
	s := &expiryStores{busy: make([]atomic.Int64, donors)}
	ttl := int64(4 * 86400)
	serve := func(donor int) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			n := s.active.Add(1)
			defer s.active.Add(-1)
			for old := s.peak.Load(); n > old; old = s.peak.Load() {
				if s.peak.CompareAndSwap(old, n) {
					break
				}
			}
			if donor >= 0 {
				s.busy[donor].Add(1)
				defer s.busy[donor].Add(-1)
				for i := range s.busy {
					if i != donor && s.busy[i].Load() > 0 {
						s.overlap.Add(1)
						break
					}
				}
			}
			s.calls.Add(1)
			var req struct {
				Names        []string
				ReceiptsOnly bool
			}
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil || len(req.Names) > 128 {
				http.Error(w, "bad request", 400)
				return
			}
			time.Sleep(delay)
			out := map[string]buckyd.ExpirationEvidence{}
			for _, name := range req.Names {
				if donor < 0 {
					out[name] = buckyd.ExpirationEvidence{Policy: "p", TTLSeconds: ttl, Receipt: &chunkstore.ExpirationReceipt{Policy: "p", DeletedAt: now.Unix() - 60, LastUpdate: now.Unix() - 6*86400}}
				} else {
					out[name] = buckyd.ExpirationEvidence{Latest: now.Unix() - 6*86400}
				}
			}
			_ = json.NewEncoder(w).Encode(out)
		}
	}
	s.target = httptest.NewServer(serve(-1))
	t.Cleanup(s.target.Close)
	for i := range donors {
		d := httptest.NewServer(serve(i))
		t.Cleanup(d.Close)
		s.donors = append(s.donors, d)
	}
	return s
}

func (s *expiryStores) options(now time.Time) syncOptions {
	o := syncOptions{to: s.target.URL, now: func() time.Time { return now }}
	for _, d := range s.donors {
		o.from = append(o.from, d.URL)
	}
	return o
}

// Donors are asked at once, and all evidence requests share one stream budget.
func TestExpectedAbsentDonorsShareStreamBudget(t *testing.T) {
	t.Parallel()
	now := time.Unix(syncTestNow, 0)
	stores := newExpiryStores(t, 2, now, 5*time.Millisecond)
	b := newSyncLiveBackend(syncLiveOptions{copyStreams: 8})
	names := copyNames("expired.", 128*16)
	got, err := expectedAbsent(context.Background(), b, stores.options(now), names)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != len(names) {
		t.Fatalf("verdict changed: %d/%d", len(got), len(names))
	}
	// target, two donors and the fresh target check: 16 requests each.
	if stores.calls.Load() != 4*16 || stores.peak.Load() > 8 || stores.overlap.Load() == 0 {
		t.Fatalf("calls=%d peak=%d donor overlap=%d", stores.calls.Load(), stores.peak.Load(), stores.overlap.Load())
	}
}

// BenchmarkExpectedAbsentLive measures one 32,000-name copy wave against
// stores answering each 128-name evidence request in 5 ms.
func BenchmarkExpectedAbsentLive(b *testing.B) {
	for _, streams := range []int{1, 64} {
		b.Run(map[int]string{1: "streams=1", 64: "streams=64"}[streams], func(b *testing.B) {
			now := time.Unix(syncTestNow, 0)
			stores := newExpiryStores(b, 2, now, 5*time.Millisecond)
			backend := newSyncLiveBackend(syncLiveOptions{copyStreams: streams})
			names := copyNames("expired.", 32000)
			o := stores.options(now)
			b.ResetTimer()
			for range b.N {
				got, err := expectedAbsent(context.Background(), backend, o, names)
				if err != nil || len(got) != len(names) {
					b.Fatal(len(got), err)
				}
			}
			b.ReportMetric(float64(stores.calls.Load())/float64(b.N)/float64(len(names))*1000, "calls/1k-names")
			b.ReportMetric(float64(len(names))*float64(b.N)/b.Elapsed().Seconds(), "names/s")
			b.ReportMetric(float64(stores.peak.Load()), "peak")
		})
	}
}
