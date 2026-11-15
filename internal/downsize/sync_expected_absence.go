package downsize

import (
	"context"
	"errors"

	"github.com/go-graphite/go-carbon/buckyd"
)

type expirationEvidenceBackend interface {
	ExpirationEvidence(context.Context, string, []string, bool) (map[string]buckyd.ExpirationEvidence, error)
}

func (s *syncer) expectedAbsent(ctx context.Context, names []string) (map[string]bool, error) {
	out, err := expectedAbsent(ctx, s.b, s.o, names)
	if err != nil {
		return nil, err
	}
	for _, name := range names {
		if out[name] {
			s.expectedAbsences.Store(name, true)
		} else {
			s.expectedAbsences.Delete(name)
		}
	}
	return out, nil
}

// expectedAbsent is shared by verification and final parity. It never caches a
// positive result: a write or policy reload must invalidate the next verdict.
func expectedAbsent(ctx context.Context, b syncBackend, o syncOptions, names []string) (map[string]bool, error) {
	out := map[string]bool{}
	backend, ok := b.(expirationEvidenceBackend)
	if !ok || len(names) == 0 {
		return out, nil
	}
	target, err := backend.ExpirationEvidence(ctx, o.to, names, true)
	if err != nil {
		return nil, err
	}
	candidates := []string{}
	for _, name := range names {
		e, ok := target[name]
		if !ok || e.Pending || e.Latest != 0 || e.Policy == "" || e.TTLSeconds <= 0 || e.Receipt == nil || e.Receipt.Policy != e.Policy {
			continue
		}
		cutoff := o.now().Unix() - e.TTLSeconds
		if e.Receipt.DeletedAt <= 0 || e.Receipt.DeletedAt > o.now().Unix() || e.Receipt.LastUpdate >= cutoff || e.Receipt.LastUpdate >= e.Receipt.DeletedAt-e.TTLSeconds {
			continue
		}
		candidates = append(candidates, name)
		out[name] = true
	}
	if len(candidates) == 0 {
		return out, nil
	}
	for _, source := range o.from {
		evidence, err := backend.ExpirationEvidence(ctx, source, candidates, false)
		if err != nil {
			return nil, err
		}
		for _, name := range candidates {
			e, ok := evidence[name]
			if !ok || e.Pending || e.Latest >= o.now().Unix()-target[name].TTLSeconds {
				delete(out, name)
			}
		}
	}
	if len(out) > 0 {
		fresh, err := backend.ExpirationEvidence(ctx, o.to, candidates, true)
		if err != nil {
			return nil, err
		}
		for _, name := range candidates {
			e, ok := fresh[name]
			old := target[name]
			if !ok || e.Pending || e.Latest != 0 || e.Policy != old.Policy || e.TTLSeconds != old.TTLSeconds || e.Receipt == nil || old.Receipt == nil || *e.Receipt != *old.Receipt {
				delete(out, name)
			}
		}
	}
	if len(out) > 0 {
		sample := ""
		for _, n := range names {
			if out[n] {
				sample = n
				break
			}
		}
		event("sync_expected_absent", map[string]any{"names": len(out), "metric_sample": sample, "reason": "target_expiration", "sources_checked": len(o.from)})
	}
	return out, nil
}

func (b *syncLiveBackend) ExpirationEvidence(ctx context.Context, store string, names []string, receiptsOnly bool) (map[string]buckyd.ExpirationEvidence, error) {
	out := map[string]buckyd.ExpirationEvidence{}
	for start := 0; start < len(names); start += 128 {
		subset := names[start:min(start+128, len(names))]
		var response map[string]buckyd.ExpirationEvidence
		err := b.wait(ctx, "expiration evidence", store, func() error {
			_, err := b.m.requestOnce(ctx, store, "/v1/expiration/evidence", map[string]any{"Names": subset, "ReceiptsOnly": receiptsOnly}, &response)
			return err
		})
		// Older daemons have no evidence: retain their ordinary strict parity.
		var api *apiError
		if errors.As(err, &api) && (api.Status == 404 || api.Status == 501) {
			return out, nil
		}
		if err != nil {
			return nil, err
		}
		for name, e := range response {
			out[name] = e
		}
	}
	return out, nil
}
