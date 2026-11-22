package downsize

import (
	"context"
	"errors"
	"sync"

	"github.com/go-graphite/go-carbon/buckyd"
	"golang.org/x/sync/errgroup"
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
	// Every donor must independently show no recent point. The donors are
	// different stores, so ask them at once; removals commute, so the verdict
	// is the same as asking them one after another.
	var mu sync.Mutex
	donors, work := errgroup.WithContext(ctx)
	for _, source := range o.from {
		donors.Go(func() error {
			evidence, err := backend.ExpirationEvidence(work, source, candidates, false)
			if err != nil {
				return err
			}
			mu.Lock()
			defer mu.Unlock()
			for _, name := range candidates {
				e, ok := evidence[name]
				if !ok || e.Pending || e.Latest >= o.now().Unix()-target[name].TTLSeconds {
					delete(out, name)
				}
			}
			return nil
		})
	}
	if err := donors.Wait(); err != nil {
		return nil, err
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
	ctx, cancel := b.readContext(ctx)
	defer cancel()
	out := map[string]buckyd.ExpirationEvidence{}
	var mu sync.Mutex
	// Stores accept at most 128 names per request, so a copy wave needs many
	// requests. Run them at the stream budget, shared by every store and caller
	// (donors are asked concurrently), instead of a fixed per-call cap.
	streams := max(1, b.o.copyStreams)
	b.evidenceOnce.Do(func() { b.evidenceSlots = make(chan struct{}, streams) })
	group, work := errgroup.WithContext(ctx)
	group.SetLimit(streams)
	unavailable := errors.New("expiration evidence unavailable")
	for start := 0; start < len(names); start += 128 {
		if work.Err() != nil {
			break
		}
		subset := names[start:min(start+128, len(names))]
		group.Go(func() error {
			select {
			case b.evidenceSlots <- struct{}{}:
			case <-work.Done():
				return work.Err()
			}
			defer func() { <-b.evidenceSlots }()
			var response map[string]buckyd.ExpirationEvidence
			err := b.wait(work, "expiration evidence", store, func() error {
				_, err := b.m.requestOnce(work, store, "/v1/expiration/evidence", map[string]any{"Names": subset, "ReceiptsOnly": receiptsOnly}, &response)
				return err
			})
			var api *apiError
			if errors.As(err, &api) && (api.Status == 404 || api.Status == 501) {
				return unavailable
			}
			if err != nil {
				return err
			}
			mu.Lock()
			for name, e := range response {
				out[name] = e
			}
			mu.Unlock()
			return nil
		})
	}
	if err := group.Wait(); err != nil {
		// Older daemons have no evidence: retain ordinary strict parity.
		if errors.Is(err, unavailable) && ctx.Err() == nil {
			return map[string]buckyd.ExpirationEvidence{}, nil
		}
		return nil, err
	}
	return out, nil
}
