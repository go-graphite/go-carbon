package v2life

import (
	"context"
	"time"

	"github.com/go-graphite/go-carbon/persister"
)

type ExpirationPolicy struct {
	Rules    persister.WhisperExpirationRules
	Fallback time.Duration
	DryRun   bool
}
type ExpirationPlan struct {
	Initialize  []Metric
	Delete      []Tombstone
	WouldExpire int
}

// PlanExpiration uses the current engine's parser/matcher, including first
// match, grace, exclusion, timed 0s exemptions and fallback. Pending includes
// unsealed owner buffers and replay; callers serialize plan+apply with seal
// activity or revalidate revisions. LastUpdate is arrival time, not sample time.
func PlanExpiration(s Snapshot, p ExpirationPolicy, now time.Time, pending func(string) bool) ExpirationPlan {
	var out ExpirationPlan
	for _, m := range s.Metrics {
		if m.Metadata.LastUpdate.IsZero() {
			if !p.DryRun {
				m = cloneMetric(m)
				m.Metadata.LastUpdate = now
				m.Metadata.Revision++
				out.Initialize = append(out.Initialize, m)
			}
			continue
		}
		ttl := p.Fallback
		if i := p.Rules.MatchIndex(m.Metadata.Name, now); i >= 0 {
			ttl = p.Rules[i].TTL()
		}
		if ttl <= 0 || now.Sub(m.Metadata.LastUpdate) < ttl || (pending != nil && pending(m.Metadata.Name)) {
			continue
		}
		out.WouldExpire++
		if !p.DryRun {
			out.Delete = append(out.Delete, Tombstone{Scope: m.Scope, Name: m.Metadata.Name, ThroughGeneration: m.Metadata.Generation, Revision: m.Metadata.Revision})
		}
	}
	return out
}

func (o *Owner) Expire(ctx context.Context, p ExpirationPolicy, now time.Time, pending func(string) bool) (ExpirationPlan, error) {
	plan := PlanExpiration(o.Snapshot(), p, now, pending)
	if len(plan.Initialize)+len(plan.Delete) == 0 {
		return plan, nil
	}
	return plan, o.Apply(ctx, plan.Initialize, plan.Delete)
}
