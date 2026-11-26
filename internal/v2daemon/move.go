package v2daemon

import (
	"context"
	"errors"
	"slices"

	"github.com/go-graphite/go-carbon/internal/v2core"
	"github.com/go-graphite/go-carbon/internal/v2recv"
)

// Flush seals every application accepted by this serial producer and waits for
// its core publication. It does not add an application or change source order.
func (p *Producer) Flush() error {
	if err := p.Drain(); err != nil {
		return err
	}
	if p.cursor == 0 {
		return nil
	}
	if p.layer == 3 {
		return p.fail(p.engine.AppendBatches(p.index, nil, p.cursor, true))
	}
	return p.fail(p.engine.Append(p.index, nil, p.cursor, true))
}

// AdoptMove is called by the producer's existing serial caller, just like
// AppendApplications or Drain. TCP callers schedule it with Server.OnOwner.
// Preparation and file checksums run outside the live owner's write queue.
// The catalog is durable before the atomic core attachment, so either side of
// a crash resolves canonical names to the same receiving numeric identities.
func (p *Producer) AdoptMove(ctx context.Context, prepared *v2core.PreparedMove, now int64) error {
	if p.layer != 3 || p.count != 1 {
		return errors.New("producer adoption requires one L3 producer; use MoveAdmin for multiple producers")
	}
	if p.failed != nil {
		return p.failed
	}
	if prepared == nil {
		return errors.New("move preparation required")
	}
	ids, err := p.prepareMoveIDs(prepared)
	if err != nil {
		return err
	}
	return p.engine.AdoptMove(ctx, prepared, ids, now)
}

func (p *Producer) prepareMoveIDs(prepared *v2core.PreparedMove) (map[string]uint64, error) {
	if err := p.openCatalog(); err != nil {
		return nil, err
	}
	policies := prepared.Policies()
	names := make([]string, 0, len(policies))
	for name := range policies {
		if v2recv.OwnerFor(name, p.count) == p.index {
			names = append(names, name)
		}
	}
	slices.Sort(names)
	ids := map[string]uint64{}
	for _, name := range names {
		id, err := p.catalog.Intern(name, policies[name])
		if err != nil {
			return nil, err
		}
		ids[name] = p.applications[id].ID()
	}
	if err := p.catalog.Sync(); err != nil {
		return nil, p.fail(err)
	}
	if err := p.Flush(); err != nil {
		return nil, err
	}
	return ids, nil
}
