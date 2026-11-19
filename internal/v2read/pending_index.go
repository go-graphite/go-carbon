package v2read

import (
	"context"
	"encoding/json"

	"github.com/go-graphite/go-carbon/internal/namefp"
	"github.com/go-graphite/go-carbon/internal/v2core"
)

// pendingName is the exact name predicate the linear scan used.
func pendingName(registered *v2core.ApplicationPolicy, policy []byte) (string, error) {
	if registered != nil {
		return registered.Name(), nil
	}
	var p v2core.RollupPolicy
	if err := json.Unmarshal(policy, &p); err != nil {
		return "", err
	}
	return string(p.Name), nil
}

// pendingHash is the hash of pendingName without reading name bytes when the
// application carries a registered identity.
func pendingHash(registered *v2core.ApplicationPolicy, policy []byte) (uint64, error) {
	if registered != nil {
		return registered.NameHash(), nil
	}
	name, err := pendingName(registered, policy)
	return namefp.Hash(name), err
}

// selectPending returns the applications whose exact name is in names, in
// arrival order. The request resolves only its own names to owner IDs: sealed
// names through their metadata in sealed, the others through the owner's
// published new-name list. It then scans the published rows by ID, and cold
// applications by their inline name hash, with no catalog lookup per position.
// Candidates are re-checked by exact name, so hash collisions cannot change
// results. Only matching applications are decoded.
func selectPending(ctx context.Context, sealed *v2core.RollupSnapshot, pending v2core.PendingView, names []string) ([]v2core.Batch, error) {
	if pending.Len() == 0 {
		return nil, nil
	}
	var hashes, ids namefp.Table // sets: keys in Hash, no local number
	for _, n := range names {
		hashes.Put(namefp.Fingerprint{Hash: namefp.Hash(n)}, 0)
		if sealed == nil {
			continue
		}
		if _, meta, ok := sealed.LookupMetadata(n); ok {
			ids.Put(namefp.Fingerprint{Hash: meta.ID}, 0)
		}
	}
	pending.NewNames(func(hash, id uint64) {
		if _, ok := hashes.Get(namefp.Fingerprint{Hash: hash}); ok {
			ids.Put(namefp.Fingerprint{Hash: id}, 0)
		}
	})
	var positions []int
	var err error
	pending.IDs(0, pending.Len(), func(pos int, id uint64, cold *v2core.Batch) bool {
		if cold == nil {
			if _, ok := ids.Get(namefp.Fingerprint{Hash: id}); ok {
				positions = append(positions, pos)
			}
			return true
		}
		var h uint64
		if h, err = pendingHash(cold.Registered, cold.Policy); err != nil {
			return false
		}
		if _, ok := hashes.Get(namefp.Fingerprint{Hash: h}); ok {
			positions = append(positions, pos)
		}
		return true
	})
	if err != nil || len(positions) == 0 {
		return nil, err
	}
	wanted := make(map[string]bool, len(names))
	for _, n := range names {
		wanted[n] = true
	}
	var out []v2core.Batch
	covered := -1 // positions before this belong to an application already taken
	for _, i := range positions {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if i < covered {
			continue
		}
		_, registered, policy := pending.Ref(i)
		name, err := pendingName(registered, policy)
		if err != nil {
			return nil, err
		}
		if wanted[name] {
			b, _, end := pending.Application(i)
			out = append(out, b)
			covered = end
		}
	}
	return out, nil
}
