package v2read

import (
	"context"
	"encoding/json"
	"slices"

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

// pendingRequestName reuses an exact request name after checking the admitted
// identity, instead of decoding its reverse dictionary entry for every batch.
func pendingRequestName(sealed *v2core.RollupSnapshot, registered *v2core.ApplicationPolicy, policy []byte, names []string) (string, error) {
	if registered != nil {
		for _, name := range names {
			if sealed.MatchesName(name, registered) {
				return name, nil
			}
		}
		return "", nil
	}
	name, err := pendingName(nil, policy)
	if err != nil {
		return "", err
	}
	if slices.Contains(names, name) {
		return name, nil
	}
	return "", nil
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
// published new-name list. It then binary-searches the owner's ID-sorted runs
// for those IDs, and matches cold applications by their inline name hash, with
// no catalog lookup and no scan per pending row.
// Candidates are re-checked by exact name, so hash collisions cannot change
// results. Only matching applications are decoded.
func selectPending(ctx context.Context, sealed *v2core.RollupSnapshot, pending v2core.PendingView, names []string) ([]v2core.Batch, error) {
	if pending.Len() == 0 {
		return nil, nil
	}
	var hashes namefp.Table // set: keys in Hash, no local number
	var ids []uint64
	for _, n := range names {
		hashes.Put(namefp.Fingerprint{Hash: namefp.Hash(n)}, 0)
		if sealed == nil {
			continue
		}
		if _, meta, ok := sealed.LookupMetadata(n); ok {
			ids = append(ids, meta.ID)
		}
	}
	pending.NewNames(func(hash, id uint64) {
		if _, ok := hashes.Get(namefp.Fingerprint{Hash: hash}); ok {
			ids = append(ids, id)
		}
	})
	slices.Sort(ids)
	positions, err := pending.Find(slices.Compact(ids), func(cold *v2core.Batch) (bool, error) {
		h, err := pendingHash(cold.Registered, cold.Policy)
		if err != nil {
			return false, err
		}
		_, ok := hashes.Get(namefp.Fingerprint{Hash: h})
		return ok, nil
	})
	if err != nil || len(positions) == 0 {
		return nil, err
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
		name, err := pendingRequestName(sealed, registered, policy, names)
		if err != nil {
			return nil, err
		}
		if name != "" {
			b, _, end := pending.Application(i)
			out = append(out, b)
			covered = end
		}
	}
	return out, nil
}
