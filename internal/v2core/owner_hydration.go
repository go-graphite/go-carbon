package v2core

import (
	"errors"

	"github.com/cockroachdb/pebble"
	"github.com/go-graphite/go-carbon/internal/v2dict"
	"github.com/go-graphite/go-carbon/internal/v2part"
)

// ensureHydrated gates discovery, publication, seals and background maintenance.
// Exact immutable reads validate their own rows. Recovery only reconstructs RAM;
// a kill can repeat it from parts.
func (o *coreOwner) ensureHydrated() error {
	o.hydrateOnce.Do(func() {
		if o.hydrate != nil {
			o.hydrateErr = o.hydrate()
			if o.hydrateErr != nil {
				o.failure.CompareAndSwap(nil, &coreFailure{err: o.hydrateErr})
			}
		}
		if o.hydrateErr == nil {
			o.hydrated.Store(true)
		}
	})
	return o.hydrateErr
}

// deferOwnerRollups checks only descriptor/namespace metadata. Copied, mixed
// name-keyed and legacy stores retain their eager validation/adoption path.
// ID-keyed owners reuse the existing dictionary when their first demand arrives.
func deferOwnerRollups(db *pebble.DB, dict *v2dict.Dict) (bool, error) {
	if dict == nil {
		return false, nil
	}
	snap := db.NewSnapshot()
	defer snap.Close()
	copied, err := snapshotHasCopiedParts(snap)
	if err != nil || copied {
		return false, err
	}
	legacy, err := snapshotHasLegacyRows(snap)
	if err != nil || legacy {
		return false, err
	}
	it, err := snap.NewIter(&pebble.IterOptions{LowerBound: sealedPartPrefix, UpperBound: v2part.PrefixEnd(sealedPartPrefix)})
	if err != nil {
		return false, err
	}
	found, eligible := false, true
	for ok := it.First(); ok; ok = it.Next() {
		d, e := v2part.ParseDescriptor(it.Value())
		if e != nil {
			return false, errors.Join(e, it.Close())
		}
		found = true
		eligible = eligible && descriptorByID(d)
	}
	return found && eligible, errors.Join(it.Error(), it.Close())
}

// hydrateForMaintenance admits one background recovery per engine. Busy owners
// retry on their next maintenance tick; request, publication and seal fences do
// not queue behind unrelated background owners.
func (o *coreOwner) hydrateForMaintenance() (bool, error) {
	if gate := o.backgroundHydration; gate != nil {
		if !gate.TryLock() {
			return false, nil
		}
		defer gate.Unlock()
	}
	return true, o.ensureHydrated()
}
