package v2core

import (
	"bytes"
	"context"
	"errors"
	"slices"

	"github.com/cockroachdb/pebble"
	"github.com/go-graphite/go-carbon/internal/v2part"
)

// The core's atomic descriptor bindings are the source of truth for its sealed
// part inventory. No live filesystem enumeration or previous snapshot is held.
// Bare GCP3 readers retain the scan fallback in NewRollupSnapshot.
func rollupInventory(ctx context.Context, snapshot *pebble.Snapshot, prior *RollupSnapshot) (map[string][]string, []string, error) {
	if prior == nil {
		return initialRollupInventory(ctx, snapshot)
	}
	it, err := snapshot.NewIter(&pebble.IterOptions{LowerBound: sealedPartPrefix, UpperBound: v2part.PrefixEnd(sealedPartPrefix)})
	if err != nil {
		return nil, nil, err
	}
	defer it.Close()
	var inventory map[string][]string
	if prior != nil && prior.inventory != nil {
		inventory = map[string][]string{}
	} else {
		// A core frontier establishes the atomic binding contract even when the
		// last part has retired. GCP3 data may remain in the LSM after retirement.
		_, closer, err := snapshot.Get(coreMetaKey)
		if err == nil {
			if err := closer.Close(); err != nil {
				return nil, nil, err
			}
			inventory = map[string][]string{}
		} else if !errors.Is(err, pebble.ErrNotFound) {
			return nil, nil, err
		}
	}
	var scopes []string
	var data *pebble.Iterator
	defer func() {
		if data != nil {
			_ = data.Close()
		}
	}()
	for valid := it.First(); valid; valid = it.Next() {
		if err := ctx.Err(); err != nil {
			return nil, nil, err
		}
		if inventory == nil {
			inventory = map[string][]string{}
		}
		id := string(it.Key())
		if prior != nil {
			if known, ok := prior.inventory[id]; ok {
				inventory[id] = known
				scopes = append(scopes, known...)
				continue
			}
		}
		d, err := v2part.ParseDescriptor(it.Value())
		if err != nil {
			return nil, nil, err
		}
		if !bytes.Equal(it.Key()[len(sealedPartPrefix):], d.SHA256[:]) {
			return nil, nil, errors.New("sealed descriptor binding mismatch")
		}
		if d.Scope.Partition != 0 {
			return nil, nil, errors.New("unexpected rollup partition")
		}
		first, err := v2part.Decode(d.FirstKey)
		if err != nil {
			return nil, nil, err
		}
		last, err := v2part.Decode(d.LastKey)
		if err != nil {
			return nil, nil, err
		}
		if data == nil {
			data, err = snapshot.NewIter(nil)
			if err != nil {
				return nil, nil, err
			}
		}
		var added []string
		for v := uint32(first.VShard); v <= uint32(last.VShard); v++ {
			scope, err := v2part.PartitionPrefixForGroup(d.Scope.Group, uint16(v), 0)
			if err != nil {
				return nil, nil, err
			}
			scope = append(scope, d.Scope.Part[:]...)
			if data.SeekGE(scope) && bytes.HasPrefix(data.Key(), scope) {
				added = append(added, string(scope))
			}
			if err := data.Error(); err != nil {
				return nil, nil, err
			}
			if err := ctx.Err(); err != nil {
				return nil, nil, err
			}
		}
		if len(added) == 0 {
			return nil, nil, errors.New("sealed descriptor has no rollup scopes")
		}
		inventory[id] = added
		scopes = append(scopes, added...)
	}
	slices.Sort(scopes)
	return inventory, scopes, it.Error()
}

// Startup walks physical scopes in key order once. Probing every virtual shard
// separately for every descriptor rereads SST blocks after the working set grows
// beyond the block cache. Atomic bindings still decide which scopes are live.
func initialRollupInventory(ctx context.Context, snapshot *pebble.Snapshot) (map[string][]string, []string, error) {
	type identity struct {
		group     v2part.Group
		partition int64
		part      v2part.PartID
	}
	type binding struct {
		id    string
		scope v2part.Scope
	}
	pending := map[identity][]binding{}
	inventory := map[string][]string{}
	it, err := snapshot.NewIter(&pebble.IterOptions{LowerBound: sealedPartPrefix, UpperBound: v2part.PrefixEnd(sealedPartPrefix)})
	if err != nil {
		return nil, nil, err
	}
	defer it.Close()
	for it.First(); it.Valid(); it.Next() {
		if err := ctx.Err(); err != nil {
			return nil, nil, err
		}
		d, err := v2part.ParseDescriptor(it.Value())
		if err != nil {
			return nil, nil, err
		}
		if !bytes.Equal(it.Key()[len(sealedPartPrefix):], d.SHA256[:]) || d.Scope.Partition != 0 {
			return nil, nil, errors.New("invalid sealed descriptor binding")
		}
		id := string(it.Key())
		k := identity{d.Scope.Group, d.Scope.Partition, d.Scope.Part}
		pending[k] = append(pending[k], binding{id, d.Scope})
		inventory[id] = nil
	}
	if err := it.Error(); err != nil {
		return nil, nil, err
	}
	if len(inventory) == 0 {
		_, closer, err := snapshot.Get(coreMetaKey)
		if err == nil {
			return inventory, nil, closer.Close()
		}
		if errors.Is(err, pebble.ErrNotFound) {
			return nil, nil, nil
		}
		return nil, nil, err
	}
	prefix := []byte{'G', 'C', 'P', 3}
	data, err := snapshot.NewIter(&pebble.IterOptions{LowerBound: prefix, UpperBound: v2part.PrefixEnd(prefix)})
	if err != nil {
		return nil, nil, err
	}
	defer data.Close()
	var scopes []string
	for valid := data.First(); valid; {
		if err := ctx.Err(); err != nil {
			return nil, nil, err
		}
		key, err := v2part.Decode(data.Key())
		if err != nil {
			return nil, nil, err
		}
		scope, err := v2part.PartitionPrefixForGroup(key.Group, key.VShard, key.Partition)
		if err != nil {
			return nil, nil, err
		}
		scope = append(scope, key.Part[:]...)
		for _, b := range pending[identity{key.Group, key.Partition, key.Part}] {
			if b.scope.VShard == v2part.GroupVShard || b.scope.VShard == key.VShard {
				id := string(scope)
				inventory[b.id] = append(inventory[b.id], id)
				scopes = append(scopes, id)
			}
		}
		valid = data.SeekGE(v2part.PrefixEnd(scope))
	}
	if err := data.Error(); err != nil {
		return nil, nil, err
	}
	for _, scopes := range inventory {
		if len(scopes) == 0 {
			return nil, nil, errors.New("sealed descriptor has no rollup scopes")
		}
	}
	return inventory, scopes, nil
}
