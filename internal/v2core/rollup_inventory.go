package v2core

import (
	"bytes"
	"context"
	"errors"
	"maps"
	"slices"

	"github.com/cockroachdb/pebble"
	"github.com/go-graphite/go-carbon/internal/v2part"
)

// The core's atomic descriptor bindings are the source of truth for its sealed
// part inventory. No live filesystem enumeration or previous snapshot is held.
// Bare GCP3 readers retain the scan fallback in NewRollupSnapshot.
// RollupInventory is an immutable set of the atomically committed descriptor
// bindings. It contains no snapshot or live writer objects. The core copies the
// small map on seal/retirement and shares each existing descriptor's bytes.
type RollupInventory struct {
	bindings map[string][]byte
	bytes    int64
	registry *partRegistry // opened parts, shared by every inventory derived from one load
}

func loadRollupInventory(db *pebble.DB) (*RollupInventory, error) {
	out := &RollupInventory{bindings: map[string][]byte{}, registry: &partRegistry{}}
	it, err := db.NewIter(&pebble.IterOptions{LowerBound: sealedPartPrefix, UpperBound: v2part.PrefixEnd(sealedPartPrefix)})
	if err != nil {
		return nil, err
	}
	defer it.Close()
	for valid := it.First(); valid; valid = it.Next() {
		out.bindings[string(it.Key())] = bytes.Clone(it.Value())
		out.bytes += int64(len(it.Key()) + len(it.Value()) + 64)
	}
	return out, it.Error()
}
func (s *RollupInventory) with(added []coreKV, removed map[v2part.Digest]bool) *RollupInventory {
	if len(added) == 0 && len(removed) == 0 {
		return s
	}
	out := &RollupInventory{bindings: maps.Clone(s.bindings), bytes: s.bytes, registry: s.registry}
	for _, kv := range added {
		if old, ok := out.bindings[string(kv.Key)]; ok {
			out.bytes -= int64(len(kv.Key) + len(old) + 64)
		}
		out.bindings[string(kv.Key)] = kv.Value
		out.bytes += int64(len(kv.Key) + len(kv.Value) + 64)
	}
	for id := range removed {
		key := string(partBinding(sealedPartPrefix, id))
		if old, ok := out.bindings[key]; ok {
			out.bytes -= int64(len(key) + len(old) + 64)
		}
		delete(out.bindings, key)
	}
	return out
}

func rollupInventory(ctx context.Context, snapshot *pebble.Snapshot, prior *RollupSnapshot, known *RollupInventory, selected ...map[string]v2part.Descriptor) (map[string][]string, []string, error) {
	inventory := map[string][]string{}
	var scopes []string
	var legacy map[string][]string
	// Pending-only publications may share the complete immutable inventory.
	if known != nil && prior != nil && known == prior.bindingSet {
		for _, ids := range prior.inventory {
			scopes = append(scopes, ids...)
		}
		return prior.inventory, scopes, ctx.Err()
	}
	consume := func(id string, value []byte) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if prior != nil {
			if known, ok := prior.inventory[id]; ok {
				inventory[id] = known
				scopes = append(scopes, known...)
				return nil
			}
		}
		d, err := v2part.ParseDescriptor(value)
		if err != nil {
			return err
		}
		if len(id) != len(sealedPartPrefix)+32 || !bytes.Equal([]byte(id)[len(sealedPartPrefix):], d.SHA256[:]) {
			return errors.New("sealed descriptor binding mismatch")
		}
		var added []string
		shards := d.VShards
		if len(shards) == 0 && d.Scope.VShard != v2part.GroupVShard {
			shards = []uint16{d.Scope.VShard}
		}
		for _, v := range shards {
			scope, err := scopePrefix(d.Scope.Group, v, d.Scope.Partition, descriptorByID(d))
			if err != nil {
				return err
			}
			scope = append(scope, d.Scope.Part[:]...)
			added = append(added, string(scope))
		}
		if len(shards) == 0 {
			// Older group descriptors lack the seal-derived inventory. Walk
			// actual physical scopes once, never every absent vshard per part.
			if legacy == nil {
				legacy, _, err = initialRollupInventory(ctx, snapshot)
			}
			if err != nil {
				return err
			}
			added = legacy[id]
		}
		if len(added) == 0 {
			return errors.New("sealed descriptor has no rollup scopes")
		}
		inventory[id] = added
		if len(selected) > 0 {
			for _, scope := range added {
				selected[0][scope] = d
			}
		}
		scopes = append(scopes, added...)
		return nil
	}
	if known != nil {
		// Old parts share their immutable scopes. New seals carry exact shards
		// in their descriptor, so inventory loading performs no archive seeks.
		for id, value := range known.bindings {
			if err := consume(id, value); err != nil {
				return nil, nil, err
			}
		}
		return inventory, scopes, ctx.Err()
	}
	it, err := snapshot.NewIter(&pebble.IterOptions{LowerBound: sealedPartPrefix, UpperBound: v2part.PrefixEnd(sealedPartPrefix)})
	if err != nil {
		return nil, nil, err
	}
	defer it.Close()
	authoritative := prior != nil && prior.inventory != nil
	if !authoritative {
		_, closer, err := snapshot.Get(coreMetaKey)
		if err == nil {
			authoritative = true
			if err = closer.Close(); err != nil {
				return nil, nil, err
			}
		} else if !errors.Is(err, pebble.ErrNotFound) {
			return nil, nil, err
		}
	}
	for valid := it.First(); valid; valid = it.Next() {
		authoritative = true
		if err := consume(string(it.Key()), it.Value()); err != nil {
			return nil, nil, err
		}
	}
	if !authoritative {
		inventory = nil
	}
	slices.Sort(scopes)
	return inventory, scopes, it.Error()
}

// Legacy descriptors walk physical scopes in key order once. Probing every virtual shard
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
		if !bytes.Equal(it.Key()[len(sealedPartPrefix):], d.SHA256[:]) {
			return nil, nil, errors.New("invalid sealed descriptor binding")
		}
		if descriptorByID(d) {
			continue // ID-keyed descriptors always carry their vshards
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
	data, err := newArchiveIter(ctx, snapshot, &pebble.IterOptions{LowerBound: prefix, UpperBound: v2part.PrefixEnd(prefix)})
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
