package v2core

import (
	"bytes"
	"context"
	"errors"
	"maps"

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
}

func loadRollupInventory(db *pebble.DB) (*RollupInventory, error) {
	out := &RollupInventory{bindings: map[string][]byte{}}
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
	out := &RollupInventory{bindings: maps.Clone(s.bindings), bytes: s.bytes}
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

func rollupInventory(ctx context.Context, snapshot *pebble.Snapshot, prior *RollupSnapshot, known *RollupInventory) (map[string][]string, []string, error) {
	inventory := map[string][]string{}
	var scopes []string
	var data *archiveIter
	defer func() {
		if data != nil {
			_ = data.Close()
		}
	}()
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
		if d.Scope.Partition != 0 {
			return errors.New("unexpected rollup partition")
		}
		first, err := v2part.Decode(d.FirstKey)
		if err != nil {
			return err
		}
		last, err := v2part.Decode(d.LastKey)
		if err != nil {
			return err
		}
		if data == nil {
			data, err = newArchiveIter(ctx, snapshot, nil)
			if err != nil {
				return err
			}
		}
		var added []string
		for v := uint32(first.VShard); v <= uint32(last.VShard); v++ {
			scope, err := v2part.PartitionPrefixForGroup(d.Scope.Group, uint16(v), 0)
			if err != nil {
				return err
			}
			scope = append(scope, d.Scope.Part[:]...)
			if data.SeekGE(scope) && bytes.HasPrefix(data.Key(), scope) {
				added = append(added, string(scope))
			}
			if err := data.Error(); err != nil {
				return err
			}
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		if len(added) == 0 {
			return errors.New("sealed descriptor has no rollup scopes")
		}
		inventory[id] = added
		scopes = append(scopes, added...)
		return nil
	}
	if known != nil {
		// Iteration touches only in-memory identities for old parts. Descriptor
		// decoding and routed-scope seeks happen exclusively for new parts.
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
	return inventory, scopes, it.Error()
}
