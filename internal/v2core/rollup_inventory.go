package v2core

import (
	"bytes"
	"context"
	"errors"

	"github.com/cockroachdb/pebble"
	"github.com/go-graphite/go-carbon/internal/v2part"
)

// The core's atomic descriptor bindings are the source of truth for its sealed
// part inventory. No live filesystem enumeration or previous snapshot is held.
// Bare GCP3 readers retain the scan fallback in NewRollupSnapshot.
func rollupInventory(ctx context.Context, snapshot *pebble.Snapshot, prior *RollupSnapshot) (map[string][]string, []string, error) {
	it, err := snapshot.NewIter(&pebble.IterOptions{LowerBound: sealedPartPrefix, UpperBound: v2part.PrefixEnd(sealedPartPrefix)})
	if err != nil {
		return nil, nil, err
	}
	defer it.Close()
	var inventory map[string][]string
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
	return inventory, scopes, it.Error()
}

func rollupScopeCount(inventory map[string][]string) int {
	n := 0
	for _, scopes := range inventory {
		n += len(scopes)
	}
	return n
}
