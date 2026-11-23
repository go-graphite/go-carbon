package v2core

import (
	"bytes"
	"cmp"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"

	"github.com/cockroachdb/pebble"
	"github.com/go-graphite/go-carbon/internal/v2part"
)

// BootstrapRollups builds an unpublished, new core using the ordinary immutable
// seal writer and atomic part/frontier ingest. The caller owns an exclusive
// staging directory and must publish the entire group only after this returns.
// Numeric IDs must already match the durable producer catalogs.
func BootstrapRollups(root string, cfg Config, rows []KV, source v2part.Provenance, stage func(string)) error {
	if !cfg.Rollups || cfg.Shards < 1 || cfg.Producers < 1 || cfg.MaxRows < 1 || cfg.PartitionSeconds < 1 || len(rows) == 0 {
		return errors.New("invalid baseline geometry or empty records")
	}
	if cfg.RollupRoutingVersion == 0 {
		cfg.RollupRoutingVersion = 1
	}
	if cfg.RollupRoutingVersion != 1 {
		return errors.New("unsupported baseline rollup routing version")
	}
	if _, err := os.Stat(root); !errors.Is(err, os.ErrNotExist) {
		return errors.New("baseline core already exists")
	}
	if err := os.MkdirAll(root, 0750); err != nil {
		return err
	}
	if _, err := openPartSource(root, cfg.JournalIdentity, source); err != nil {
		return err
	}
	// Resolve and validate all metadata/slots through the same reader used by
	// recovery, after each owner has been written. No producer can see this root.
	ids := map[v2part.ID]uint64{}
	for _, kv := range rows {
		k, err := v2part.Decode(kv.Key)
		if err != nil {
			return err
		}
		if k.Archive == v2part.MetadataArchive {
			m, err := DecodeRollupMetadata(k.Metric, k.Group, kv.Value)
			if err != nil {
				return err
			}
			// The envelope repeats once per month partition; copies must agree.
			if m.ID == 0 || (ids[k.Metric] != 0 && ids[k.Metric] != m.ID) {
				return errors.New("duplicate baseline metadata")
			}
			ids[k.Metric] = m.ID
		}
	}
	// One part per owner and partition scope, as a live seal writes them.
	type scope struct {
		partition int64
		part      v2part.PartID
	}
	files := make([]map[scope][]KV, cfg.Shards)
	for _, kv := range rows {
		k, _ := v2part.Decode(kv.Key)
		id := ids[k.Metric]
		if id == 0 {
			return errors.New("baseline slots lack metadata")
		}
		owner := int(v2part.Route(k.Metric)) % cfg.Shards
		if files[owner] == nil {
			files[owner] = map[scope][]KV{}
		}
		s := scope{k.Partition, k.Part}
		files[owner][s] = append(files[owner][s], kv)
	}
	cfg.Owners = 0
	for owner, scoped := range files {
		dir := filepath.Join(root, fmt.Sprintf("owner-%03d", owner))
		db, err := pebble.Open(dir, &pebble.Options{MemTableSize: 1 << 20})
		if err != nil {
			return err
		}
		next := coreFrontier{Version: 1, Config: cfg, Cursors: make([]uint64, cfg.Producers), NextPart: 1, ExpiredBefore: -1 << 63}
		o := &coreOwner{db: db, dir: dir, cfg: cfg, partSource: source, stage: stage}
		scopes := make([]scope, 0, len(scoped))
		for s := range scoped {
			scopes = append(scopes, s)
		}
		slices.SortFunc(scopes, func(a, b scope) int {
			if a.partition != b.partition {
				return cmp.Compare(a.partition, b.partition)
			}
			return bytes.Compare(a.part[:], b.part[:])
		})
		var plan [][]KV
		for _, s := range scopes {
			file := scoped[s]
			slices.SortFunc(file, func(a, b KV) int { return bytes.Compare(a.Key, b.Key) })
			plan = append(plan, file)
		}
		o.plan = func(coreSealInput) (coreSealPlan, error) {
			return coreSealPlan{Files: plan}, nil
		}
		_, err = o.sealBatches(nil, 0, 0, PendingView{}, next)
		if err == nil {
			_, _, err = recoverCoreRollups(db)
		}
		err = errors.Join(err, db.Close())
		if err != nil {
			return err
		}
	}
	return SyncPartDirectory(root)
}
