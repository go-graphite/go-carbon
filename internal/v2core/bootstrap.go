package v2core

import (
	"bytes"
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
			m, err := DecodeRollupMetadata(kv.Value)
			if err != nil {
				return err
			}
			if ids[k.Metric] != 0 || m.ID == 0 {
				return errors.New("duplicate baseline metadata")
			}
			ids[k.Metric] = m.ID
		}
	}
	files := make([][]KV, cfg.Shards)
	for _, kv := range rows {
		k, _ := v2part.Decode(kv.Key)
		id := ids[k.Metric]
		if id == 0 {
			return errors.New("baseline slots lack metadata")
		}
		owner := int(v2part.Route(k.Metric)) % cfg.Shards
		files[owner] = append(files[owner], kv)
	}
	cfg.Owners = 0
	for owner, file := range files {
		dir := filepath.Join(root, fmt.Sprintf("owner-%03d", owner))
		db, err := pebble.Open(dir, &pebble.Options{MemTableSize: 1 << 20})
		if err != nil {
			return err
		}
		next := coreFrontier{Version: 1, Config: cfg, Cursors: make([]uint64, cfg.Producers), NextPart: 1, ExpiredBefore: -1 << 63}
		o := &coreOwner{db: db, dir: dir, cfg: cfg, partSource: source, stage: stage}
		slices.SortFunc(file, func(a, b KV) int { return bytes.Compare(a.Key, b.Key) })
		o.plan = func(coreSealInput) (coreSealPlan, error) {
			p := coreSealPlan{}
			if len(file) > 0 {
				p.Files = [][]KV{file}
			}
			return p, nil
		}
		_, err = o.sealBatches(nil, nil, next)
		if err == nil {
			_, err = recoverCoreRollups(db)
		}
		err = errors.Join(err, db.Close())
		if err != nil {
			return err
		}
	}
	return SyncPartDirectory(root)
}
