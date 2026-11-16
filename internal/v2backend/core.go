package v2backend

import (
	"path/filepath"

	"github.com/cockroachdb/pebble"
	"github.com/go-graphite/go-carbon/internal/v2core"
)

func (s *Store) openCore() error {
	engine, err := v2core.Open(filepath.Join(s.opts.DataDir, "core"), v2core.Config{
		JournalIdentity: s.log.Identity(), Shards: s.opts.Shards, Producers: s.opts.Owners,
		Owners: s.opts.Shards, MaxRows: 2 << 20, PartitionSeconds: 86400, AppendParts: true, RawCodec: "run1",
	}, v2core.Hooks{PlanSeal: s.planCoreSeal, Publish: func(id int, snap *pebble.Snapshot, _ []v2core.Row) error {
		_ = snap.Close()
		if !s.coreViewsReady.Load() || s.coreViewsClosed.Load() {
			return nil
		}
		for _, o := range s.owners {
			if o.id%s.opts.Shards != id {
				continue
			}
			if base := o.readBase.Load(); base != nil && base.cursor == o.meta.Cursor {
				continue
			}
			if err := o.publish(); err != nil {
				return err
			}
		}
		return nil
	}})
	if err != nil {
		return err
	}
	s.core = engine
	s.dbs = engine.DBs() // Borrowed until Close; the engine owns their lifetime.
	return nil
}

// Already published suffixes need no barrier. Otherwise serialize a durable
// producer barrier with intake; the core reports any failed ingest through it.
// Publication skips other producers' frontier-only seals in openCore.
func (o *owner) awaitCorePublication(fs []frame) error {
	if len(fs) == 0 {
		return nil
	}
	if base := o.readBase.Load(); base != nil && base.cursor >= fs[len(fs)-1].cursor {
		return nil
	}
	durable := make(chan answer, 1)
	o.requests <- command{op: 's', answer: durable}
	return (<-durable).err
}
