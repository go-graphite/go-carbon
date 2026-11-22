package downsize

// The discovery controller owns scheduling; this boundary owns only its
// consumed catalog prefix. Production constructs the legacy implementation.
// SQLite is exercised by controller differential tests until ALL persistence
// adapters can be selected together; it has no independent CLI/config selector.

import (
	"database/sql"
	"errors"
	"fmt"
)

type syncDiscoveryPersistence interface {
	load() (syncDiscoveryCheckpoint, bool, error)
	open(syncDiscoveryCheckpoint) error
	append(syncName) error
	commit(syncDiscoveryCheckpoint, bool) (syncDiscoveryCheckpoint, error)
	close() error
}

type legacySyncDiscovery struct {
	s              *syncer
	union, missing *syncTSV
}

func (p *legacySyncDiscovery) load() (cp syncDiscoveryCheckpoint, complete bool, err error) {
	cp.Cursors = map[string]string{}
	if rows := p.s.j.find(func(r syncRow) bool { return r.Kind == "discovered" }); len(rows) > 0 {
		r := rows[0]
		cp.Names, cp.Missing, cp.NotOwned, cp.OnTarget, cp.Both = r.Names, r.Missing, r.NotOwned, r.OnTarget, r.Both
		return cp, true, nil
	}
	if rows := p.s.j.find(func(r syncRow) bool { return r.Kind == "discover_checkpoint" }); len(rows) > 0 {
		cp = *rows[len(rows)-1].Discovery
		cp.Cursors = cloneCursors(cp.Cursors)
	}
	return cp, false, nil
}

func (p *legacySyncDiscovery) open(cp syncDiscoveryCheckpoint) error {
	var err error
	if p.union, err = openDurableTSV(p.s.unionPath(), cp.UnionBytes); err != nil {
		return err
	}
	if p.missing, err = openDurableTSV(p.s.missingPath(), cp.MissingBytes); err != nil {
		return err
	}
	return syncDirectory(p.s.o.state)
}

func (p *legacySyncDiscovery) append(n syncName) error {
	if !n.OnTarget {
		if _, err := fmt.Fprintf(p.missing.w, "%s\t%x\n", n.Name, n.Sources); err != nil {
			return err
		}
	}
	_, err := fmt.Fprintf(p.union.w, "%s\t%x\t%d\n", n.Name, n.Sources, stateBool(n.OnTarget))
	return err
}

func (p *legacySyncDiscovery) commit(cp syncDiscoveryCheckpoint, complete bool) (syncDiscoveryCheckpoint, error) {
	var err error
	if cp.UnionBytes, err = p.union.durableOffset(); err != nil {
		return cp, err
	}
	if cp.MissingBytes, err = p.missing.durableOffset(); err != nil {
		return cp, err
	}
	// The controller advances its cursor map after this call. Freeze the map
	// retained by the legacy journal, just as the original writer did.
	saved := cp
	saved.Cursors = cloneCursors(cp.Cursors)
	if err := p.s.j.append(syncRow{Kind: "discover_checkpoint", Discovery: &saved}); err != nil {
		return cp, err
	}
	if complete {
		err = p.s.j.append(syncRow{Kind: "discovered", Names: cp.Names, Missing: cp.Missing, NotOwned: cp.NotOwned, OnTarget: cp.OnTarget, Both: cp.Both})
	}
	return cp, err
}

func (p *legacySyncDiscovery) close() error {
	var err error
	if p.union != nil {
		err = p.union.file.Close()
	}
	if p.missing != nil {
		err = errors.Join(err, p.missing.file.Close())
	}
	return err
}

type sqliteSyncDiscovery struct {
	store   *stateStore
	scope   string
	saved   stateDiscovery
	pending []syncName
}

func (p *sqliteSyncDiscovery) load() (cp syncDiscoveryCheckpoint, complete bool, err error) {
	err = p.store.update(func(tx *stateTx) error {
		p.saved, err = readStateDiscovery(tx.tx, p.scope)
		if errors.Is(err, sql.ErrNoRows) {
			if err := tx.beginDiscovery(p.scope); err != nil {
				return err
			}
			p.saved, err = readStateDiscovery(tx.tx, p.scope)
		}
		if err == nil && (p.saved.Generation != tx.unit.CatalogGeneration || p.saved.Complete && int64(p.saved.Checkpoint.Names) != tx.unit.CatalogNames || !p.saved.Complete && tx.unit.CatalogNames != 0) {
			return errors.New("discovery generation or sealed unit count changed")
		}
		return err
	})
	cp = p.saved.Checkpoint
	// Never share the transaction's authoritative checkpoint map with the
	// controller while the next prefix is being assembled.
	cp.Cursors = cloneCursors(cp.Cursors)
	return cp, p.saved.Complete, err
}

func (p *sqliteSyncDiscovery) append(n syncName) error {
	if len(p.pending) >= syncListPage {
		return errors.New("discovery batch exceeds its checkpoint bound")
	}
	p.pending = append(p.pending, n)
	return nil
}

func (p *sqliteSyncDiscovery) open(syncDiscoveryCheckpoint) error { return nil }

func (p *sqliteSyncDiscovery) commit(cp syncDiscoveryCheckpoint, complete bool) (syncDiscoveryCheckpoint, error) {
	cp.UnionBytes, cp.MissingBytes = p.saved.Checkpoint.UnionBytes, p.saved.Checkpoint.MissingBytes
	for _, n := range p.pending {
		cp.UnionBytes += int64(len(fmt.Sprintf("%s\t%x\t%d\n", n.Name, n.Sources, stateBool(n.OnTarget))))
		if !n.OnTarget {
			cp.MissingBytes += int64(len(fmt.Sprintf("%s\t%x\n", n.Name, n.Sources)))
		}
	}
	var next stateDiscovery
	err := p.store.update(func(tx *stateTx) error {
		if err := tx.advanceDiscovery(p.saved, cp, p.pending); err != nil {
			return err
		}
		if err := tx.reduceProgress(p.scope, syncRow{Kind: "discover_checkpoint", Discovery: &cp}); err != nil {
			return err
		}
		var err error
		next, err = readStateDiscovery(tx.tx, p.scope)
		if err == nil && complete {
			err = tx.sealDiscovery(next)
			if err == nil {
				next, err = readStateDiscovery(tx.tx, p.scope)
			}
		}
		return err
	})
	if err == nil {
		p.saved = next
		p.pending = p.pending[:0]
	}
	return cp, err
}

func (p *sqliteSyncDiscovery) close() error { return nil }
