package downsize

// A growing discovery catalog has no final name count yet. Its durable prefix,
// source cursors and legacy extents form one transaction; its last (partial)
// page may grow until discovery seals it. Sealing publishes the final unit
// size without copying the catalog into another relation.
// This is a candidate adapter, not a partial runtime backend switch.

import (
	"bytes"
	"crypto/sha256"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
)

const stateDiscoveryDDL = `
CREATE TABLE discovery(scope TEXT PRIMARY KEY REFERENCES scope(id), version INTEGER NOT NULL CHECK(version=1), generation TEXT NOT NULL UNIQUE, revision INTEGER NOT NULL CHECK(revision>=0), complete INTEGER NOT NULL CHECK(complete IN (0,1)), cursors_present INTEGER NOT NULL CHECK(cursors_present IN (0,1)), union_bytes INTEGER NOT NULL CHECK(union_bytes>=0), missing_bytes INTEGER NOT NULL CHECK(missing_bytes>=0), names INTEGER NOT NULL CHECK(names>=0), missing INTEGER NOT NULL CHECK(missing>=0), not_owned INTEGER NOT NULL CHECK(not_owned>=0), on_target INTEGER NOT NULL CHECK(on_target>=0), both INTEGER NOT NULL CHECK(both>=0)) STRICT;
CREATE TABLE discovery_cursor(scope TEXT NOT NULL REFERENCES discovery(scope), holder TEXT NOT NULL, version INTEGER NOT NULL CHECK(version=1), cursor TEXT NOT NULL, PRIMARY KEY(scope,holder)) STRICT, WITHOUT ROWID;
`

var stateDiscoveryDigestQueries = map[string]string{
	"discovery":        "SELECT * FROM discovery ORDER BY scope",
	"discovery_cursor": "SELECT * FROM discovery_cursor ORDER BY scope,holder",
}

type stateDiscovery struct {
	Scope, Generation string
	Revision          int64
	Complete          bool
	Checkpoint        syncDiscoveryCheckpoint
}

func (t *stateTx) beginDiscovery(scope string) error {
	if t.unit.CatalogNames != 0 {
		return errors.New("discovery requires an unknown final catalog size")
	}
	var n int
	if err := t.tx.QueryRow("SELECT (SELECT count(*) FROM discovery)+(SELECT count(*) FROM catalog_page)").Scan(&n); err != nil {
		return err
	}
	if n != 0 {
		return errors.New("catalog discovery already exists")
	}
	_, err := t.tx.Exec("INSERT INTO discovery VALUES(?,1,?,0,0,0,0,0,0,0,0,0,0)", scope, t.unit.CatalogGeneration)
	return err
}

func readStateDiscovery(tx *sql.Tx, scope string) (d stateDiscovery, err error) {
	d.Scope = scope
	c := &d.Checkpoint
	var present bool
	err = tx.QueryRow("SELECT generation,revision,complete,cursors_present,union_bytes,missing_bytes,names,missing,not_owned,on_target,both FROM discovery WHERE scope=?", scope).Scan(&d.Generation, &d.Revision, &d.Complete, &present, &c.UnionBytes, &c.MissingBytes, &c.Names, &c.Missing, &c.NotOwned, &c.OnTarget, &c.Both)
	if err != nil {
		return d, err
	}
	if present {
		c.Cursors = map[string]string{}
	}
	err = stateTaskRows(tx, "SELECT holder,cursor FROM discovery_cursor WHERE scope=? ORDER BY holder", []any{scope}, func(rows *sql.Rows) error {
		var holder, cursor string
		if err := rows.Scan(&holder, &cursor); err != nil {
			return err
		}
		if !present {
			return errors.New("discovery cursor contradicts absent cursor map")
		}
		c.Cursors[holder] = cursor
		return nil
	})
	return d, err
}

func readStateDiscoveryPage(tx *sql.Tx, d stateDiscovery, first int64) ([]syncName, error) {
	if first < 1 || (first-1)%statePageNames != 0 || first > int64(d.Checkpoint.Names) {
		return nil, errors.New("discovery page outside committed prefix")
	}
	var data, digest []byte
	var count int
	var firstName, lastName string
	query := "SELECT names,first_name,last_name,sha256,data FROM catalog_page WHERE generation=? AND first_id=?"
	args := []any{d.Generation, first}
	if err := tx.QueryRow(query, args...).Scan(&count, &firstName, &lastName, &digest, &data); err != nil {
		return nil, err
	}
	sum := sha256.Sum256(data)
	if !bytes.Equal(sum[:], digest) {
		return nil, errors.New("discovery page checksum mismatch")
	}
	names, err := decodeStateCatalog(data)
	if err == nil && (len(names) != count || count != min(statePageNames, d.Checkpoint.Names-int(first)+1) || names[0].Name != firstName || names[count-1].Name != lastName) {
		err = errors.New("discovery page index or committed extent changed")
	}
	return names, err
}

// Compare-and-swap the whole checkpoint, so an old batch cannot overwrite the
// next consumed prefix. The caller supplies only newly admitted names, bounded
// by one source list page. Unowned names advance cursors and not_owned without entering the
// catalog. All extents are the exact bytes the legacy TSV writer would emit.
func (t *stateTx) advanceDiscovery(expected stateDiscovery, next syncDiscoveryCheckpoint, admitted []syncName) error {
	actual, err := readStateDiscovery(t.tx, expected.Scope)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(actual, expected) || actual.Generation != t.unit.CatalogGeneration {
		return errors.New("discovery checkpoint changed")
	}
	if actual.Complete || len(admitted) > syncListPage {
		return errors.New("discovery is sealed or batch exceeds one list page")
	}
	want := actual.Checkpoint
	want.Names += len(admitted)
	for _, n := range admitted {
		want.UnionBytes += int64(len(fmt.Sprintf("%s\t%x\t%d\n", n.Name, n.Sources, stateBool(n.OnTarget))))
		if n.OnTarget {
			want.OnTarget++
			if n.Sources != 0 {
				want.Both++
			}
		} else {
			want.Missing++
			want.MissingBytes += int64(len(fmt.Sprintf("%s\t%x\n", n.Name, n.Sources)))
		}
	}
	if next.NotOwned < want.NotOwned {
		return errors.New("discovery unowned counter regressed")
	}
	for holder, cursor := range want.Cursors {
		if newer, ok := next.Cursors[holder]; !ok || newer < cursor {
			return errors.New("discovery source cursor regressed")
		}
	}
	want.Cursors, want.NotOwned = next.Cursors, next.NotOwned
	if !reflect.DeepEqual(want, next) {
		return errors.New("discovery counters or file extents differ from admitted prefix")
	}
	if len(admitted) != 0 {
		first := int64(actual.Checkpoint.Names/statePageNames*statePageNames + 1)
		var page []syncName
		if actual.Checkpoint.Names%statePageNames != 0 {
			page, err = readStateDiscoveryPage(t.tx, actual, first)
			if err != nil {
				return err
			}
		} else if actual.Checkpoint.Names != 0 {
			previous, err := readStateDiscoveryPage(t.tx, actual, first-statePageNames)
			if err != nil {
				return err
			}
			if admitted[0].Name <= previous[len(previous)-1].Name {
				return errors.New("discovery catalog prefix is not byte-sorted")
			}
		}
		pending := append(page, admitted...)
		for len(pending) > 0 {
			count := min(statePageNames, len(pending))
			page := pending[:count]
			data, err := encodeStateCatalog(page)
			if err != nil {
				return err
			}
			sum := sha256.Sum256(data)
			if first <= int64(actual.Checkpoint.Names) {
				// Only the formerly partial tail can change. A pre-existing row
				// beyond the committed prefix is a contradiction, not an upsert.
				_, err = t.tx.Exec("UPDATE catalog_page SET names=?,first_name=?,last_name=?,sha256=?,data=? WHERE first_id=? AND generation=?", count, page[0].Name, page[count-1].Name, sum[:], data, first, actual.Generation)
			} else {
				_, err = t.tx.Exec("INSERT INTO catalog_page VALUES(?,1,?,?,?,?,?,?)", first, actual.Generation, count, page[0].Name, page[count-1].Name, sum[:], data)
			}
			if err != nil {
				return err
			}
			pending = pending[count:]
			if len(pending) > 0 && pending[0].Name <= page[count-1].Name {
				return errors.New("discovery catalog pages overlap")
			}
			first += statePageNames
		}
	}
	if _, err = t.tx.Exec("UPDATE discovery SET revision=revision+1,cursors_present=?,union_bytes=?,missing_bytes=?,names=?,missing=?,not_owned=?,on_target=?,both=? WHERE scope=?", next.Cursors != nil, next.UnionBytes, next.MissingBytes, next.Names, next.Missing, next.NotOwned, next.OnTarget, next.Both, expected.Scope); err != nil {
		return err
	}
	if _, err = t.tx.Exec("DELETE FROM discovery_cursor WHERE scope=?", expected.Scope); err != nil {
		return err
	}
	for holder, cursor := range next.Cursors {
		if holder == "" {
			return errors.New("discovery source requires an identity")
		}
		if _, err = t.tx.Exec("INSERT INTO discovery_cursor VALUES(?,?,1,?)", expected.Scope, holder, cursor); err != nil {
			return err
		}
	}
	return nil
}

func stateBool(v bool) int {
	if v {
		return 1
	}
	return 0
}

// The final page count becomes immutable only after all holders are exhausted.
// No phase/receipt may refer to these IDs before this transaction commits.
func (t *stateTx) sealDiscovery(expected stateDiscovery) error {
	actual, err := readStateDiscovery(t.tx, expected.Scope)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(actual, expected) || actual.Generation != t.unit.CatalogGeneration {
		return errors.New("discovery checkpoint changed before seal")
	}
	previous := ""
	for first := int64(1); first <= int64(actual.Checkpoint.Names); first += statePageNames {
		page, err := readStateDiscoveryPage(t.tx, actual, first)
		if err != nil {
			return err
		}
		if page[0].Name <= previous {
			return errors.New("discovery catalog pages overlap at seal")
		}
		previous = page[len(page)-1].Name
	}
	var pages, names int64
	if err = t.tx.QueryRow("SELECT count(*),coalesce(sum(names),0) FROM catalog_page WHERE generation=?", actual.Generation).Scan(&pages, &names); err != nil {
		return err
	}
	if names != int64(actual.Checkpoint.Names) || pages != (names+statePageNames-1)/statePageNames {
		return errors.New("discovery catalog has an unexpected page or extent")
	}
	if actual.Complete {
		if t.unit.CatalogNames != names {
			return errors.New("sealed discovery differs from final unit size")
		}
		return nil
	}
	if _, err = t.tx.Exec("UPDATE unit SET catalog_names=? WHERE singleton=1", actual.Checkpoint.Names); err != nil {
		return err
	}
	if _, err = t.tx.Exec("UPDATE discovery SET complete=1,revision=revision+1 WHERE scope=?", actual.Scope); err == nil {
		t.unit.CatalogNames = int64(actual.Checkpoint.Names)
	}
	return err
}
