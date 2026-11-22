package downsize

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
)

// Catalog readers share the discovery owner's dictionary. In particular, the
// SQLite controller must never reconstruct union.tsv as a second live catalog.
type syncCatalogPersistence interface {
	source(missing bool) syncNameSource
	selectNames([]string) (syncNameSource, error)
}

type legacySyncCatalog struct{ s *syncer }

func (p legacySyncCatalog) source(missing bool) syncNameSource {
	path := p.s.unionPath()
	if missing {
		path = p.s.missingPath()
	}
	var f *os.File
	var scanner *bufio.Scanner
	ordinal := int64(-1)
	onTarget := false
	return syncNameSource{onTarget: func() bool { return onTarget }, id: func() int64 { return ordinal }, next: func() (string, uint64, error) {
		if f == nil {
			var err error
			f, scanner, err = openLines(path)
			if err != nil {
				return "", 0, err
			}
		}
		if scanner.Scan() {
			ordinal++
			name, mask, target, err := parseUnionLine(scanner.Text())
			onTarget = target
			return name, mask, err
		}
		if err := scanner.Err(); err != nil {
			return "", 0, err
		}
		return "", 0, io.EOF
	}, close: func() {
		if f != nil {
			_ = f.Close()
		}
	}}
}

func (p legacySyncCatalog) selectNames(names []string) (syncNameSource, error) {
	return p.s.legacySetSource(names)
}

func (s *syncer) catalogPersistence() syncCatalogPersistence {
	if s.catalogState != nil {
		return s.catalogState
	}
	return legacySyncCatalog{s}
}

type sqliteSyncCatalog struct{ store *stateStore }

func (p sqliteSyncCatalog) source(missing bool) syncNameSource {
	unit := p.store.currentUnit()
	id := int64(0)
	onTarget := false
	var page []syncName
	return syncNameSource{onTarget: func() bool { return onTarget }, id: func() int64 { return id - 1 }, total: int(unit.CatalogNames), next: func() (string, uint64, error) {
		for id < unit.CatalogNames {
			if id%statePageNames == 0 {
				var err error
				page, err = p.store.catalogPage("first_id=?", id+1)
				if err != nil {
					return "", 0, err
				}
				if int64(len(page)) != min(int64(statePageNames), unit.CatalogNames-id) {
					return "", 0, errors.New("catalog page lacks its sealed extent")
				}
			}
			n := page[id%statePageNames]
			id++
			onTarget = n.OnTarget
			if missing && n.OnTarget {
				continue
			}
			return n.Name, n.Sources, nil
		}
		return "", 0, io.EOF
	}, close: func() { page = nil }}
}

func (p sqliteSyncCatalog) selectNames(set []string) (syncNameSource, error) {
	names := append([]string(nil), set...)
	sort.Strings(names)
	type entry struct {
		id   int64
		name string
		mask uint64
	}
	var picked []entry
	var page []syncName
	var first int64
	for i, name := range names {
		if i > 0 && name == names[i-1] {
			continue
		}
		if len(page) == 0 || name < page[0].Name || name > page[len(page)-1].Name {
			if err := p.store.db.QueryRow("SELECT first_id FROM catalog_page WHERE first_name<=? ORDER BY first_name DESC LIMIT 1", name).Scan(&first); err != nil {
				return syncNameSource{}, fmt.Errorf("verification frontier name %q absent from catalog: %w", name, err)
			}
			var err error
			page, err = p.store.catalogPage("first_id=?", first)
			if err != nil {
				return syncNameSource{}, err
			}
		}
		at := sort.Search(len(page), func(i int) bool { return page[i].Name >= name })
		if at == len(page) || page[at].Name != name {
			return syncNameSource{}, fmt.Errorf("verification frontier name %q absent from catalog", name)
		}
		picked = append(picked, entry{first + int64(at) - 1, name, page[at].Sources})
	}
	i := 0
	return syncNameSource{id: func() int64 { return picked[i-1].id }, total: len(picked), next: func() (string, uint64, error) {
		if i == len(picked) {
			return "", 0, io.EOF
		}
		n := picked[i]
		i++
		return n.name, n.mask, nil
	}, close: func() {}}, nil
}
