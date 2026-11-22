package downsize

import (
	"bytes"
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"strings"
)

// Prefix pages store a name exactly once, preserving all 63 source bits. IDs
// are one-based, immutable sorted ordinals within unit.CatalogGeneration.
// Reading one name decodes at most 4096 entries, never the whole dictionary.
func encodeStateCatalog(names []syncName) ([]byte, error) {
	if len(names) == 0 || len(names) > statePageNames {
		return nil, errors.New("invalid catalog page length")
	}
	data := []byte{1}
	data = binary.AppendUvarint(data, uint64(len(names)))
	previous := ""
	for i, n := range names {
		if n.Name == "" || strings.ContainsAny(n.Name, "\x00\r\n\t") || i > 0 && n.Name <= previous || n.Sources>>63 != 0 || n.Short {
			return nil, errors.New("invalid catalog name, ordering, source mask or short-retention entry")
		}
		prefix := 0
		for prefix < len(previous) && prefix < len(n.Name) && previous[prefix] == n.Name[prefix] {
			prefix++
		}
		data = binary.AppendUvarint(data, uint64(prefix))
		data = binary.AppendUvarint(data, uint64(len(n.Name)-prefix))
		data = append(data, n.Name[prefix:]...)
		data = binary.AppendUvarint(data, n.Sources)
		flag := byte(0)
		if n.OnTarget {
			flag = 1
		}
		data = append(data, flag)
		previous = n.Name
	}
	return data, nil
}

func decodeStateCatalog(data []byte) ([]syncName, error) {
	if len(data) == 0 || data[0] != 1 {
		return nil, errors.New("unsupported catalog page version")
	}
	r := bytes.NewReader(data[1:])
	n, err := binary.ReadUvarint(r)
	if err != nil || n == 0 || n > statePageNames {
		return nil, errors.New("invalid catalog page count")
	}
	names := make([]syncName, 0, n)
	previous := ""
	for range n {
		prefix, err := binary.ReadUvarint(r)
		if err != nil || prefix > uint64(len(previous)) {
			return nil, errors.New("invalid catalog prefix")
		}
		suffix, err := binary.ReadUvarint(r)
		if err != nil || suffix > uint64(r.Len()) {
			return nil, errors.New("invalid catalog suffix")
		}
		b := make([]byte, suffix)
		if _, err = io.ReadFull(r, b); err != nil {
			return nil, err
		}
		name := previous[:prefix] + string(b)
		sources, err := binary.ReadUvarint(r)
		if err != nil || sources>>63 != 0 {
			return nil, errors.New("invalid catalog mask")
		}
		flags, err := r.ReadByte()
		if err != nil || flags > 1 || name == "" || len(names) > 0 && name <= previous || strings.ContainsAny(name, "\x00\r\n\t") {
			return nil, errors.New("invalid catalog flags or name")
		}
		names = append(names, syncName{Name: name, Sources: sources, OnTarget: flags == 1})
		previous = name
	}
	if r.Len() != 0 {
		return nil, errors.New("trailing catalog page bytes")
	}
	return names, nil
}

func (t *stateTx) putCatalog(first int64, names []syncName) error {
	if first < 1 || (first-1)%statePageNames != 0 || first > t.unit.CatalogNames || int64(len(names)) != min(int64(statePageNames), t.unit.CatalogNames-first+1) {
		return errors.New("catalog page is not an exact generation-bound range")
	}
	data, err := encodeStateCatalog(names)
	if err != nil {
		return err
	}
	digest := sha256.Sum256(data)
	var previous []byte
	err = t.tx.QueryRow("SELECT data FROM catalog_page WHERE first_id=?", first).Scan(&previous)
	if err == nil {
		if !bytes.Equal(previous, data) {
			return errors.New("immutable catalog page changed")
		}
		return nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	if first > 1 {
		var last string
		if err = t.tx.QueryRow("SELECT last_name FROM catalog_page WHERE first_id=?", first-statePageNames).Scan(&last); err != nil {
			return fmt.Errorf("catalog page gap: %w", err)
		}
		if names[0].Name <= last {
			return errors.New("catalog pages overlap")
		}
	}
	_, err = t.tx.Exec("INSERT INTO catalog_page VALUES(?,1,?,?,?,?,?,?)", first, t.unit.CatalogGeneration, len(names), names[0].Name, names[len(names)-1].Name, digest[:], data)
	return err
}

func (s *stateStore) catalogName(id int64) (syncName, error) {
	if id < 1 || id > s.currentUnit().CatalogNames {
		return syncName{}, errors.New("name ID outside catalog")
	}
	first := (id-1)/statePageNames*statePageNames + 1
	names, err := s.catalogPage("first_id=?", first)
	if err != nil {
		return syncName{}, err
	}
	if id-first >= int64(len(names)) {
		return syncName{}, errors.New("catalog page lacks expected ID")
	}
	return names[id-first], nil
}

func (s *stateStore) catalogPage(where string, arg any) ([]syncName, error) {
	var data, digest []byte
	var generation, firstName, lastName string
	var count int
	err := s.db.QueryRow("SELECT generation,names,first_name,last_name,sha256,data FROM catalog_page WHERE "+where, arg).Scan(&generation, &count, &firstName, &lastName, &digest, &data)
	if err != nil {
		return nil, err
	}
	sum := sha256.Sum256(data)
	if generation != s.currentUnit().CatalogGeneration || !bytes.Equal(sum[:], digest) {
		return nil, errors.New("catalog generation or page checksum mismatch")
	}
	names, err := decodeStateCatalog(data)
	if err == nil && (len(names) != count || names[0].Name != firstName || names[len(names)-1].Name != lastName) {
		err = errors.New("catalog index differs from encoded page")
	}
	return names, err
}
