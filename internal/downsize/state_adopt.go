package downsize

// Adoption primitives are intentionally not an activation path. In particular,
// importing the catalog cannot authorize any phase, native transfer or READY
// work. The remaining legacy adapters and independent reducer oracle must be
// complete before sync may call a whole-unit adopter or publish config v2.

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"syscall"
	"time"
)

type stateAdoptSource struct {
	Path           string
	Size, Modified int64
	SHA256         [32]byte
	Device, Inode  string // portable across signed/unsigned stat fields
}

type stateAdoptPosition struct {
	Source          stateAdoptSource
	Offset, Records int64
	PrefixSHA256    [32]byte
	Complete        bool
}

type stateAdoptFile struct {
	store  *stateStore
	file   *os.File
	cursor stateAdoptPosition
	prefix hash.Hash
}

// Open only regular non-symlink files beneath the unit directory. Both the
// namespace and opened descriptor are checked before and after authentication.
// The existing sync.jsonl lock remains held for this object's entire lifetime.
func (s *stateStore) adoptSource(relative string) (_ *stateAdoptFile, err error) {
	if s.lock == nil || s.closed {
		return nil, errors.New("legacy adoption requires the exclusive unit lock")
	}
	if filepath.IsAbs(relative) || relative == "." || filepath.Clean(relative) != relative || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
		return nil, errors.New("invalid legacy source path")
	}
	root := filepath.Dir(s.path)
	path := root
	for _, part := range strings.Split(relative, string(filepath.Separator)) {
		path = filepath.Join(path, part)
		st, err := os.Lstat(path)
		if err != nil {
			return nil, err
		}
		if st.Mode()&os.ModeSymlink != 0 {
			return nil, errors.New("legacy source symlinks are not authenticated")
		}
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			err = errors.Join(err, f.Close())
		}
	}()
	before, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if !before.Mode().IsRegular() {
		return nil, errors.New("legacy source is not a regular file")
	}
	h := sha256.New()
	n, err := io.Copy(h, f)
	if err != nil {
		return nil, err
	}
	after, err := f.Stat()
	if err != nil {
		return nil, err
	}
	named, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if !os.SameFile(before, after) || !os.SameFile(after, named) || n != before.Size() || before.Size() != after.Size() || before.ModTime() != after.ModTime() || named.Mode()&os.ModeSymlink != 0 {
		return nil, errors.New("legacy source changed during authentication")
	}
	stat, ok := after.Sys().(*syscall.Stat_t)
	if !ok {
		return nil, errors.New("legacy source has no stable file identity")
	}
	source := stateAdoptSource{Path: relative, Size: n, Modified: after.ModTime().UnixNano(), Device: fmt.Sprint(stat.Dev), Inode: fmt.Sprint(stat.Ino)}
	copy(source.SHA256[:], h.Sum(nil))
	cursor, err := readStateAdoptPosition(s.db, source)
	if err != nil {
		return nil, err
	}
	if _, err = f.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}
	prefix := sha256.New()
	if _, err = io.CopyN(prefix, f, cursor.Offset); err != nil {
		return nil, err
	}
	if !bytes.Equal(prefix.Sum(nil), cursor.PrefixSHA256[:]) {
		return nil, errors.New("legacy import cursor does not authenticate its consumed prefix")
	}
	return &stateAdoptFile{store: s, file: f, cursor: cursor, prefix: prefix}, nil
}

func readStateAdoptPosition(q interface{ QueryRow(string, ...any) *sql.Row }, source stateAdoptSource) (stateAdoptPosition, error) {
	p := stateAdoptPosition{Source: source, PrefixSHA256: sha256.Sum256(nil)}
	var actual stateAdoptSource
	var digest, prefix []byte
	actual.Path = source.Path
	err := q.QueryRow("SELECT size,sha256,device,inode,modified,offset,records,prefix_sha256,complete FROM adoption WHERE path=?", source.Path).Scan(&actual.Size, &digest, &actual.Device, &actual.Inode, &actual.Modified, &p.Offset, &p.Records, &prefix, &p.Complete)
	if errors.Is(err, sql.ErrNoRows) {
		return p, nil
	}
	if err != nil {
		return p, err
	}
	copy(actual.SHA256[:], digest)
	if len(digest) != 32 || len(prefix) != 32 || actual != source || p.Offset < 0 || p.Offset > source.Size || p.Records < 0 || p.Complete && p.Offset != source.Size {
		return p, errors.New("legacy import source or committed cursor changed")
	}
	copy(p.PrefixSHA256[:], prefix)
	return p, nil
}

// The source's committed boundary is a compare-and-swap in the same transaction
// as typed relation changes. A restarted reader uses this boundary exclusively.
func (f *stateAdoptFile) commit(next stateAdoptPosition, apply func(*stateTx) error) error {
	previous := f.cursor
	if next.Source != previous.Source || previous.Complete || next.Offset < previous.Offset || next.Offset > next.Source.Size || next.Records < previous.Records || next.Complete && next.Offset != next.Source.Size || next.Offset == previous.Offset && !next.Complete {
		return errors.New("invalid legacy import boundary advance")
	}
	err := f.store.update(func(tx *stateTx) error {
		actual, err := readStateAdoptPosition(tx.tx, previous.Source)
		if err != nil {
			return err
		}
		if actual != previous {
			return errors.New("legacy import cursor changed; reopen at committed boundary")
		}
		if err = apply(tx); err != nil {
			return err
		}
		source := next.Source
		_, err = tx.tx.Exec("INSERT INTO adoption VALUES(?,1,?,?,?,?,?,?,?,?,?) ON CONFLICT(path) DO UPDATE SET offset=excluded.offset,records=excluded.records,prefix_sha256=excluded.prefix_sha256,complete=excluded.complete", source.Path, source.Size, source.SHA256[:], source.Device, source.Inode, source.Modified, next.Offset, next.Records, next.PrefixSHA256[:], next.Complete)
		return err
	})
	if err == nil {
		f.cursor = next
	}
	return err
}

type stateAdoptStats struct {
	AdoptionSeconds float64 `json:"adoption_seconds"`
	AdoptedNames    int64   `json:"adopted_names"`
	ResumedNames    int64   `json:"resumed_names"`
}

// Stream the real union.tsv format directly into immutable ID pages. There is
// no whole-catalog map, normalization spool, or in-memory JSON dictionary.
// Partial discovery must first select the legacy reducer's committed TSV prefix;
// this method accepts only a complete catalog of unit.CatalogNames names.
func (s *stateStore) adoptCatalog(relative string) (stats stateAdoptStats, err error) {
	started := time.Now()
	defer func() {
		stats.AdoptionSeconds = time.Since(started).Seconds()
		fields := map[string]any{"source": relative, "adoption_seconds": stats.AdoptionSeconds, "adopted_names": stats.AdoptedNames, "resumed_names": stats.ResumedNames}
		if err != nil {
			fields["level"], fields["error"] = "ERROR", err.Error()
			event("sync_state_adoption_failed", fields)
		} else {
			event("sync_state_catalog_adopted", fields)
		}
	}()
	f, err := s.adoptSource(relative)
	if err != nil {
		return stats, err
	}
	defer func() { err = errors.Join(err, f.file.Close()) }()
	stats.ResumedNames = f.cursor.Records
	if f.cursor.Complete {
		return stats, nil
	}
	if f.cursor.Records%statePageNames != 0 {
		return stats, errors.New("catalog cursor is not a committed page boundary")
	}
	event("sync_state_adoption_started", map[string]any{"source": relative, "resumed_names": stats.ResumedNames, "offset": f.cursor.Offset})
	reader := bufio.NewReaderSize(f.file, 1<<20)
	for {
		next := f.cursor
		page := make([]syncName, 0, statePageNames)
		pageBytes := 0
		for len(page) < statePageNames && next.Offset < next.Source.Size {
			line, readErr := reader.ReadSlice('\n')
			if readErr != nil && !errors.Is(readErr, io.EOF) {
				return stats, fmt.Errorf("catalog line at %d: %w", next.Offset, readErr)
			}
			if len(line) == 0 {
				return stats, io.ErrUnexpectedEOF
			}
			pageBytes += len(line)
			if pageBytes > 32<<20 {
				return stats, errors.New("legacy catalog page exceeds 32 MiB import budget")
			}
			text := strings.TrimSuffix(strings.TrimSuffix(string(line), "\n"), "\r")
			name, mask, target, err := parseUnionLine(text)
			if err != nil {
				return stats, err
			}
			page = append(page, syncName{Name: name, Sources: mask, OnTarget: target})
			f.prefix.Write(line)
			next.Offset += int64(len(line))
			next.Records++
		}
		next.Complete = next.Offset == next.Source.Size
		copy(next.PrefixSHA256[:], f.prefix.Sum(nil))
		if next.Complete && next.Records != s.unit.CatalogNames {
			return stats, fmt.Errorf("catalog reducer count mismatch: expected %d, imported %d", s.unit.CatalogNames, next.Records)
		}
		if next.Complete && next.PrefixSHA256 != next.Source.SHA256 {
			return stats, errors.New("catalog changed after initial authentication")
		}
		first := f.cursor.Records + 1
		if err = f.commit(next, func(tx *stateTx) error {
			if len(page) == 0 {
				return nil
			}
			return tx.putCatalog(first, page)
		}); err != nil {
			return stats, err
		}
		stats.AdoptedNames += int64(len(page))
		event("sync_state_adoption_progress", map[string]any{"source": relative, "offset": next.Offset, "adopted_names": stats.AdoptedNames, "resumed_names": stats.ResumedNames})
		if next.Complete {
			return stats, nil
		}
	}
}

func (s *stateStore) verifyRelations(expected map[string]stateRelationDigest) error {
	actual, err := s.relationDigests()
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(actual, expected) {
		for relation, digest := range actual {
			if want, exists := expected[relation]; !exists || want != digest {
				return fmt.Errorf("legacy reducer parity mismatch in %s: expected %+v, indexed %+v; legacy inputs retained", relation, expected[relation], digest)
			}
		}
		return errors.New("legacy reducer parity has missing or extra relations; legacy inputs retained")
	}
	return nil
}

func (s stateAdoptSource) digest() string { return hex.EncodeToString(s.SHA256[:]) }
