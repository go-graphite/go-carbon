package v2dict

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"maps"
	"math"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"sync/atomic"
)

// The policy table gives Entry.Policy a meaning: index i > 0 names the
// i-th immutable policy suffix (the canonical definition after the name, as
// the v2 journal writes it). Policy 0 is "none recorded": an entry written
// before the table existed, whose definition still lives in the producer
// catalog. A suffix is written and synced before its index is returned, so
// every entry that uses an index finds it after any kill. The table is one
// small append-only file beside the levels (schema x aggregation pairs, a
// few dozen), never merged, which keeps the level and log formats unchanged:
// an old dictionary simply has no file and reads every entry as policy 0.
const policyFile = "policies"

// groupFile holds placement group labels in the same record format.
const groupFile = "groups"

// MaxPolicies bounds the table: Entry.Policy is a uint16 and 0 is reserved.
const MaxPolicies = math.MaxUint16

type policyTable struct {
	file string
	mu   sync.Mutex // appends
	f    *os.File
	byID atomic.Pointer[[][]byte] // suffix of index i at [i-1]; copy on write
	// ix maps a suffix to its index, copy on write like byID, so a known
	// suffix (every name admitted after its policy's first) takes no lock.
	ix  atomic.Pointer[map[string]uint16]
	err error // sticky: a failed write may leave a partial record
}

// openPolicies loads dir's policy table, without a torn last record.
func (t *policyTable) open(dir, file string) error {
	t.file = file
	path := filepath.Join(dir, file)
	data, err := os.ReadFile(path)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	ix := map[string]uint16{}
	var byID [][]byte
	off := 0
	for off+8 <= len(data) {
		size := int(binary.LittleEndian.Uint32(data[off:]))
		if size < 2 || size > len(data)-off-8 || crc32.Checksum(data[off+8:off+8+size], castagnoli) != binary.LittleEndian.Uint32(data[off+4:]) {
			break
		}
		p := data[off+8 : off+8+size]
		if int(binary.LittleEndian.Uint16(p)) != len(byID)+1 {
			break
		}
		suffix := slices.Clone(p[2:])
		if _, dup := ix[string(suffix)]; dup {
			break
		}
		byID = append(byID, suffix)
		ix[string(suffix)] = uint16(len(byID))
		off += 8 + size
	}
	if t.f, err = os.OpenFile(path, os.O_WRONLY|os.O_CREATE, 0o644); err != nil {
		return err
	}
	if err = t.f.Truncate(int64(off)); err == nil {
		_, err = t.f.Seek(int64(off), 0)
	}
	if err != nil {
		t.f.Close()
		t.f = nil
		return err
	}
	t.byID.Store(&byID)
	t.ix.Store(&ix)
	return nil
}

// InternPolicy returns the index of an immutable policy suffix, appending
// and syncing it on first use. The index never changes or is reused.
func (d *Dict) InternPolicy(suffix []byte) (uint16, error) { return d.internTable(&d.pol, suffix) }

// InternGroup returns the durable index (from 1) of a placement group label,
// recording it once like a policy suffix. Packed engine IDs carry it.
func (d *Dict) InternGroup(group string) (uint16, error) {
	return d.internTable(&d.grp, []byte(group))
}

// Group returns the label of group index i, lock-free.
func (d *Dict) Group(i uint16) (string, bool) {
	g, ok := d.grp.get(i)
	return string(g), ok
}

// internTable appends key to t once, durably, and returns its index.
func (d *Dict) internTable(t *policyTable, suffix []byte) (uint16, error) {
	if ix := t.ix.Load(); ix != nil {
		if i, ok := (*ix)[string(suffix)]; ok {
			return i, nil
		}
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	var ix map[string]uint16
	if p := t.ix.Load(); p != nil {
		ix = *p
	}
	if i, ok := ix[string(suffix)]; ok {
		return i, nil
	}
	if err := d.writableShared(); err != nil {
		return 0, err
	}
	if t.f == nil {
		return 0, errors.New("v2dict: closed")
	}
	if t.err != nil {
		return 0, t.err
	}
	byID := *t.byID.Load()
	if len(byID) >= MaxPolicies || len(suffix)+2 > math.MaxUint32-8 {
		return 0, fmt.Errorf("v2dict: %s table full or key too long (%d bytes)", t.file, len(suffix))
	}
	i := uint16(len(byID) + 1)
	rec := make([]byte, 8, 10+len(suffix))
	rec = binary.LittleEndian.AppendUint16(rec, i)
	rec = append(rec, suffix...)
	binary.LittleEndian.PutUint32(rec, uint32(len(rec)-8))
	binary.LittleEndian.PutUint32(rec[4:], crc32.Checksum(rec[8:], castagnoli))
	_, err := t.f.Write(rec)
	if err == nil {
		err = t.f.Sync()
	}
	if err == nil && i == 1 {
		// The file may be new: make its name durable with its first record.
		err = syncDir(d.dir)
	}
	if err != nil {
		t.err = fmt.Errorf("v2dict: %s append: %w", t.file, err)
		return 0, t.err
	}
	owned := slices.Clone(suffix)
	next := append(slices.Clip(byID), owned)
	t.byID.Store(&next)
	nix := maps.Clone(ix)
	if nix == nil {
		nix = map[string]uint16{}
	}
	nix[string(owned)] = i
	t.ix.Store(&nix)
	return i, nil
}

// Policy returns the suffix of policy index i. It reports false for 0 (an
// entry with no recorded policy) and for an index never interned. The bytes
// are shared and must not be modified.
func (d *Dict) Policy(i uint16) ([]byte, bool) { return d.pol.get(i) }

func (t *policyTable) get(i uint16) ([]byte, bool) {
	byID := *t.byID.Load()
	if i == 0 || int(i) > len(byID) {
		return nil, false
	}
	return byID[i-1], true
}

func (t *policyTable) close() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.f == nil {
		return nil
	}
	err := t.f.Close()
	t.f = nil
	return err
}
