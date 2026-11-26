package v2dict

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
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

// MaxPolicies bounds the table: Entry.Policy is a uint16 and 0 is reserved.
const MaxPolicies = math.MaxUint16

type policyTable struct {
	mu   sync.Mutex // appends and the index map
	f    *os.File
	byID atomic.Pointer[[][]byte] // suffix of index i at [i-1]; copy on write
	ix   map[string]uint16
	err  error // sticky: a failed write may leave a partial record
}

// openPolicies loads dir's policy table, without a torn last record.
func (t *policyTable) open(dir string) error {
	path := filepath.Join(dir, policyFile)
	data, err := os.ReadFile(path)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	t.ix = map[string]uint16{}
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
		if _, dup := t.ix[string(suffix)]; dup {
			break
		}
		byID = append(byID, suffix)
		t.ix[string(suffix)] = uint16(len(byID))
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
	return nil
}

// InternPolicy returns the index of an immutable policy suffix, appending
// and syncing it on first use. The index never changes or is reused.
func (d *Dict) InternPolicy(suffix []byte) (uint16, error) {
	t := &d.pol
	t.mu.Lock()
	defer t.mu.Unlock()
	if i, ok := t.ix[string(suffix)]; ok {
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
		return 0, fmt.Errorf("v2dict: policy table full or suffix too long (%d bytes)", len(suffix))
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
		t.err = fmt.Errorf("v2dict: policy append: %w", err)
		return 0, t.err
	}
	owned := slices.Clone(suffix)
	next := append(slices.Clip(byID), owned)
	t.byID.Store(&next)
	t.ix[string(owned)] = i
	return i, nil
}

// Policy returns the suffix of policy index i. It reports false for 0 (an
// entry with no recorded policy) and for an index never interned. The bytes
// are shared and must not be modified.
func (d *Dict) Policy(i uint16) ([]byte, bool) {
	byID := *d.pol.byID.Load()
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
