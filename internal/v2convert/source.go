// Package v2convert contains offline, part-resumable conversion primitives.
// Source files must be immutable and owned by the caller. Never open a running
// database here: a loose SST set is not a logical Pebble snapshot.
package v2convert

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
	"os"

	"github.com/cockroachdb/pebble/sstable"
)

// Entry retains Pebble's internal sequence and kind. Dropping these while
// scanning a live store's SST set would resurrect overwritten/deleted values.
type Entry struct {
	Key, Value []byte
	Trailer    uint64
}
type ScanStats struct {
	Bytes                         int64
	Entries, KeyBytes, ValueBytes uint64
	Kinds                         map[string]uint64
	Prefixes                      map[string]uint64
	Digest                        string
	RangeDeletes, RangeKeys       uint64
}

func hashEntry(h hash.Hash, e Entry) {
	var b [24]byte
	binary.LittleEndian.PutUint64(b[:8], uint64(len(e.Key)))
	binary.LittleEndian.PutUint64(b[8:16], uint64(len(e.Value)))
	binary.LittleEndian.PutUint64(b[16:], e.Trailer)
	h.Write(b[:])
	h.Write(e.Key)
	h.Write(e.Value)
}

// ScanSST walks each data block forwards, without per-name lookups. Callback
// slices are borrowed. It inventories range records too; point entries alone
// must never be treated as an authoritative database image.
func ScanSST(ctx context.Context, path string, fn func(Entry) error) (out ScanStats, err error) {
	out.Kinds = map[string]uint64{}
	out.Prefixes = map[string]uint64{}
	f, err := os.Open(path)
	if err != nil {
		return out, err
	}
	st, err := f.Stat()
	if err != nil {
		f.Close()
		return out, err
	}
	out.Bytes = st.Size()
	readable, err := sstable.NewSimpleReadable(f)
	if err != nil {
		f.Close()
		return out, err
	}
	r, err := sstable.NewReader(readable, sstable.ReaderOptions{}, sstable.Mergers{"go-carbon.chunks.v1": nil, "go-carbon.chunks.exact.v1": nil})
	if err != nil {
		readable.Close()
		return out, err
	}
	defer func() { err = errors.Join(err, r.Close()) }()
	out.RangeDeletes = r.Properties.NumRangeDeletions
	out.RangeKeys = r.Properties.NumRangeKeys()
	it, err := r.NewIter(nil, nil)
	if err != nil {
		return out, err
	}
	defer func() { err = errors.Join(err, it.Close()) }()
	h := sha256.New()
	var prefixes [256]string
	for i := range prefixes {
		prefixes[i] = fmt.Sprintf("%02x", i)
	}
	for k, v := it.First(); k != nil; k, v = it.Next() {
		if err = ctx.Err(); err != nil {
			return out, err
		}
		value, _, e := v.Value(nil)
		if e != nil {
			return out, e
		}
		entry := Entry{Key: k.UserKey, Value: value, Trailer: k.Trailer}
		hashEntry(h, entry)
		out.Entries++
		out.KeyBytes += uint64(len(entry.Key))
		out.ValueBytes += uint64(len(value))
		out.Kinds[k.Kind().String()]++
		prefix := "empty"
		if len(entry.Key) > 0 {
			prefix = prefixes[entry.Key[0]]
		}
		out.Prefixes[prefix]++
		if fn != nil {
			if err = fn(entry); err != nil {
				return out, err
			}
		}
	}
	if err = it.Error(); err != nil {
		return out, err
	}
	out.Digest = hex.EncodeToString(h.Sum(nil))
	return out, nil
}

func FileSum(path string) (string, int64, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", 0, err
	}
	defer f.Close()
	h := sha256.New()
	n, err := io.Copy(h, f)
	return hex.EncodeToString(h.Sum(nil)), n, err
}
