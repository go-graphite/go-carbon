package partsconvert

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"os"
	"path/filepath"
	"slices"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/objstorage/objstorageprovider"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/go-graphite/go-carbon/internal/partsformat"
)

type Table struct {
	Path                      string
	Prefix, Smallest, Largest []byte
	Digest                    string
	Records, Points, Bytes    uint64
}

func digestKV(h hash.Hash, key, value []byte) {
	var sizes [16]byte
	binary.BigEndian.PutUint64(sizes[:8], uint64(len(key)))
	binary.BigEndian.PutUint64(sizes[8:], uint64(len(value)))
	_, _ = h.Write(sizes[:])
	_, _ = h.Write(key)
	_, _ = h.Write(value)
}

func validateGroup(k partsformat.Key, group string) error {
	if string(k.Group) != group {
		return errors.New("source emitted another placement group")
	}
	return nil
}

func buildTables(dir, token string, index, limit int, input []Record) ([]Table, error) {
	type pair struct {
		key, value []byte
		points     uint64
		prefix     []byte
	}
	rows := make([]pair, 0, len(input))
	for _, r := range input {
		prefix, err := partsformat.PartitionPrefixForGroup(r.Key.Group, r.Key.VShard, r.Key.Partition)
		if err != nil {
			return nil, err
		}
		// Stable across a repeat scan of this immutable source and cursor.
		sum := sha256.Sum256(append([]byte(fmt.Sprintf("history-part:%s:%d:", token, index)), prefix...))
		copy(r.Key.Part[:], sum[:16])
		key, err := partsformat.Encode(r.Key)
		if err != nil {
			return nil, err
		}
		rows = append(rows, pair{key, r.Value, r.Points, append(bytes.Clone(prefix), r.Key.Part[:]...)})
	}
	slices.SortFunc(rows, func(a, b pair) int { return bytes.Compare(a.key, b.key) })
	var tables []Table
	for start := 0; start < len(rows); {
		if len(tables) >= limit {
			return nil, errors.New("conversion exceeds table manifest bound")
		}
		end := start + 1
		for end < len(rows) && bytes.Equal(rows[start].prefix, rows[end].prefix) {
			end++
		}
		path := filepath.Join(dir, fmt.Sprintf("part-%08d.sst", index+len(tables)))
		f, err := vfs.Default.Create(path)
		if err != nil {
			return nil, err
		}
		w := sstable.NewWriter(objstorageprovider.NewFileWritable(f), sstable.WriterOptions{Compression: sstable.SnappyCompression})
		h := sha256.New()
		t := Table{Path: path, Prefix: bytes.Clone(rows[start].prefix), Smallest: bytes.Clone(rows[start].key), Largest: bytes.Clone(rows[end-1].key)}
		for i := start; i < end; i++ {
			if i > start && bytes.Equal(rows[i-1].key, rows[i].key) {
				_ = w.Close()
				return nil, errors.New("duplicate source archive block")
			}
			if err = w.Set(rows[i].key, rows[i].value); err != nil {
				_ = w.Close()
				return nil, err
			}
			digestKV(h, rows[i].key, rows[i].value)
			t.Records++
			t.Points += rows[i].points
		}
		if err = w.Close(); err != nil {
			return nil, err
		}
		// Pebble's Writer.Close does not promise fsync for external staging.
		file, err := os.OpenFile(path, os.O_RDWR, 0)
		if err != nil {
			return nil, err
		}
		err = errors.Join(file.Sync(), file.Close())
		if err != nil {
			return nil, err
		}
		info, err := os.Stat(path)
		if err != nil {
			return nil, err
		}
		t.Bytes = uint64(info.Size())
		t.Digest = hex.EncodeToString(h.Sum(nil))
		tables = append(tables, t)
		start = end
	}
	return tables, nil
}

// PebbleTarget uses stock external SST ingestion, which chooses the lowest
// non-overlapping level. Stats exposes actual L0 ingestion; bottom-level
// placement is an observed result, not a promise to bypass overlapping files.
// DB must be the same physical owner selected by the durable ownership map.
type PebbleTarget struct {
	DB            *pebble.DB
	DestinationID string
	OnIngest      func(pebble.IngestOperationStats)
}

func (p PebbleTarget) Identity() string { return p.DestinationID }

func (p PebbleTarget) Ingest(ctx context.Context, tables []Table) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(tables) == 0 {
		return nil
	}
	paths := make([]string, len(tables))
	defer func() {
		for _, path := range paths {
			if path != "" {
				_ = os.Remove(path)
			}
		}
	}()
	for i, t := range tables {
		// Pebble takes ownership of local ingest paths and removes them on
		// success. Keep a durable original for the ambiguous-crash retry.
		paths[i] = t.Path + ".ingest"
		if err := os.Remove(paths[i]); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
		if err := os.Link(t.Path, paths[i]); err != nil {
			return fmt.Errorf("retain replayable SST link: %w", err)
		}
	}
	stats, err := p.DB.IngestWithStats(paths)
	if err == nil && p.OnIngest != nil {
		p.OnIngest(stats)
	}
	return err
}
func (p PebbleTarget) Verify(ctx context.Context, tables []Table) error {
	snap := p.DB.NewSnapshot()
	defer snap.Close()
	for _, t := range tables {
		if err := ctx.Err(); err != nil {
			return err
		}
		iter, err := snap.NewIter(&pebble.IterOptions{LowerBound: t.Prefix, UpperBound: partsformat.PrefixEnd(t.Prefix)})
		if err != nil {
			return err
		}
		h := sha256.New()
		var count uint64
		for ok := iter.First(); ok; ok = iter.Next() {
			if err = ctx.Err(); err != nil {
				break
			}
			digestKV(h, iter.Key(), iter.Value())
			count++
		}
		err = errors.Join(err, iter.Error(), iter.Close())
		if err != nil {
			return err
		}
		if count != t.Records || hex.EncodeToString(h.Sum(nil)) != t.Digest {
			return fmt.Errorf("archive digest mismatch for %s (%d/%d records)", filepath.Base(t.Path), count, t.Records)
		}
	}
	return nil
}
