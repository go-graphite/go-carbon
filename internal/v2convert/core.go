package v2convert

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"slices"

	"github.com/cockroachdb/pebble/sstable"
	"github.com/go-graphite/go-carbon/internal/partsformat"
	"github.com/go-graphite/go-carbon/internal/sealrollup"
)

// CoreRecords accepts ONLY materialized current-core GCP3 records. Native
// chunkstore values are deliberately rejected: they need an independently
// verified physical-slot/continuation adapter before conversion is safe.
type CoreRecords struct{}

func (CoreRecords) Format() string { return "gcp3-materialized-sealrollup-v1" }
func (CoreRecords) Convert(e Entry) (Record, error) {
	k, err := partsformat.Decode(e.Key)
	if err != nil {
		return Record{}, fmt.Errorf("source is not current core GCP3: %w", err)
	}
	if k.Partition != 0 {
		return Record{}, errors.New("unsupported core rollup partition")
	}
	if k.Archive == math.MaxUint32 {
		if !json.Valid(e.Value) {
			return Record{}, errors.New("invalid core metadata")
		}
	} else if _, err = sealrollup.DecodeRecord(int(k.Archive), k.BlockStart, e.Value); err != nil {
		return Record{}, err
	}
	return Record{e.Key, e.Value}, nil
}

// CoreWriter retains current core bytes. Larger blocks reduce table overhead,
// but singleton values remain singleton values: this writer does not claim the
// native-density gate. One SST never crosses a placement-group/vshard boundary.
type CoreWriter struct{}

func (CoreWriter) Format() string { return "gcp3-sst-zstd-256k-v1" }
func (CoreWriter) Write(ctx context.Context, dir string, rows []Record) (paths []string, err error) {
	slices.SortFunc(rows, func(a, b Record) int { return bytes.Compare(a.Key, b.Key) })
	var w *sstable.Writer
	var ownership string
	defer func() {
		if w != nil {
			err = errors.Join(err, w.Close())
		}
	}()
	for i, r := range rows {
		if err = ctx.Err(); err != nil {
			return paths, err
		}
		if i > 0 && bytes.Equal(rows[i-1].Key, r.Key) {
			return paths, errors.New("duplicate materialized key")
		}
		if _, err = (CoreRecords{}).Convert(Entry{Key: r.Key, Value: r.Value}); err != nil {
			return paths, err
		}
		k, _ := partsformat.Decode(r.Key)
		lower, _, e := partsformat.VShardRangeForGroup(k.Group, k.VShard)
		if e != nil {
			return paths, e
		}
		if w == nil || ownership != string(lower) {
			if w != nil {
				e = w.Close()
				w = nil
				if e != nil {
					return paths, e
				}
			}
			name := fmt.Sprintf("part-%06d.sst", len(paths))
			f, e := os.OpenFile(filepath.Join(dir, name), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
			if e != nil {
				return paths, e
			}
			w = sstable.NewWriter(&partFile{f}, sstable.WriterOptions{Compression: sstable.ZstdCompression, BlockSize: 256 << 10, BlockRestartInterval: 64})
			paths = append(paths, name)
			ownership = string(lower)
		}
		if err = w.Set(r.Key, r.Value); err != nil {
			return paths, err
		}
	}
	if w != nil {
		err = w.Close()
		w = nil
	}
	return paths, err
}

func (CoreWriter) Read(ctx context.Context, path string, fn func(Record) error) error {
	stats, err := ScanSST(ctx, path, func(e Entry) error {
		if sstable.InternalKeyKind(e.Trailer&255) != sstable.InternalKeyKindSet {
			return errors.New("non-SET output")
		}
		return fn(Record{e.Key, e.Value})
	})
	if err != nil {
		return err
	}
	if stats.RangeDeletes != 0 || stats.RangeKeys != 0 {
		return errors.New("range records in output")
	}
	return nil
}

type partFile struct{ f *os.File }

func (p *partFile) Write(b []byte) error {
	n, e := p.f.Write(b)
	if e == nil && n != len(b) {
		e = io.ErrShortWrite
	}
	return e
}
func (p *partFile) Finish() error { return errors.Join(p.f.Sync(), p.f.Close()) }
func (p *partFile) Abort()        { p.f.Close() }
