package v2convert

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"github.com/cockroachdb/pebble/sstable"
)

// Adapter owns source visibility/merge semantics and maps source entries to
// current core records. A raw SST set cannot resolve overlapping versions or
// range tombstones; adapters must reject unsupported input, never ignore it.
type Adapter interface {
	Format() string
	Convert(Entry) (Record, error)
}

// ExpandingAdapter accepts a physical multi-slot source value and emits its
// materialized logical records. The converter applies the same memory bound.
type ExpandingAdapter interface {
	Expand(Entry, func(Record) error) error
}
type Record struct{ Key, Value []byte }

// Writer is the replacement boundary for the dense part writer. Write must
// close/sync every file under dir before returning. No database reads occur.
type Writer interface {
	Format() string
	Write(context.Context, string, []Record) ([]string, error)
	Read(context.Context, string, func(Record) error) error
}
type Options struct {
	Directory  string
	PartBytes  int
	Adapter    Adapter
	Writer     Writer
	Checkpoint func(string) error
}
type Receipt struct {
	Version                            int
	Source, SourceSHA, Adapter, Writer string
	InputBytes                         int64
	Entries                            uint64
	LogicalDigest                      string
	Files                              []File
}
type File struct {
	Name, SHA256 string
	Bytes        int64
}

// ConvertSST commits one immutable input table as a part transaction. Large
// input tables are bounded by PartBytes and refused before unbounded growth.
// A receipt and all its output SSTs are atomically published by directory
// rename; interrupted staging is disposable. No per-name progress is stored.
// A production source adapter must first supply a coherent resolved snapshot.
func ConvertSST(ctx context.Context, source string, o Options) (receipt Receipt, err error) {
	if err = ctx.Err(); err != nil {
		return receipt, err
	}
	if o.Directory == "" || o.Adapter == nil || o.Writer == nil || o.PartBytes <= 0 {
		return receipt, errors.New("invalid conversion options")
	}
	if o.Adapter.Format() == "" || o.Writer.Format() == "" {
		return receipt, errors.New("empty format identity")
	}
	sum, n, err := FileSum(source)
	if err != nil {
		return receipt, err
	}
	identity := sha256.Sum256([]byte(sum + "\x00" + o.Adapter.Format() + "\x00" + o.Writer.Format()))
	name := hex.EncodeToString(identity[:])
	final := filepath.Join(o.Directory, name)
	if b, e := os.ReadFile(filepath.Join(final, "receipt.json")); e == nil {
		if e = json.Unmarshal(b, &receipt); e != nil {
			return receipt, e
		}
		if receipt.Version != 1 || receipt.SourceSHA != sum || receipt.Adapter != o.Adapter.Format() || receipt.Writer != o.Writer.Format() {
			return receipt, errors.New("receipt identity mismatch")
		}
		return receipt, Verify(ctx, final, receipt)
	} else if !errors.Is(e, os.ErrNotExist) {
		return receipt, e
	}
	if err = os.MkdirAll(o.Directory, 0700); err != nil {
		return receipt, err
	}
	stage, err := os.MkdirTemp(o.Directory, ".building-")
	if err != nil {
		return receipt, err
	}
	defer os.RemoveAll(stage)
	records := []Record{}
	used := 0
	accept := func(r Record) error {
		if o.PartBytes-used < 48 || len(r.Key) > o.PartBytes-used-48 || len(r.Value) > o.PartBytes-used-48-len(r.Key) {
			return errors.New("source table exceeds part memory limit; split resolved input tables first")
		}
		used += 48 + len(r.Key) + len(r.Value)
		records = append(records, Record{Key: bytes.Clone(r.Key), Value: bytes.Clone(r.Value)})
		return nil
	}
	stats, err := ScanSST(ctx, source, func(e Entry) error {
		if sstable.InternalKeyKind(e.Trailer&255) != sstable.InternalKeyKindSet {
			return errors.New("unresolved non-SET input: adapter requires a materialized snapshot")
		}
		if expanded, ok := o.Adapter.(ExpandingAdapter); ok {
			return expanded.Expand(e, accept)
		}
		r, err := o.Adapter.Convert(e)
		if err != nil {
			return err
		}
		return accept(r)
	})
	if err != nil {
		return receipt, err
	}
	if stats.RangeDeletes != 0 || stats.RangeKeys != 0 {
		return receipt, errors.New("range records require a resolved source snapshot")
	}
	receipt = Receipt{Version: 1, Source: filepath.Base(source), SourceSHA: sum, Adapter: o.Adapter.Format(), Writer: o.Writer.Format(), InputBytes: n, Entries: uint64(len(records)), LogicalDigest: stats.Digest}
	slices.SortFunc(records, func(a, b Record) int { return bytes.Compare(a.Key, b.Key) })
	expected := sha256.New()
	for _, r := range records {
		hashEntry(expected, Entry{Key: r.Key, Value: r.Value})
	}
	receipt.LogicalDigest = hex.EncodeToString(expected.Sum(nil))
	paths, err := o.Writer.Write(ctx, stage, records)
	if err != nil {
		return receipt, err
	}
	if len(records) > 0 && len(paths) == 0 {
		return receipt, errors.New("writer produced no files")
	}
	seen := map[string]bool{}
	for _, path := range paths {
		if filepath.Base(path) != path || path == "." || path == "receipt.json" || strings.HasPrefix(path, ".") || seen[path] {
			return receipt, errors.New("invalid or duplicate writer path")
		}
		seen[path] = true
		sum, size, e := FileSum(filepath.Join(stage, path))
		if e != nil {
			return receipt, e
		}
		receipt.Files = append(receipt.Files, File{path, sum, size})
	}
	seenRows := make([]bool, len(records))
	var count uint64
	for _, file := range receipt.Files {
		if err = o.Writer.Read(ctx, filepath.Join(stage, file.Name), func(r Record) error {
			i, ok := slices.BinarySearchFunc(records, r, func(a, b Record) int { return bytes.Compare(a.Key, b.Key) })
			if !ok || seenRows[i] || !sameValue(r.Key, records[i].Value, r.Value) {
				return errors.New("writer readback differs from transformed source")
			}
			seenRows[i] = true
			count++
			return nil
		}); err != nil {
			return receipt, err
		}
	}
	if count != receipt.Entries {
		return receipt, errors.New("writer readback differs from transformed source")
	}
	if o.Checkpoint != nil {
		if err = o.Checkpoint("files"); err != nil {
			return receipt, err
		}
	}
	data, err := json.MarshalIndent(receipt, "", "  ")
	if err != nil {
		return receipt, err
	}
	f, err := os.OpenFile(filepath.Join(stage, "receipt.json"), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return receipt, err
	}
	_, err = f.Write(data)
	err = errors.Join(err, f.Sync(), f.Close())
	if err != nil {
		return receipt, err
	}
	if err = syncDir(stage); err != nil {
		return receipt, err
	}
	if o.Checkpoint != nil {
		if err = o.Checkpoint("receipt"); err != nil {
			return receipt, err
		}
	}
	if err = os.Rename(stage, final); err != nil {
		return receipt, err
	}
	err = syncDir(o.Directory)
	return receipt, err
}
func syncDir(path string) error {
	f, e := os.Open(path)
	if e != nil {
		return e
	}
	return errors.Join(f.Sync(), f.Close())
}
func Verify(ctx context.Context, dir string, r Receipt) error {
	for _, f := range r.Files {
		if err := ctx.Err(); err != nil {
			return err
		}
		if filepath.Base(f.Name) != f.Name || f.Name == "." {
			return errors.New("unsafe receipt path")
		}
		sum, n, e := FileSum(filepath.Join(dir, f.Name))
		if e != nil {
			return e
		}
		if sum != f.SHA256 || n != f.Bytes {
			return fmt.Errorf("part checksum mismatch: %s", f.Name)
		}
	}
	return nil
}
