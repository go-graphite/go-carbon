package v2convert

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"slices"

	"strings"

	"github.com/go-graphite/go-carbon/internal/partsformat"
	"github.com/go-graphite/go-carbon/internal/sealrollup"
	"github.com/go-graphite/go-carbon/internal/v2core"
	"github.com/go-graphite/go-carbon/internal/v2part"
)

// CoreRecords accepts ONLY materialized current-core GCP3 records. Native
// chunkstore values are deliberately rejected: they need an independently
// verified physical-slot/continuation adapter before conversion is safe.
type CoreRecords struct{}

func (CoreRecords) Format() string { return "gcp3-materialized-sealrollup-v2" }
func (CoreRecords) Convert(e Entry) (Record, error) {
	if e.Trailer>>8 != 0 {
		return Record{}, errors.New("nonzero materialized sequence")
	}
	k, err := partsformat.Decode(e.Key)
	if err != nil {
		return Record{}, fmt.Errorf("source is not current core GCP3: %w", err)
	}
	if !validPartition(k.Partition) {
		return Record{}, errors.New("unsupported core rollup partition")
	}
	if k.Archive == math.MaxUint32 {
		// Envelopes convert to the canonical encoding of their decoded record,
		// so records and digests do not depend on how a source stored them
		// (legacy JSON, full binary, or dense parts without the definition).
		m, err := v2core.DecodeRollupMetadata(k.Metric, k.Group, e.Value)
		if err != nil {
			return Record{}, errors.New("invalid core metadata")
		}
		return Record{e.Key, v2core.AppendRollupMetadata(nil, m)}, nil
	} else if _, err = sealrollup.DecodeRecord(int(k.Archive), k.BlockStart, e.Value); err != nil {
		return Record{}, err
	}
	return Record{e.Key, e.Value}, nil
}

// Live seals partition by UTC calendar month; zero is the pre-partition layout.
func validPartition(p int64) bool { return p == 0 || v2core.RollupMonthPartition(p) == p }

// Expand is the shared dense/source adapter. It reconstructs the same logical
// singleton stream, so an already-dense part can be repacked and reverified.
func (c CoreRecords) Expand(e Entry, emit func(Record) error) error {
	k, err := v2part.Decode(e.Key)
	if err != nil {
		return err
	}
	if k.Archive == v2part.MetadataArchive {
		r, err := c.Convert(e)
		if err != nil {
			return err
		}
		return emit(r)
	}
	if k.Ordinal != 0 || !validPartition(k.Partition) || e.Trailer>>8 != 0 {
		return errors.New("unsupported materialized source key/sequence")
	}
	envelope, run, err := v2part.SplitHeader(e.Value)
	if err != nil {
		return err
	}
	records, err := v2part.DecodeSlots(k.Archive, k.BlockStart, run)
	if err != nil {
		return err
	}
	for _, r := range records {
		k.BlockStart = r.Slot
		key, err := v2part.Encode(k)
		if err != nil {
			return err
		}
		if err = emit(Record{Key: key, Value: sealrollup.EncodeRecord(r)}); err != nil {
			return err
		}
	}
	if envelope == nil {
		return nil
	}
	// A folded envelope follows its metric's last run in the logical stream.
	key, err := v2part.Encode(v2part.EnvelopeKey(k))
	if err != nil {
		return err
	}
	return c.Expand(Entry{Key: key, Value: envelope, Trailer: e.Trailer}, emit)
}

// CoreWriter uses the common dense part writer and verifies through its reader.
// Source must identify the original ordering stream, never this conversion run.
type CoreWriter struct{ Source v2part.Provenance }

func (cw CoreWriter) Format() string {
	// Resume identity must include immutable producer provenance, not just codec.
	identity := sha256.Sum256(append(cw.Source.Incarnation[:], []byte(cw.Source.Stream)...))
	return fmt.Sprintf("%s-sst-v1:%x", v2part.DenseEncoding, identity)
}
func (cw CoreWriter) Write(ctx context.Context, dir string, rows []Record) ([]string, error) {
	slices.SortFunc(rows, func(a, b Record) int { return bytes.Compare(a.Key, b.Key) })
	type scope struct {
		group     v2part.Group
		partition int64
		part      v2part.PartID
	}
	var scopes []scope
	files := map[scope][]v2core.KV{}
	for i, row := range rows {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if i > 0 && bytes.Equal(rows[i-1].Key, row.Key) {
			return nil, errors.New("duplicate materialized key")
		}
		if _, e := (CoreRecords{}).Convert(Entry{Key: row.Key, Value: row.Value}); e != nil {
			return nil, e
		}
		k, _ := v2part.Decode(row.Key)
		id := scope{k.Group, k.Partition, k.Part}
		if _, ok := files[id]; !ok {
			scopes = append(scopes, id)
		}
		files[id] = append(files[id], v2core.KV{Key: row.Key, Value: row.Value})
	}
	var paths []string
	for _, id := range scopes {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		name := fmt.Sprintf("part-%06d.sst", len(paths))
		path := filepath.Join(dir, name)
		d, err := v2core.WriteDenseRollupPart(path, files[id], cw.Source)
		if err != nil {
			return nil, err
		}
		name = fmt.Sprintf("%x.sst", d.SHA256)
		if err = os.Rename(path, filepath.Join(dir, name)); err != nil {
			return nil, err
		}
		b, err := d.MarshalBinary()
		if err != nil {
			return nil, err
		}
		desc := name + ".descriptor"
		f, err := os.OpenFile(filepath.Join(dir, desc), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		if err != nil {
			return nil, err
		}
		_, err = f.Write(b)
		err = errors.Join(err, f.Sync(), f.Close())
		if err != nil {
			return nil, err
		}
		paths = append(paths, name, desc)
	}
	return paths, nil
}
func (CoreWriter) Read(ctx context.Context, path string, fn func(Record) error) error {
	if strings.HasSuffix(path, ".descriptor") {
		b, e := os.ReadFile(path)
		if e != nil {
			return e
		}
		_, e = v2part.ParseDescriptor(b)
		return e
	}
	b, e := os.ReadFile(path + ".descriptor")
	if e != nil {
		return e
	}
	d, e := v2part.ParseDescriptor(b)
	if e != nil {
		return e
	}
	return v2part.ReadExpanded(ctx, path, &d, func(k v2part.Key, value []byte) error {
		key, e := v2part.Encode(k)
		if e != nil {
			return e
		}
		return fn(Record{Key: key, Value: value})
	})
}

// sameValue reports whether two stored values of key hold the same logical
// record. Dense parts store envelopes without the definition, so envelopes
// compare decoded, definition bytes included.
func sameValue(key, a, b []byte) bool {
	if bytes.Equal(a, b) {
		return true
	}
	k, err := v2part.Decode(key)
	if err != nil || k.Archive != v2part.MetadataArchive {
		return false
	}
	x, errA := v2core.DecodeRollupMetadata(k.Metric, k.Group, a)
	y, errB := v2core.DecodeRollupMetadata(k.Metric, k.Group, b)
	return errA == nil && errB == nil && reflect.DeepEqual(x, y)
}
