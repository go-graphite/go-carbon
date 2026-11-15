package main

// This opt-in offline seal adapter keeps original per-metric UpdateMany batch
// boundaries and clocks explicit. It deliberately does not infer them from the
// prototype's flattened raw fixture: that information has already been lost.
import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"slices"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/objstorage/objstorageprovider"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/go-graphite/go-carbon/internal/sealrollup"
)

// sealArchiveInput belongs to one canonical metric ID. Config must carry the
// actual stored retention counts, not only the prototype's resolution list.
type sealArchiveInput struct {
	ID      uint64
	Config  sealrollup.Config
	View    sealrollup.View
	Batches []sealrollup.Batch
}
type sealArchiveDelta struct {
	ID    uint64
	Delta sealrollup.Delta
}

// sealArchiveParts computes all archive replacements at seal time and atomically
// ingests them as one immutable SST. Disabled callers use the original seal.
// There are no DB reads or shared locks. Views are owner-local immutable inputs;
// only publish/install returned deltas AFTER successful ingest. The caller must
// atomically couple the corresponding raw rows and log frontier before using
// this helper in a durable daemon. This standalone experimental SST has neither.
func sealArchiveParts(db *pebble.DB, dir string, shard int, part uint64, inputs []sealArchiveInput, enabled bool) (uint64, []sealArchiveDelta, error) {
	if !enabled {
		return 0, nil, nil
	}
	var deltas []sealArchiveDelta
	seen := map[uint64]bool{}
	for _, input := range inputs {
		if seen[input.ID] {
			return 0, nil, fmt.Errorf("duplicate metric ID in archive seal")
		}
		seen[input.ID] = true
		ds, err := sealrollup.Reduce(input.Config, input.View, input.Batches)
		if err != nil {
			return 0, nil, err
		}
		for _, d := range ds {
			deltas = append(deltas, sealArchiveDelta{input.ID, d})
		}
	}
	if len(deltas) == 0 {
		return 0, nil, nil
	}
	slices.SortFunc(deltas, func(a, b sealArchiveDelta) int {
		if a.ID < b.ID {
			return -1
		}
		if a.ID > b.ID {
			return 1
		}
		if a.Delta.Archive < b.Delta.Archive {
			return -1
		}
		if a.Delta.Archive > b.Delta.Archive {
			return 1
		}
		if a.Delta.Slot < b.Delta.Slot {
			return -1
		}
		if a.Delta.Slot > b.Delta.Slot {
			return 1
		}
		return 0
	})
	path := filepath.Join(dir, fmt.Sprintf("rollup-%03d-%020d.sst", shard, part))
	defer os.Remove(path)
	f, err := vfs.Default.Create(path)
	if err != nil {
		return 0, nil, err
	}
	w := sstable.NewWriter(objstorageprovider.NewFileWritable(f), sstable.WriterOptions{Compression: sstable.SnappyCompression})
	for _, r := range deltas {
		k, v := sealArchiveKV(r, part)
		if err = w.Set(k, v); err != nil {
			_ = w.Close()
			return 0, nil, err
		}
	}
	if err = w.Close(); err != nil {
		return 0, nil, err
	}
	st, err := os.Stat(path)
	if err != nil {
		return 0, nil, err
	}
	if err = db.Ingest([]string{path}); err != nil {
		return 0, nil, err
	}
	return uint64(st.Size()), deltas, nil
}

// Versioned, distinct namespace: metric/physical archive slot/part. Readers
// must choose latest Order before checking the stored timestamp. Equal Order
// conflicting replacements are corruption, not a timestamp tie to guess at.
func sealArchiveKV(r sealArchiveDelta, part uint64) ([]byte, []byte) {
	k := make([]byte, 30)
	k[0] = 'R'
	k[1] = 1
	binary.BigEndian.PutUint64(k[2:], r.ID)
	binary.BigEndian.PutUint32(k[10:], uint32(r.Delta.Archive))
	binary.BigEndian.PutUint64(k[14:], uint64(r.Delta.Slot))
	binary.BigEndian.PutUint64(k[22:], part)
	v := make([]byte, 24)
	binary.LittleEndian.PutUint64(v, uint64(r.Delta.Sample.Time))
	binary.LittleEndian.PutUint64(v[8:], r.Delta.Sample.Bits)
	binary.LittleEndian.PutUint64(v[16:], r.Delta.Order)
	return k, v
}
