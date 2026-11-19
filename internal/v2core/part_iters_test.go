package v2core

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"math"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/go-graphite/go-carbon/internal/sealrollup"
	"github.com/go-graphite/go-carbon/internal/v2part"
)

// TestOpenedPartItersMatchMerged reads one name spread over several real part
// files twice: through per-part cursors (opened parts) and through the one
// merged iterator. Both must yield the same winners, in slot order. A legacy
// LSM row that disagrees with a part row must still fail the read.
func TestOpenedPartItersMatchMerged(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "parts")
	if err := os.Mkdir(dir, 0700); err != nil {
		t.Fatal(err)
	}
	db, err := pebble.Open(filepath.Join(root, "manifest"), &pebble.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.Set(rollupDirectoryKey, []byte(root), pebble.NoSync); err != nil {
		t.Fatal(err)
	}
	const name = "partiters.metric"
	_, raw := rollupTestPolicy(t, name, "1s:1h,10s:1d", sealrollup.Last, .5)
	var firstKV coreKV
	install := func(number int, rows []coreKV) {
		t.Helper()
		var part v2part.PartID
		binary.BigEndian.PutUint64(part[:], uint64(number+1))
		b := &partTableBuffer{}
		w := sstable.NewWriter(b, sstable.WriterOptions{Compression: sstable.NoCompression})
		var d v2part.Descriptor
		for i, kv := range rows {
			key, err := v2part.Decode(kv.Key)
			if err != nil {
				t.Fatal(err)
			}
			key.Part = part
			encoded, err := v2part.Encode(key)
			if err != nil {
				t.Fatal(err)
			}
			if i == 0 {
				template, err := WriteDenseRollupPart(filepath.Join(root, hex.EncodeToString(part[:])+".template.sst"), rows, v2part.Provenance{Incarnation: v2part.PartID{1}, Stream: "test"})
				if err != nil {
					t.Fatal(err)
				}
				d = template
				d.Scope.Part = part
				d.FirstKey = bytes.Clone(encoded)
				d.Scope.Group = key.Group
				d.Scope.VShard = key.VShard
				d.VShards = []uint16{key.VShard}
			}
			if number == 0 && key.Archive != v2part.MetadataArchive && firstKV.Key == nil {
				firstKV = coreKV{Key: bytes.Clone(encoded)}
			}
			d.LastKey = bytes.Clone(encoded)
			if err := w.Set(encoded, kv.encodedValue()); err != nil {
				t.Fatal(err)
			}
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
		d.Bytes = uint64(b.Len())
		d.SHA256 = sha256.Sum256(b.Bytes())
		if err := os.WriteFile(filepath.Join(dir, hex.EncodeToString(d.SHA256[:])+".sst"), b.Bytes(), 0600); err != nil {
			t.Fatal(err)
		}
		encoded, err := d.MarshalBinary()
		if err != nil {
			t.Fatal(err)
		}
		if err := db.Set(partBinding(sealedPartPrefix, d.SHA256), encoded, pebble.NoSync); err != nil {
			t.Fatal(err)
		}
	}
	// Five independent seals. Later seals rewrite some earlier slots with a
	// newer order, and every seal lands in the same open 10 s bucket, so
	// slots repeat across parts and must reduce to one winner each.
	const base = 1700000000
	planner := newCoreRollups()
	for p := range 5 {
		var points []Row
		for i := range 12 {
			ts := int64(base + p*7 + i)
			points = append(points, Row{ID: uint64(p*100 + i + 1), Time: ts, Bits: math.Float64bits(float64(p*1000 + i)), Seq: uint64(p*100 + i + 1)})
		}
		plan, err := planner.plan(coreSealInput{Batches: []Batch{{ID: 1, Policy: raw, Order: uint64(p + 1), Clock: base + 100, Points: points}}})
		if err != nil {
			t.Fatal(err)
		}
		// Install the newest seals first so part order differs from slot order.
		install(4-p, plan.Files[0])
		planner.release()
	}
	inventory, err := loadRollupInventory(db)
	if err != nil {
		t.Fatal(err)
	}
	read := func(t *testing.T, withMerged bool) (opened, merged [][]sealrollup.Record, openedErr error) {
		t.Helper()
		snap := db.NewSnapshot()
		defer func() { _ = snap.Close() }()
		parts, err := OpenRollupParts(context.Background(), snap, inventory, nil, testBlockCache(t), 4<<20)
		if err != nil {
			t.Fatal(err)
		}
		defer parts.Close()
		byNames, err := parts.SnapshotForNames(context.Background(), snap, []string{name}, NewRollupReadCache(1<<20))
		if err != nil {
			t.Fatal(err)
		}
		var whole *RollupSnapshot
		if withMerged {
			if whole, err = NewRollupSnapshot(context.Background(), snap, nil, nil, inventory); err != nil {
				t.Fatal(err)
			}
			if whole.opened != nil {
				t.Fatal("merged reference must not use opened parts")
			}
		}
		policy, _, ok := byNames.LookupMetadata(name)
		if !ok {
			t.Fatal("metadata missing")
		}
		for archive, a := range policy.Config.Archives {
			var o, m []sealrollup.Record
			if err := byNames.ScanArchive(context.Background(), name, archive, 0, a.Count, func(r sealrollup.Record) error { o = append(o, r); return nil }); err != nil {
				return nil, nil, err
			}
			if whole == nil {
				opened = append(opened, o)
				continue
			}
			if err := whole.ScanArchive(context.Background(), name, archive, 0, a.Count, func(r sealrollup.Record) error { m = append(m, r); return nil }); err != nil {
				t.Fatal(err)
			}
			opened, merged = append(opened, o), append(merged, m)
		}
		return opened, merged, nil
	}
	check := func(t *testing.T) {
		opened, merged, err := read(t, true)
		if err != nil {
			t.Fatal(err)
		}
		total := 0
		for archive := range opened {
			o, m := opened[archive], merged[archive]
			if !slices.IsSortedFunc(o, func(a, b sealrollup.Record) int { return int(a.Slot - b.Slot) }) {
				t.Fatalf("archive %d: opened records not in slot order", archive)
			}
			slices.SortFunc(m, func(a, b sealrollup.Record) int { return int(a.Slot - b.Slot) })
			if len(o) != len(m) {
				t.Fatalf("archive %d: opened %d records, merged %d", archive, len(o), len(m))
			}
			for i := range o {
				if o[i] != m[i] {
					t.Fatalf("archive %d slot %d: opened %+v merged %+v", archive, o[i].Slot, o[i], m[i])
				}
			}
			total += len(o)
		}
		if total < 40 {
			t.Fatalf("fixture too small: %d records", total)
		}
	}
	t.Run("no legacy rows", func(t *testing.T) {
		snap := db.NewSnapshot()
		parts, err := OpenRollupParts(context.Background(), snap, inventory, nil, testBlockCache(t), 4<<20)
		if err != nil {
			t.Fatal(err)
		}
		legacy := parts.legacyRows
		parts.Close()
		_ = snap.Close()
		if legacy {
			t.Fatal("a store without LSM rollup rows must not open the legacy iterator")
		}
		check(t)
	})
	if firstKV.Key == nil {
		t.Fatal("fixture has no archive row")
	}
	partValue := func() []byte {
		snap := db.NewSnapshot()
		defer func() { _ = snap.Close() }()
		parts, err := OpenRollupParts(context.Background(), snap, inventory, nil, testBlockCache(t), 4<<20)
		if err != nil {
			t.Fatal(err)
		}
		defer parts.Close()
		k, err := v2part.Decode(firstKV.Key)
		if err != nil {
			t.Fatal(err)
		}
		c, err := parts.partCursor(context.Background(), snap, archivePartIdentity{k.Group, k.Part})
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close()
		if !c.SeekGE(firstKV.Key) || !bytes.Equal(c.Key(), firstKV.Key) {
			t.Fatal("part row missing")
		}
		return bytes.Clone(c.Value())
	}()
	t.Run("identical legacy row", func(t *testing.T) {
		if err := db.Set(firstKV.Key, partValue, pebble.NoSync); err != nil {
			t.Fatal(err)
		}
		check(t)
	})
	t.Run("conflicting legacy row", func(t *testing.T) {
		bad := bytes.Clone(partValue)
		bad[len(bad)-1] ^= 1
		if err := db.Set(firstKV.Key, bad, pebble.NoSync); err != nil {
			t.Fatal(err)
		}
		_, _, err := read(t, false)
		if err == nil || !strings.Contains(err.Error(), "conflicting immutable archive key") {
			t.Fatalf("conflicting legacy row: got %v", err)
		}
	})
}
