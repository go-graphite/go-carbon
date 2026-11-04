package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/cockroachdb/pebble/objstorage/objstorageprovider"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/go-graphite/go-carbon/internal/chunkstore"
)

func TestOrdinaryReasonsAcrossSample(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "sst")
	if err := os.Mkdir(dir, 0700); err != nil {
		t.Fatal(err)
	}
	metric := make([]byte, 16)
	metric[15] = 7
	key := func(kind byte, block, group uint32, bucket bool) []byte {
		k := append(append([]byte{'u'}, metric...), fixtureKey(kind, int(block))...)
		if kind == 'c' {
			binary.BigEndian.PutUint32(k[len(k)-4:], group)
		}
		if bucket {
			k = append(binary.BigEndian.AppendUint32([]byte{'t'}, 9), k...)
		}
		return k
	}
	metadata, groups := map[string][]byte{}, map[string][]byte{}
	// Manifest for 60-second, 35-day retention. Descriptor and tail data are in
	// different SSTs so a same-file-only join cannot pass this test.
	manifest := []byte{2, 1}
	for _, n := range []uint64{100000, 100, 1, 100, 60, 50400, 99900, 1, 0} {
		manifest = binary.AppendUvarint(manifest, n)
	}
	manifestKey := append(append([]byte{'u'}, metric...), fixtureKey('b', 0)[:9]...)
	manifestKey = append(manifestKey, 'h')
	metadata[string(manifestKey)] = manifest
	for i, count := range []uint32{1024, 64, 65537, 129, 256, 256, 0} {
		d := binary.BigEndian.AppendUint32(nil, count)
		if i != 0 {
			d = binary.BigEndian.AppendUint32(d, 9)
		}
		if i == 4 {
			d = binary.BigEndian.AppendUint32(d, 4096)
		}
		if i != 5 {
			metadata[string(key('b', uint32(i), 0, false))] = d
		}
		// Group zero plus the descriptor tail where applicable.
		for _, g := range []uint32{0, (max(count, 1) - 1) / 128} {
			var b chunkstore.PointBlock
			b.Present[0] = 1
			b.Points[0] = chunkstore.Point{Timestamp: 1700000000 + int64(i)*1000, Value: float64(i)}
			if count > 0 {
				slot := (count - 1) % 128
				b.Present[slot/64] |= 1 << uint(slot%64)
				b.Points[slot] = chunkstore.Point{Timestamp: 1700000001 + int64(i)*1000, Value: float64(i)}
			}
			groups[string(key('c', uint32(i), g, i != 0))] = chunkstore.EncodePointBlock(&b)
			if i == 4 {
				break
			} // Packed descriptor with an older ordinary image is explicit.
		}
	}
	// Exclude malformed mixed tail from newest lookup while retaining its bytes.
	metadata[string(key('b', 4, 0, false))] = binary.BigEndian.AppendUint32(binary.BigEndian.AppendUint32(binary.BigEndian.AppendUint32(nil, 256), 8), 4096)
	write := func(name string, values map[string][]byte) {
		f, err := vfs.Default.Create(filepath.Join(dir, name))
		if err != nil {
			t.Fatal(err)
		}
		w := sstable.NewWriter(objstorageprovider.NewFileWritable(f), sstable.WriterOptions{})
		keys := make([]string, 0, len(values))
		for k := range values {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			if err := w.Set([]byte(k), values[k]); err != nil {
				t.Fatal(err)
			}
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
	}
	write("000001.sst", metadata)
	write("000002.sst", groups)
	output := filepath.Join(root, "inventory.json")
	if err := run([]string{"inventory", "-sst-dir", dir, "-out", output, "-read-mb-s", "0"}); err != nil {
		t.Fatal(err)
	}
	r := loadReport(t, output)
	for _, reason := range []string{"non_bucketed/pre_relayout", "count_le_128", "count_gt_max", "cold_eligible_shape/benefit_or_backlog_unknown", "descriptor_layout_or_count_mismatch", "descriptor_missing"} {
		if r.OrdinaryByReason[reason] == nil {
			t.Fatalf("missing reason %s", reason)
		}
	}
	var sum tally
	var newest, older bool
	for _, row := range r.Ordinary {
		addTally(&sum, &row.tally)
		if row.Step != 60 || row.RetentionSeconds != 3024000 {
			t.Fatalf("schema not joined: %+v", row)
		}
		newest = newest || row.Position == "newest-observed/open-candidate"
		older = older || row.Position == "older-than-observed-newest"
	}
	ordinary := r.Classes["ordinary"]
	if !newest || !older || sum.Entries != ordinary.Entries || sum.LogicalBytes != ordinary.LogicalBytes || math.Abs(sum.PhysicalBytes-ordinary.PhysicalBytes) > 1e-6 {
		t.Fatalf("ordinary reconciliation failed: %+v %+v newest=%t older=%t", sum, ordinary, newest, older)
	}
	// The data sample contains no descriptors. Resolve them from all store SSTs,
	// while physical accounting remains restricted to the selected data file.
	sampleDir := filepath.Join(root, "sample")
	if err := os.Mkdir(sampleDir, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.Link(filepath.Join(dir, "000002.sst"), filepath.Join(sampleDir, "000002.sst")); err != nil {
		t.Fatal(err)
	}
	for _, args := range [][]string{
		{"inventory", "-sst-dir", sampleDir, "-metadata-dir", dir, "-out", filepath.Join(dir, "forbidden.json")},
		{"transcode", "-sst-dir", sampleDir, "-metadata-dir", dir, "-tmp", filepath.Join(dir, "forbidden-tmp"), "-out", filepath.Join(root, "forbidden.json")},
	} {
		if err := run(args); err == nil {
			t.Fatal("accepted output in metadata source")
		}
	}
	sampleOut := filepath.Join(root, "sample.json")
	if err := run([]string{"inventory", "-sst-dir", sampleDir, "-metadata-dir", dir, "-out", sampleOut, "-read-mb-s", "0"}); err != nil {
		t.Fatal(err)
	}
	joined := loadReport(t, sampleOut)
	if len(joined.Files) != 1 || joined.Files[0].Name != "000002.sst" || joined.Classes["descriptor"] != nil {
		t.Fatal("metadata files entered sample accounting")
	}
	if len(joined.Ordinary) != len(r.Ordinary) {
		t.Fatal("whole-store metadata differs from full sample")
	}
	for key, row := range r.Ordinary {
		other := joined.Ordinary[key]
		if other == nil || other.Entries != row.Entries || other.LogicalBytes != row.LogicalBytes || math.Abs(other.PhysicalBytes-row.PhysicalBytes) > 1e-6 {
			t.Fatalf("unsampled descriptors not resolved: %s", key)
		}
	}
	// -files limits data accounting only; the default metadata directory still
	// includes an unsampled metadata file.
	if err := os.Link(filepath.Join(dir, "000001.sst"), filepath.Join(sampleDir, "000003.sst")); err != nil {
		t.Fatal(err)
	}
	defaultOut := filepath.Join(root, "default-metadata.json")
	if err := run([]string{"inventory", "-sst-dir", sampleDir, "-files", "1", "-out", defaultOut, "-read-mb-s", "0"}); err != nil {
		t.Fatal(err)
	}
	defaultJoin := loadReport(t, defaultOut)
	if len(defaultJoin.Files) != 1 || len(defaultJoin.Ordinary) != len(joined.Ordinary) {
		t.Fatal("-files truncated metadata scope")
	}
	// Exercise reader eviction with more store files than the open-reader cap.
	paths := []string{}
	for i := range 35 {
		name := fmt.Sprintf("extra-%03d.sst", i)
		write(name, map[string][]byte{})
		paths = append(paths, filepath.Join(dir, name))
	}
	x, err := (&runner{ctx: context.Background()}).openOrdinaryIndex([]string{filepath.Join(dir, "000002.sst")})
	if err != nil {
		t.Fatal(err)
	}
	defer x.close()
	x.runner = &runner{ctx: context.Background()}
	x.metadataPaths = paths
	if _, err := x.metric(metric, 1); err != nil {
		t.Fatal(err)
	}
	if len(x.metadataReaders) != 32 {
		t.Fatalf("reader cap: %d", len(x.metadataReaders))
	}
	// No metadata in the selected SST: retain archive/layout with unknown schema.
	scan := runner{ctx: context.Background(), cfg: config{mode: "inventory"}, levels: map[string]int{}}
	got, err := scan.scan(filepath.Join(dir, "000002.sst"))
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range got.Ordinary {
		if row.Step != 0 || row.Descriptor != "missing" {
			t.Fatalf("invented metadata: %+v", row)
		}
	}
	t.Logf("sample-only missing descriptors: %d entries; store-wide join missing descriptors: %d entries", got.Classes["ordinary"].Entries, joined.OrdinaryByReason["descriptor_missing"].Entries)
	// Source files remain byte identical on a repeated read.
	before, err := os.ReadFile(filepath.Join(dir, "000001.sst"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = scan.scan(filepath.Join(dir, "000001.sst")); err != nil {
		t.Fatal(err)
	}
	after, err := os.ReadFile(filepath.Join(dir, "000001.sst"))
	if err != nil || !bytes.Equal(before, after) {
		t.Fatal("source mutated", err)
	}
}

func TestV10PrefixClassification(t *testing.T) {
	points := make([]chunkstore.Point, 4096)
	for i := range points {
		points[i] = chunkstore.Point{Timestamp: 1700000000 + int64(i), Value: float64(i)}
	}
	value, err := chunkstore.EncodeSealedPoints(points, 1)
	if err != nil {
		t.Fatal(err)
	}
	key := append(append([]byte{'u'}, make([]byte, 16)...), fixtureKey('c', 0)...)
	payloadKey := append(bytes.Clone(key), make([]byte, 17)...)
	class, n, err := classify(payloadKey, append([]byte{4}, value...))
	if err != nil || n != 4096 || !strings.HasPrefix(class, "sealed/prefix/") {
		t.Fatalf("v10 payload: %s %d %v", class, n, err)
	}
	class, n, err = classify(key, append([]byte{5}, make([]byte, 20)...))
	if err != nil || n != 0 || class != "sealed/prefix-reference" {
		t.Fatalf("v10 reference: %s %d %v", class, n, err)
	}
}
