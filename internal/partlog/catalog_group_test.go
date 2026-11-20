package partlog

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/klauspost/compress/zstd"
)

func TestCatalogGroupedMixedRecovery(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	c, e := OpenNameCatalog(dir)
	if e != nil {
		t.Fatal(e)
	}
	// Preserve a legacy prefix, then interleave two sorted groups whose IDs are
	// deliberately in reverse lexical order and contain binary metadata.
	if _, e = c.buffer.Write(catalogRecord(1, "legacy", []byte{0, 255})); e != nil {
		t.Fatal(e)
	}
	c.ids.insert(NameHash("legacy"), 1)
	c.entries = append(c.entries, catalogEntry{key: "legacy", definition: []byte{0, 255}})
	for group := range 2 {
		for i := 999; i >= 0; i-- {
			if _, e = c.Intern(fmt.Sprintf("series.%d.%04d\x00", group, i), []byte{0, byte(i), 255}); e != nil {
				t.Fatal(e)
			}
		}
		if e = c.Sync(); e != nil {
			t.Fatal(e)
		}
	}
	raw, e := os.ReadFile(filepath.Join(dir, "catalog.wal"))
	if e != nil {
		t.Fatal(e)
	}
	if len(raw) > 14000 {
		t.Fatal("group compression regressed", len(raw))
	}
	_ = c.Close()
	c, e = OpenNameCatalog(dir)
	if e != nil {
		t.Fatal(e)
	}
	defer c.Close()
	if c.Count() != 2001 {
		t.Fatal(c.Count())
	}
	for id := uint64(2); id <= c.Count(); id++ {
		key, def, ok := c.Lookup(id)
		i := 999 - int((id-2)%1000)
		group := int((id - 2) / 1000)
		if !ok || key != fmt.Sprintf("series.%d.%04d\x00", group, i) || !bytes.Equal(def, []byte{0, byte(i), 255}) {
			t.Fatal(id, key, def)
		}
	}
	if e = c.Checkpoint(); e != nil {
		t.Fatal(e)
	}
	if _, e = c.Intern("after checkpoint", nil); e != nil {
		t.Fatal(e)
	}
	if e = c.Sync(); e != nil {
		t.Fatal(e)
	}
}

func TestCatalogGroupedTornAndCorrupt(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	c, e := OpenNameCatalog(dir)
	if e != nil {
		t.Fatal(e)
	}
	for _, k := range []string{"z", "a\x00", "m"} {
		if _, e = c.Intern(k, []byte{0, 255}); e != nil {
			t.Fatal(e)
		}
	}
	if e = c.Sync(); e != nil {
		t.Fatal(e)
	}
	_ = c.Close()
	path := filepath.Join(dir, "catalog.wal")
	original, e := os.ReadFile(path)
	if e != nil {
		t.Fatal(e)
	}
	for i := 28; i < len(original); i++ {
		if e = os.WriteFile(path, original[:i], 0600); e != nil {
			t.Fatal(e)
		}
		c, e = OpenNameCatalog(dir)
		if e != nil {
			t.Fatal(i, e)
		}
		if c.Count() != 0 {
			t.Fatal("partial group published", i)
		}
		_ = c.Close()
	}
	for i := 28; i < len(original); i++ {
		b := bytes.Clone(original)
		b[i] ^= 1
		if e = os.WriteFile(path, b, 0600); e != nil {
			t.Fatal(e)
		}
		c, e = OpenNameCatalog(dir)
		if e == nil {
			_ = c.Close()
			t.Fatal("corruption accepted", i)
		}
	}
}

func TestCatalogGroupMemoryBound(t *testing.T) {
	t.Parallel()
	c := newTestCatalog(t)
	for i := range 10000 {
		if _, e := c.Intern(fmt.Sprintf("%08d.%s", i, bytes.Repeat([]byte{'x'}, 512)), nil); e != nil {
			t.Fatal(e)
		}
		if c.pendingBytes > catalogGroupLimit {
			t.Fatal(c.pendingBytes)
		}
	}
	if e := c.Sync(); e != nil {
		t.Fatal(e)
	}
	if c.pendingBytes != 0 || len(c.pending) != 0 {
		t.Fatal("pending group not cleared")
	}
	dir := c.dir
	if e := c.Close(); e != nil {
		t.Fatal(e)
	}
	recovered, e := OpenNameCatalog(dir)
	if e != nil {
		t.Fatal(e)
	}
	defer recovered.Close()
	if recovered.Count() != 10000 {
		t.Fatal(recovered.Count())
	}
}

func TestCatalogGroupRejectsPrefixAmplification(t *testing.T) {
	t.Parallel()
	c := newTestCatalog(t)
	raw := binary.AppendUvarint(nil, 100)
	key := bytes.Repeat([]byte{'x'}, 100000)
	for i := range 100 {
		raw = binary.AppendUvarint(raw, uint64(i+1))
		if i == 0 {
			raw = binary.AppendUvarint(raw, 0)
			raw = binary.AppendUvarint(raw, uint64(len(key)))
			raw = append(raw, key...)
		} else {
			raw = binary.AppendUvarint(raw, uint64(len(key)))
			raw = binary.AppendUvarint(raw, 1)
			raw = append(raw, byte(i))
		}
		raw = binary.AppendUvarint(raw, 0)
	}
	z, e := zstd.NewWriter(nil, zstd.WithEncoderConcurrency(1))
	if e != nil {
		t.Fatal(e)
	}
	defer z.Close()
	body := binary.LittleEndian.AppendUint32(nil, uint32(len(raw)))
	body = z.EncodeAll(raw, body)
	if e = c.readGroup(body); e == nil {
		t.Fatal("prefix amplification accepted")
	}
	if c.Count() != 0 {
		t.Fatal("failed group published")
	}
}
