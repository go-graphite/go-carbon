package partlog

import (
	"bytes"
	"encoding/binary"
	"errors"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
)

func newTestCatalog(t testing.TB) *NameCatalog {
	t.Helper()
	c, e := OpenNameCatalog(t.TempDir())
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}
func TestCatalogSegmentsExact(t *testing.T) {
	t.Parallel()
	c := newTestCatalog(t)
	fs := v3Fixture()
	for _, sparse := range []bool{false, true} {
		for _, chunk := range [][]DictionaryFrame{fs[:700], fs[700:1400], fs[1400:]} {
			e := CatalogEncoder{Catalog: c, SeriesClocks: true, SparseClocks: sparse}
			b, err := e.Encode(chunk)
			e.Close()
			if err != nil {
				t.Fatal(err)
			}
			if err = c.Checkpoint(); err != nil {
				t.Fatal(err)
			}
			d := CatalogDecoder{Catalog: c}
			got, err := d.Decode(b)
			d.Close()
			if err != nil {
				t.Fatal(err)
			}
			exactV3(t, chunk, got)
		}
	}
	other := newTestCatalog(t)
	e := CatalogEncoder{Catalog: c}
	b, err := e.Encode(fs[:2])
	e.Close()
	if err != nil {
		t.Fatal(err)
	}
	d := CatalogDecoder{Catalog: other}
	defer d.Close()
	if _, err = d.Decode(b); err == nil {
		t.Fatal("epoch mismatch accepted")
	}
}
func TestCatalogCodecCorrupt(t *testing.T) {
	t.Parallel()
	c := newTestCatalog(t)
	e := CatalogEncoder{Catalog: c, SeriesClocks: true, SparseClocks: true}
	defer e.Close()
	b, err := e.Encode(v3Fixture()[:3])
	if err != nil {
		t.Fatal(err)
	}
	for i := range b {
		d := CatalogDecoder{Catalog: c}
		_, err = d.Decode(b[:i])
		d.Close()
		if err == nil {
			t.Fatal("truncation", i)
		}
		bad := bytes.Clone(b)
		bad[i] ^= 1
		d = CatalogDecoder{Catalog: c}
		_, err = d.Decode(bad)
		d.Close()
		if err == nil {
			t.Fatal("corruption", i)
		}
	}
	next, err := e.Encode(v3Fixture()[3:6])
	if err != nil {
		t.Fatal(err)
	}
	d := CatalogDecoder{Catalog: c}
	defer d.Close()
	if _, err = d.Decode(next); err == nil {
		t.Fatal("missing prefix accepted")
	}
}
func TestCatalogWALTruncationAndCorrupt(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	c, e := OpenNameCatalog(dir)
	if e != nil {
		t.Fatal(e)
	}
	_, e = c.Intern("one", []byte{1})
	if e != nil {
		t.Fatal(e)
	}
	if e = c.Sync(); e != nil {
		t.Fatal(e)
	}
	_ = c.Close()
	original, e := os.ReadFile(filepath.Join(dir, "catalog.wal"))
	if e != nil {
		t.Fatal(e)
	}
	second := catalogRecord(2, "two", nil)
	for i := 0; i < len(second); i++ {
		b := append(bytes.Clone(original), second[:i]...)
		if e = os.WriteFile(filepath.Join(dir, "catalog.wal"), b, 0600); e != nil {
			t.Fatal(e)
		}
		c, e = OpenNameCatalog(dir)
		if e != nil {
			t.Fatal(i, e)
		}
		if c.Count() != 1 {
			t.Fatal("partial tail accepted")
		}
		_ = c.Close()
	}
	for i := range original {
		b := bytes.Clone(original)
		b[i] ^= 1
		if e = os.WriteFile(filepath.Join(dir, "catalog.wal"), b, 0600); e != nil {
			t.Fatal(e)
		}
		c, e = OpenNameCatalog(dir)
		if e == nil {
			_ = c.Close()
			t.Fatal("corrupt catalog accepted", i)
		}
	}
}
func TestCatalogCheckpointSIGKILL(t *testing.T) {
	t.Parallel()
	if dir := os.Getenv("CATALOG_KILL_DIR"); dir != "" {
		c, e := OpenNameCatalog(dir)
		if e != nil {
			panic(e)
		}
		fs := v3Fixture()[:50]
		enc := CatalogEncoder{Catalog: c, SeriesClocks: true, SparseClocks: true}
		b, e := enc.Encode(fs)
		if e != nil {
			panic(e)
		}
		if e = c.Sync(); e != nil {
			panic(e)
		}
		f, e := os.Create(filepath.Join(dir, "segment"))
		if e != nil {
			panic(e)
		}
		if e = writeCatalogBytes(f, b); e != nil {
			panic(e)
		}
		if e = f.Sync(); e != nil {
			panic(e)
		}
		_ = f.Close()
		if e = syncCatalogDir(dir); e != nil {
			panic(e)
		}
		stage := os.Getenv("CATALOG_KILL_STAGE")
		if stage == "append-partial" {
			record := catalogRecord(c.Count()+1, "unacknowledged", []byte{1, 2, 3})
			if _, e = c.wal.Write(record[:len(record)/2]); e != nil {
				panic(e)
			}
			_ = syscall.Kill(os.Getpid(), syscall.SIGKILL)
			panic("kill returned")
		}
		if stage == "append" {
			_ = syscall.Kill(os.Getpid(), syscall.SIGKILL)
			panic("kill returned")
		}
		c.checkpointHook = func(s string) {
			if s == stage {
				_ = syscall.Kill(os.Getpid(), syscall.SIGKILL)
				panic("kill returned")
			}
		}
		if e = c.Checkpoint(); e != nil {
			panic(e)
		}
		panic("checkpoint hook not reached")
	}
	for _, stage := range []string{"append-partial", "append", "written", "synced", "renamed", "published", "truncated"} {
		t.Run(stage, func(t *testing.T) {
			dir := t.TempDir()
			cmd := exec.Command(os.Args[0], "-test.run=^TestCatalogCheckpointSIGKILL$")
			cmd.Env = append(os.Environ(), "CATALOG_KILL_DIR="+dir, "CATALOG_KILL_STAGE="+stage)
			out, e := cmd.CombinedOutput()
			exit := &exec.ExitError{}
			ok := errors.As(e, &exit)
			if !ok {
				t.Fatalf("expected SIGKILL: %v %s", e, out)
			}
			status, ok := exit.Sys().(syscall.WaitStatus)
			if !ok || !status.Signaled() || status.Signal() != syscall.SIGKILL {
				t.Fatalf("not SIGKILL: %v %s", e, out)
			}
			c, e := OpenNameCatalog(dir)
			if e != nil {
				t.Fatal(e)
			}
			defer c.Close()
			b, e := os.ReadFile(filepath.Join(dir, "segment"))
			if e != nil {
				t.Fatal(e)
			}
			d := CatalogDecoder{Catalog: c}
			defer d.Close()
			got, e := d.Decode(b)
			if e != nil {
				t.Fatal(e)
			}
			exactV3(t, v3Fixture()[:50], got)
			if _, e = c.Intern("after restart", []byte{0, 255}); e != nil {
				t.Fatal(e)
			}
			if e = c.Checkpoint(); e != nil {
				t.Fatal(e)
			}
		})
	}
}
func FuzzCatalogDecode(f *testing.F) {
	c := newTestCatalog(f)
	e := CatalogEncoder{Catalog: c, SeriesClocks: true, SparseClocks: true}
	b, err := e.Encode(v3Fixture()[:2])
	e.Close()
	if err != nil {
		f.Fatal(err)
	}
	f.Add(b)
	f.Fuzz(func(t *testing.T, b []byte) { d := CatalogDecoder{Catalog: c}; defer d.Close(); _, _ = d.Decode(b) })
}

func TestCatalogCheckpointReopen(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	c, e := OpenNameCatalog(dir)
	if e != nil {
		t.Fatal(e)
	}
	for _, name := range []string{"a", "b\x00", "c"} {
		if _, e = c.Intern(name, []byte{0, 255}); e != nil {
			t.Fatal(e)
		}
	}
	if e = c.Checkpoint(); e != nil {
		t.Fatal(e)
	}
	if _, e = c.Intern("suffix", nil); e != nil {
		t.Fatal(e)
	}
	if e = c.Sync(); e != nil {
		t.Fatal(e)
	}
	epoch := c.Epoch()
	_ = c.Close()
	cp, e := os.ReadFile(filepath.Join(dir, "catalog.checkpoint"))
	if e != nil {
		t.Fatal(e)
	}
	for i := range cp {
		bad := bytes.Clone(cp)
		bad[i] ^= 1
		if e = os.WriteFile(filepath.Join(dir, "catalog.checkpoint"), bad, 0600); e != nil {
			t.Fatal(e)
		}
		got, e := OpenNameCatalog(dir)
		if e == nil {
			_ = got.Close()
			t.Fatal("corrupt checkpoint accepted", i)
		}
	}
	if e = os.WriteFile(filepath.Join(dir, "catalog.checkpoint"), cp, 0600); e != nil {
		t.Fatal(e)
	}
	c, e = OpenNameCatalog(dir)
	if e != nil {
		t.Fatal(e)
	}
	defer c.Close()
	if c.Count() != 4 || c.Epoch() != epoch {
		t.Fatal("checkpoint suffix lost")
	}
	if _, e = c.Intern("a", nil); e == nil {
		t.Fatal("conflicting definition accepted")
	}
}
func FuzzCatalogRoundtrip(f *testing.F) {
	f.Add([]byte{0, 1, 255, 4, 8, 64, 7, 9})
	f.Fuzz(func(t *testing.T, b []byte) {
		if len(b) == 0 || len(b) > 512 {
			return
		}
		c := newTestCatalog(t)
		fs := v3Fixture()[:1]
		fs[0].Key = string(b)
		fs[0].Definition = bytes.Clone(b)
		for i := range fs[0].Points {
			var bits uint64
			for j := range 8 {
				bits = bits<<8 | uint64(b[(i+j)%len(b)])
			}
			fs[0].Points[i].Timestamp = int64(bits)
			fs[0].Points[i].Value = math.Float64frombits(bits)
			fs[0].Clock = int64(bits)
		}
		e := CatalogEncoder{Catalog: c, SeriesClocks: true, SparseClocks: true}
		record, err := e.Encode(fs)
		e.Close()
		if err != nil {
			t.Fatal(err)
		}
		d := CatalogDecoder{Catalog: c}
		got, err := d.Decode(record)
		d.Close()
		if err != nil {
			t.Fatal(err)
		}
		exactV3(t, fs, got)
	})
}

func TestCatalogFieldAccounting(t *testing.T) {
	t.Parallel()
	for _, sparse := range []bool{false, true} {
		c := newTestCatalog(t)
		var a PredictorV3Accounting
		e := CatalogEncoder{Catalog: c, SeriesClocks: true, SparseClocks: sparse, Accounting: &a}
		b, err := e.Encode(v3Fixture())
		e.Close()
		if err != nil {
			t.Fatal(err)
		}
		raw := int64(binary.LittleEndian.Uint32(b[17:]))
		sum := a.Names + a.Definitions + a.IDs + a.Clocks + a.Framing + a.Timestamps + a.Values
		if sum != raw || a.Names != 0 || a.Definitions != 0 || a.Headers != 73 || a.Encoded != int64(len(b)+32) {
			t.Fatal("accounting", sum, raw, a)
		}
	}
}

// Exercise recovery across compressed blocks without materialising decoded
// checkpoint bytes. Binary keys and metadata survive checkpoint + suffix replay.
func TestCatalogStreamingCheckpointSuffix(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	c, e := OpenNameCatalog(dir)
	if e != nil {
		t.Fatal(e)
	}
	const count = 8192
	definition := bytes.Repeat([]byte{0, 255, 17}, 100)
	for i := 0; i < count; i++ {
		key := string(binary.LittleEndian.AppendUint64([]byte("binary\x00"), uint64(i)))
		if _, e = c.Intern(key, definition); e != nil {
			t.Fatal(e)
		}
	}
	if e = c.Checkpoint(); e != nil {
		t.Fatal(e)
	}
	if _, e = c.Intern("suffix", []byte{255}); e != nil {
		t.Fatal(e)
	}
	if e = c.Sync(); e != nil {
		t.Fatal(e)
	}
	if e = c.Close(); e != nil {
		t.Fatal(e)
	}
	c, e = OpenNameCatalog(dir)
	if e != nil {
		t.Fatal(e)
	}
	defer c.Close()
	if c.Count() != count+1 {
		t.Fatal(c.Count())
	}
	for i := 0; i < count; i++ {
		key, def, ok := c.Lookup(uint64(i + 1))
		want := string(binary.LittleEndian.AppendUint64([]byte("binary\x00"), uint64(i)))
		if !ok || key != want || !bytes.Equal(def, definition) {
			t.Fatal("entry", i)
		}
	}
	if key, def, ok := c.Lookup(count + 1); !ok || key != "suffix" || !bytes.Equal(def, []byte{255}) {
		t.Fatal("suffix")
	}
}
