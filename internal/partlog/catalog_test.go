package partlog

import (
	"bytes"
	"encoding/binary"
	"errors"
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

// nameFixture is deterministic: binary keys, varied definitions, 2000 names.
func nameFixture() (keys []string, defs [][]byte) {
	for i := 0; i < 2000; i++ {
		keys = append(keys, string([]byte{'a', 0, byte(i % 31), byte(i), byte(i >> 8)}))
		defs = append(defs, []byte{0, 1, byte(i % 7), 255})
	}
	return keys, defs
}

// checkNames asserts IDs 1..n resolve to the fixture names and definitions.
func checkNames(t testing.TB, c *NameCatalog, keys []string, defs [][]byte) {
	t.Helper()
	for i, k := range keys {
		key, def, ok := c.Lookup(uint64(i + 1))
		if !ok || key != k || !bytes.Equal(def, defs[i]) {
			t.Fatal("id", i+1)
		}
	}
}

// Interning in chunks with checkpoints between them keeps every ID stable, and
// re-interning an existing name returns its ID.
func TestCatalogSegmentsExact(t *testing.T) {
	t.Parallel()
	c := newTestCatalog(t)
	keys, defs := nameFixture()
	for _, r := range [][2]int{{0, 700}, {700, 1400}, {1400, 2000}} {
		for i := r[0]; i < r[1]; i++ {
			id, err := c.Intern(keys[i], defs[i])
			if err != nil || id != uint64(i+1) {
				t.Fatal(i, id, err)
			}
		}
		if err := c.Checkpoint(); err != nil {
			t.Fatal(err)
		}
	}
	checkNames(t, c, keys, defs)
	if id, err := c.Intern(keys[5], defs[5]); err != nil || id != 6 {
		t.Fatal("re-intern", id, err)
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
		keys, defs := nameFixture()
		for i := range 50 {
			if _, e = c.Intern(keys[i], defs[i]); e != nil {
				panic(e)
			}
		}
		if e = c.Sync(); e != nil {
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
			keys, defs := nameFixture()
			if c.Count() != 50 {
				t.Fatal("count", c.Count())
			}
			checkNames(t, c, keys[:50], defs[:50])
			if id, e := c.Intern("after restart", []byte{0, 255}); e != nil || id != 51 {
				t.Fatal(id, e)
			}
			if e = c.Checkpoint(); e != nil {
				t.Fatal(e)
			}
		})
	}
}

// Arbitrary WAL bytes must open with an error or a consistent catalog, never panic.
func FuzzCatalogDecode(f *testing.F) {
	dir := f.TempDir()
	c, err := OpenNameCatalog(dir)
	if err != nil {
		f.Fatal(err)
	}
	_, _ = c.Intern("one", []byte{1})
	_ = c.Sync()
	_ = c.Close()
	seed, err := os.ReadFile(filepath.Join(dir, "catalog.wal"))
	if err != nil {
		f.Fatal(err)
	}
	f.Add(seed)
	f.Fuzz(func(t *testing.T, b []byte) {
		d := t.TempDir()
		if err := os.WriteFile(filepath.Join(d, "catalog.wal"), b, 0600); err != nil {
			t.Fatal(err)
		}
		if c, err := OpenNameCatalog(d); err == nil {
			_ = c.Close()
		}
	})
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
		dir := t.TempDir()
		c, err := OpenNameCatalog(dir)
		if err != nil {
			t.Fatal(err)
		}
		id, err := c.Intern(string(b), bytes.Clone(b))
		if err != nil {
			t.Fatal(err)
		}
		if err = c.Sync(); err != nil {
			t.Fatal(err)
		}
		_ = c.Close()
		c, err = OpenNameCatalog(dir)
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close()
		key, def, ok := c.Lookup(id)
		if !ok || key != string(b) || !bytes.Equal(def, b) {
			t.Fatal("roundtrip")
		}
	})
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
