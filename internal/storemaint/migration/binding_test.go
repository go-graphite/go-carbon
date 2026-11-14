package migration

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

const testUUID = "ac561b83-31d7-4e11-aada-87ff24d866b1"

type fixture struct {
	b                                 binding
	dir, source, store, archive, path string
}

func setup(t *testing.T, archive bool) fixture {
	t.Helper()
	dir, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	f := fixture{b: defaults(), dir: dir, source: filepath.Join(dir, "source"), store: filepath.Join(dir, "store"), path: filepath.Join(dir, "proof.json")}
	f.b.volume = func(string) (string, error) { return testUUID, nil }
	paths := []string{f.source, f.store}
	if archive {
		f.archive = filepath.Join(dir, "archive")
		paths = append(paths, f.archive)
	}
	for _, p := range paths {
		if err = os.Mkdir(p, 0700); err != nil {
			t.Fatal(err)
		}
	}
	for name, value := range map[string]string{"CHUNKSTORE": "go-carbon-chunks-v6\n", "LOCK": "", "000001.sst": "SST sentinel", "000002.log": "WAL sentinel"} {
		if err = os.WriteFile(filepath.Join(f.store, name), []byte(value), 0640); err != nil {
			t.Fatal(err)
		}
	}
	roots := map[string]root{}
	for name, path := range map[string]string{"source": f.source, "store": f.store, "archive": f.archive} {
		if path != "" {
			r, err := f.b.identity(path)
			if err != nil {
				t.Fatal(err)
			}
			roots[name] = r
		}
	}
	p := proof{Version: 1, Roots: roots}
	for name, value := range markerBytes(roots) {
		if err = os.WriteFile(markers(p)[name], value, 0640); err != nil {
			t.Fatal(err)
		}
	}
	return f
}
func (f fixture) record(t *testing.T) proof {
	t.Helper()
	p, err := f.b.record(f.path, f.source, f.store, f.archive)
	if err != nil {
		t.Fatal(err)
	}
	return p
}
func (f fixture) renumber(t *testing.T) proof {
	t.Helper()
	data, err := os.ReadFile(f.path)
	if err != nil {
		t.Fatal(err)
	}
	var p proof
	if err = json.Unmarshal(data, &p); err != nil {
		t.Fatal(err)
	}
	for k, v := range p.Roots {
		v.Device += 17
		p.Roots[k] = v
	}
	data, _ = json.Marshal(p)
	os.WriteFile(f.path, data, 0600)
	for k, v := range markerBytes(p.Roots) {
		os.WriteFile(markers(p)[k], v, 0640)
	}
	return p
}
func snap(t *testing.T, dir string) map[string]string {
	t.Helper()
	out := map[string]string{}
	err := filepath.WalkDir(dir, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.Type().IsRegular() {
			b, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			out[path] = string(b)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return out
}
func TestRecordAndRenumber(t *testing.T) {
	for _, archive := range []bool{false, true} {
		t.Run(fmt.Sprint(archive), func(t *testing.T) {
			f := setup(t, archive)
			f.record(t)
			before := snap(t, f.dir)
			r, err := f.b.rebind(f.path, false)
			if err != nil || r["changed"] != false {
				t.Fatal(r, err)
			}
			if !reflect.DeepEqual(before, snap(t, f.dir)) {
				t.Fatal("same-device check changed files")
			}
			if _, err = f.b.record(f.path, f.source, f.store, f.archive); err == nil {
				t.Fatal("replaced proof")
			}
			f.renumber(t)
			r, err = f.b.rebind(f.path, true)
			if err != nil || r["pending_changes"] != len(markers(proof{Roots: map[string]root{"source": {Path: f.source}, "store": {Path: f.store}}}))+map[bool]int{true: 1}[archive] {
				t.Fatal(r, err)
			}
			r, err = f.b.rebind(f.path, false)
			if err != nil || r["changed"] != true {
				t.Fatal(r, err)
			}
			for _, name := range []string{"000001.sst", "000002.log"} {
				p := filepath.Join(f.store, name)
				if string(mustRead(t, p)) != before[p] {
					t.Fatal("rewrote metric data")
				}
			}
			for _, p := range markers(f.recorded(t)) {
				info, err := os.Stat(p)
				if err != nil || info.Mode().Perm() != 0640 {
					t.Fatal("lost marker mode", err)
				}
			}
			if r, err = f.b.rebind(f.path, false); err != nil || r["changed"] != false {
				t.Fatal("repeat not idempotent", r, err)
			}
		})
	}
}
func mustRead(t *testing.T, path string) []byte {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return b
}
func (f fixture) recorded(t *testing.T) proof {
	t.Helper()
	var p proof
	if err := json.Unmarshal(mustRead(t, f.path), &p); err != nil {
		t.Fatal(err)
	}
	return p
}
func TestRejectedIdentityDoesNotMutate(t *testing.T) {
	for _, kind := range []string{"uuid", "inode", "foreign", "symlink", "archive"} {
		t.Run(kind, func(t *testing.T) {
			f := setup(t, false)
			f.record(t)
			p := f.renumber(t)
			switch kind {
			case "uuid":
				f.b.volume = func(string) (string, error) { return "foreign", nil }
			case "inode":
				r := p.Roots["source"]
				r.Inode++
				p.Roots["source"] = r
				b, _ := json.Marshal(p)
				os.WriteFile(f.path, b, 0600)
			case "foreign":
				os.WriteFile(filepath.Join(f.store, "INCREMENTAL-FILE-MIGRATION"), []byte("foreign"), 0640)
			case "symlink":
				os.Remove(filepath.Join(f.store, "INCREMENTAL-FILE-MIGRATION"))
				os.Symlink(filepath.Join(f.store, "000001.sst"), filepath.Join(f.store, "INCREMENTAL-FILE-MIGRATION"))
			case "archive":
				os.WriteFile(filepath.Join(f.store, "INCREMENTAL-SOURCE-ARCHIVE.json"), []byte("{}"), 0640)
			}
			before := snap(t, f.dir)
			if _, err := f.b.rebind(f.path, false); err == nil {
				t.Fatal("accepted", kind)
			}
			if !reflect.DeepEqual(before, snap(t, f.dir)) {
				t.Fatal("mutated rejected identity")
			}
		})
	}
}
func TestInterruptedRepairResumes(t *testing.T) {
	for failAt := 1; failAt <= 4; failAt++ {
		t.Run(fmt.Sprint(failAt), func(t *testing.T) {
			f := setup(t, true)
			f.record(t)
			f.renumber(t)
			calls := 0
			f.b.replace = func(path string, data []byte) error {
				calls++
				if calls == failAt {
					return errors.New("injected failure")
				}
				return atomic(path, data)
			}
			if _, err := f.b.rebind(f.path, false); err == nil {
				t.Fatal("expected interruption")
			}
			f.b.replace = atomic
			if _, err := f.b.rebind(f.path, false); err != nil {
				t.Fatal("resume failed", err)
			}
			r, err := f.b.rebind(f.path, false)
			if err != nil || r["changed"] != false {
				t.Fatal(r, err)
			}
		})
	}
}
func TestStableBindingsAreReadOnly(t *testing.T) {
	for _, format := range []int{6, 7, 9, 11, 12} {
		for _, kind := range []string{"token", "birth"} {
			t.Run(fmt.Sprint(format)+kind, func(t *testing.T) {
				f := setup(t, false)
				os.WriteFile(filepath.Join(f.store, "CHUNKSTORE"), []byte(fmt.Sprintf("go-carbon-chunks-v%d\n", format)), 0640)
				os.Remove(filepath.Join(f.source, ".go-carbon-incremental-source-root.keep"))
				id := "token:" + strings.Repeat("a", 32)
				if kind == "token" {
					os.WriteFile(filepath.Join(f.source, ".go-carbon-migration-source"), []byte(strings.Repeat("a", 32)+"\n"), 0640)
				} else {
					id = "birth:42:1790000000:000000001"
					f.b.birth = func(string, root) (string, error) { return id, nil }
				}
				os.WriteFile(filepath.Join(f.store, "INCREMENTAL-FILE-MIGRATION"), []byte("go-carbon-incremental-v2\n"+f.source+"\n"+id+"\n"), 0640)
				f.record(t)
				p := f.recorded(t)
				r := p.Roots["source"]
				r.Device += 17
				p.Roots["source"] = r
				data, _ := json.Marshal(p)
				os.WriteFile(f.path, data, 0600)
				before := snap(t, f.dir)
				got, err := f.b.rebind(f.path, false)
				if err != nil || got["source_identity"] != id || got["changed"] != false {
					t.Fatal(got, err)
				}
				if !reflect.DeepEqual(before, snap(t, f.dir)) {
					t.Fatal("rewrote stable bindings")
				}
			})
		}
	}
}
func TestRecordLockExcludesOtherProcess(t *testing.T) {
	if path := os.Getenv("MIGRATION_LOCK_CHILD"); path != "" {
		b := defaults()
		b.volume = func(string) (string, error) { return testUUID, nil }
		_, err := b.rebind(path, false)
		if err == nil {
			os.Exit(2)
		}
		os.Exit(0)
	}
	f := setup(t, false)
	f.record(t)
	f.renumber(t)
	before := snap(t, f.dir)
	lock, err := lockStore(f.store)
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Close()
	cmd := exec.Command(os.Args[0], "-test.run=^TestRecordLockExcludesOtherProcess$")
	cmd.Env = append(os.Environ(), "MIGRATION_LOCK_CHILD="+f.path)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatal("record lock bypass", err, string(out))
	}
	if !reflect.DeepEqual(before, snap(t, f.dir)) {
		t.Fatal("locked engine bindings changed")
	}
}
func TestBindingFileBounds(t *testing.T) {
	f := setup(t, false)
	path := filepath.Join(f.dir, "large")
	os.WriteFile(path, bytes.Repeat([]byte("x"), 16385), 0600)
	if _, err := readRegular(path); err == nil {
		t.Fatal("oversized file accepted")
	}
}

func TestStableIdentityMismatch(t *testing.T) {
	f := setup(t, false)
	os.WriteFile(filepath.Join(f.store, "CHUNKSTORE"), []byte("go-carbon-chunks-v12\n"), 0640)
	os.Remove(filepath.Join(f.source, ".go-carbon-incremental-source-root.keep"))
	f.b.birth = func(string, root) (string, error) { return "birth:42:1790000000:000000001", nil }
	os.WriteFile(filepath.Join(f.store, "INCREMENTAL-FILE-MIGRATION"), []byte("go-carbon-incremental-v2\n"+f.source+"\nbirth:42:1790000000:000000001\n"), 0640)
	f.record(t)
	f.b.birth = func(string, root) (string, error) { return "birth:42:1790000000:000000002", nil }
	before := snap(t, f.dir)
	if _, err := f.b.rebind(f.path, false); err == nil {
		t.Fatal("accepted reused inode with different birth nanoseconds")
	}
	if !reflect.DeepEqual(before, snap(t, f.dir)) {
		t.Fatal("mutated mismatched identity")
	}
}
