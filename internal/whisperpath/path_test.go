package whisperpath

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"golang.org/x/sys/unix"
)

func TestPruneBoundaries(t *testing.T) {
	for _, tc := range []string{"empty", "occupied", "missing", "outside", "prefix", "symlink", "ancestor-symlink", "quarantine"} {
		t.Run(tc, func(t *testing.T) {
			base := t.TempDir()
			root := filepath.Join(base, "whisper")
			leaf := filepath.Join(root, "a", "b")
			if err := os.MkdirAll(leaf, 0700); err != nil {
				t.Fatal(err)
			}
			dir, wantError := leaf, false
			switch tc {
			case "occupied":
				for _, suffix := range []string{".wsp", ".wsp.ooo", ".wsp.lock"} {
					if err := os.WriteFile(filepath.Join(leaf, "keep"+suffix), nil, 0600); err != nil {
						t.Fatal(err)
					}
				}
			case "missing":
				if err := os.Remove(leaf); err != nil {
					t.Fatal(err)
				}
			case "outside":
				dir, wantError = base, true
			case "prefix":
				dir, wantError = root+"-other", true
			case "symlink", "ancestor-symlink":
				outside := filepath.Join(base, "outside")
				if err := os.MkdirAll(filepath.Join(outside, "empty"), 0700); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(outside, filepath.Join(root, "link")); err != nil {
					t.Fatal(err)
				}
				dir, wantError = filepath.Join(root, "link"), true
				if tc == "ancestor-symlink" {
					dir = filepath.Join(dir, "empty")
				}
			case "quarantine":
				dir = filepath.Join(root, Quarantine, "empty")
				if err := os.MkdirAll(dir, 0700); err != nil {
					t.Fatal(err)
				}
			}
			err := Prune(root, dir, nil)
			if (err != nil) != wantError {
				t.Fatalf("error=%v wantError=%t", err, wantError)
			}
			if _, err := os.Stat(root); err != nil {
				t.Fatalf("root removed: %v", err)
			}
			if tc == "empty" || tc == "missing" {
				if _, err := os.Stat(filepath.Dir(leaf)); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("ancestors remain: %v", err)
				}
			} else if tc == "quarantine" {
				if _, err := os.Stat(dir); err != nil {
					t.Fatalf("quarantine changed: %v", err)
				}
			} else if _, err := os.Stat(leaf); err != nil {
				t.Fatalf("unrelated tree changed: %v", err)
			}
			if tc == "symlink" || tc == "ancestor-symlink" {
				if _, err := os.Stat(filepath.Join(base, "outside", "empty")); err != nil {
					t.Fatal("followed symlink", err)
				}
			}
		})
	}
}

func TestCreateRetriesVanishedParentOnce(t *testing.T) {
	for _, name := range []string{"metric.wsp", "metric.wsp.ooo", "metric.wsp.lock", "buckyd-import.wsp"} {
		t.Run(name, func(t *testing.T) {
			root := t.TempDir()
			dir := filepath.Join(root, "a", "b")
			calls := 0
			f, err := Create(dir, 0700, func() (*os.File, error) {
				calls++
				if calls == 1 {
					if err := Prune(root, dir, nil); err != nil {
						t.Fatal(err)
					}
				}
				return os.OpenFile(filepath.Join(dir, name), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
			})
			if err != nil {
				t.Fatal(err)
			}
			f.Close()
			if calls != 2 {
				t.Fatalf("calls=%d", calls)
			}
			if err := Prune(root, dir, nil); err != nil {
				t.Fatal(err)
			}
			if _, err := os.Stat(filepath.Join(dir, name)); err != nil {
				t.Fatal("removed created file", err)
			}
		})
	}
	t.Run("bounded", func(t *testing.T) {
		calls := 0
		_, err := Create(t.TempDir(), 0700, func() (int, error) { calls++; return 0, os.ErrNotExist })
		if !errors.Is(err, os.ErrNotExist) || calls != 2 {
			t.Fatalf("calls=%d error=%v", calls, err)
		}
	})
}

func TestPruneConcurrentQuarantineMoveAndSymlinkSwap(t *testing.T) {
	for _, swap := range []bool{false, true} {
		t.Run(map[bool]string{false: "quarantine-move", true: "symlink-swap"}[swap], func(t *testing.T) {
			base := t.TempDir()
			root := filepath.Join(base, "whisper")
			leaf := filepath.Join(root, "a", "b")
			if err := os.MkdirAll(leaf, 0700); err != nil {
				t.Fatal(err)
			}
			outside := filepath.Join(base, "outside")
			if err := os.Mkdir(outside, 0700); err != nil {
				t.Fatal(err)
			}
			source := filepath.Join(root, "staged.wsp")
			if err := os.WriteFile(source, []byte("keep"), 0600); err != nil {
				t.Fatal(err)
			}
			calls := 0
			err := Prune(root, leaf, func() error {
				calls++
				if calls != 1 {
					return nil
				}
				if swap {
					if err := os.Rename(filepath.Join(root, "a"), filepath.Join(root, "parked")); err != nil {
						return err
					}
					return os.Symlink(outside, filepath.Join(root, "a"))
				}
				// Model trim publishing a source into its quarantine between a pruner's
				// directory read and rmdir. The nonempty destination blocks removal.
				quarantine := filepath.Join(leaf, Quarantine)
				if err := os.Mkdir(quarantine, 0700); err != nil {
					return err
				}
				return os.Rename(source, filepath.Join(quarantine, "keep.wsp"))
			})
			if err != nil && !(swap && errors.Is(err, unix.ENOTDIR)) {
				t.Fatal(err)
			}
			if !swap {
				if data, err := os.ReadFile(filepath.Join(leaf, Quarantine, "keep.wsp")); err != nil || string(data) != "keep" {
					t.Fatal("quarantine lost", err)
				}
			}
			if _, err := os.Stat(outside); err != nil {
				t.Fatal("outside removed", err)
			}
		})
	}
}

func TestPruneRefusesSymlinkRoot(t *testing.T) {
	base := t.TempDir()
	root := filepath.Join(base, "real")
	leaf := filepath.Join(root, "leaf")
	if err := os.MkdirAll(leaf, 0700); err != nil {
		t.Fatal(err)
	}
	alias := filepath.Join(base, "alias")
	if err := os.Symlink(root, alias); err != nil {
		t.Fatal(err)
	}
	if err := Prune(alias, filepath.Join(alias, "leaf"), nil); err == nil {
		t.Fatal("followed symlink root")
	}
	if _, err := os.Stat(leaf); err != nil {
		t.Fatal("modified symlink target", err)
	}
}

func TestPruneAtKeepsBorrowedRoot(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "a", "b")
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	fd, err := OpenDir(root)
	if err != nil {
		t.Fatal(err)
	}
	defer unix.Close(fd)
	if err := PruneAt(fd, root, dir, nil); err != nil {
		t.Fatal(err)
	}
	var st unix.Stat_t
	if err := unix.Fstat(fd, &st); err != nil {
		t.Fatal("borrowed root was closed", err)
	}
	if _, err := os.Stat(filepath.Join(root, "a")); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("ancestor remains", err)
	}
	if err := PruneAt(fd, root, filepath.Dir(root), nil); err == nil {
		t.Fatal("crossed borrowed data root")
	}
}
