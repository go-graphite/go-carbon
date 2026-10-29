package chunkstore

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/vfs"
)

func TestCheckpointRefusesUnownedFileImports(t *testing.T) {
	s := openFileMigrationStore(t, vfs.NewMem())
	defer s.Close()
	if _, err := s.StageFileSnapshot(context.Background(), fileMigrationSnapshot()); err != nil {
		t.Fatal(err)
	}
	if err := s.Checkpoint("/copy"); err == nil {
		t.Fatal("checkpoint accepted a hidden file import")
	}
}

func TestCheckpointHardLinksAndIncludesWAL(t *testing.T) {
	for _, separate := range []bool{false, true} {
		t.Run(map[bool]string{false: "local-wal", true: "separate-wal"}[separate], func(t *testing.T) {
			root := t.TempDir()
			opts := Options{ExactBlocks: true, SyncInterval: time.Hour, CacheSize: 1 << 20, MemTableSize: 1 << 20}
			if separate {
				opts.WALDir = filepath.Join(root, "wal")
				if err := os.Mkdir(opts.WALDir, 0700); err != nil {
					t.Fatal(err)
				}
			}
			s, err := Open(filepath.Join(root, "live"), opts)
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			createTestMetric(t, s, "flushed")
			if err := s.Flush(); err != nil {
				t.Fatal(err)
			}
			createTestMetric(t, s, "in.wal")
			dest := filepath.Join(root, "checkpoint")
			if err := s.Checkpoint(dest); err != nil {
				t.Fatal(err)
			}
			createTestMetric(t, s, "after")
			entries, _ := os.ReadDir(dest)
			linked := false
			for _, entry := range entries {
				if strings.HasSuffix(entry.Name(), ".sst") {
					a, _ := os.Stat(filepath.Join(root, "live", entry.Name()))
					b, _ := entry.Info()
					if a != nil && os.SameFile(a, b) {
						linked = true
					}
				}
			}
			if !linked {
				t.Fatal("checkpoint did not hard-link tables")
			}
			opts.WALDir = ""
			clone, err := Open(dest, opts)
			if err != nil {
				t.Fatal(err)
			}
			defer clone.Close()
			for _, name := range []string{"flushed", "in.wal"} {
				if _, err := clone.Metadata(context.Background(), name); err != nil {
					t.Fatal(name, err)
				}
			}
			if _, err := clone.Metadata(context.Background(), "after"); !errors.Is(err, ErrNotFound) {
				t.Fatal("checkpoint includes later write", err)
			}
		})
	}
}
