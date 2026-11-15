package chunkstore_test

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io/fs"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/internal/whisperio"
)

type diskState struct {
	Size     int64
	Modified time.Time
	Hash     [32]byte
}

func diskTree(t *testing.T, root string) map[string]diskState {
	t.Helper()
	result := map[string]diskState{}
	err := filepath.WalkDir(root, func(path string, e fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		st, err := e.Info()
		if err != nil {
			return err
		}
		r := diskState{Size: st.Size(), Modified: st.ModTime()}
		if !e.IsDir() {
			b, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			r.Hash = sha256.Sum256(b)
		}
		result[path] = r
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return result
}
func TestReadOnlyZeroWrites(t *testing.T) {
	now := time.Unix(1794700800, 0)
	ctx := context.Background()
	for _, format := range []int{1, 2, 4, 6, 7, 9, 11, 12} {
		t.Run(fmt.Sprint(format), func(t *testing.T) {
			dir := filepath.Join(t.TempDir(), "store")
			opts := chunkstore.Options{Now: func() time.Time { return now }, ExactBlocks: format >= 4}
			if format >= 6 {
				opts.Continuation = &whisperio.ContinuationCodec{ReuseBlockKeys: true, DirectLateWrites: true}
				opts.TimeBuckets = format >= 7
			}
			s, err := chunkstore.Open(dir, opts)
			if err != nil {
				t.Fatal(err)
			}
			config := chunkstore.MetricConfig{Name: "history.one", Retentions: []chunkstore.Retention{{Step: 1, Count: 3600}, {Step: 60, Count: 43200}}, AggregationMethod: chunkstore.Average, XFilesFactor: .5}
			if _, err = s.Create(ctx, config); err != nil {
				t.Fatal(err)
			}
			input := []chunkstore.Point{{Timestamp: now.Unix() - 120, Value: math.Copysign(0, -1)}, {Timestamp: now.Unix() - 119, Value: math.Float64frombits(0x7ff8000000000042)}, {Timestamp: now.Unix() - 61, Value: 42}}
			if err = s.UpdateMany(ctx, config.Name, input); err != nil {
				t.Fatal(err)
			}
			want, err := s.Fetch(ctx, config.Name, int(now.Unix()-180), int(now.Unix()))
			if err != nil {
				t.Fatal(err)
			}
			if err = s.Close(); err != nil {
				t.Fatal(err)
			}
			// All newer encodings retain support for older records. Exercise read-only
			// acceptance without allowing marker promotion (including v6 + TimeBuckets).
			if format != 2 && format != 4 && format != 6 && format != 7 {
				if err = os.WriteFile(filepath.Join(dir, "CHUNKSTORE"), []byte(fmt.Sprintf("go-carbon-chunks-v%d\n", format)), 0600); err != nil {
					t.Fatal(err)
				}
			}
			before := diskTree(t, dir)
			opts.ReadOnly = true
			opts.TimeBuckets = format >= 6
			opts.SyncInterval = time.Nanosecond
			opts.MetadataCacheSize = 1 << 20
			opts.MetadataCacheCheckpoint = true
			opts.CompactOnce = "must-not-run"
			opts.RelayoutRate = 1 << 30
			opts.ParallelTableSyncs = 4
			opts.WALDir = filepath.Join(t.TempDir(), "do-not-create")
			opts.MaintenanceWait = func(context.Context, string) error { t.Error("read-only maintenance started"); return context.Canceled }
			s, err = chunkstore.Open(dir, opts)
			if err != nil {
				t.Fatal(err)
			}
			if !s.ReadOnly() {
				t.Fatal("read-only flag missing")
			}
			for range 3 {
				found, e := s.ListPage(ctx, "history.", "", 10)
				if e != nil || len(found) != 1 {
					t.Fatalf("find %v %v", found, e)
				}
				got, e := s.Fetch(ctx, config.Name, int(now.Unix()-180), int(now.Unix()))
				if e != nil {
					t.Fatal(e)
				}
				if got.Step != want.Step || got.FromTime != want.FromTime || got.UntilTime != want.UntilTime || len(got.Values) != len(want.Values) {
					t.Fatal("render shape changed")
				}
				for i, v := range got.Values {
					if math.Float64bits(v) != math.Float64bits(want.Values[i]) {
						t.Fatalf("render bits changed at %d", i)
					}
				}
			}
			for name, call := range map[string]func() error{"update": func() error { return s.UpdateMany(ctx, config.Name, input) }, "create": func() error { _, e := s.Create(ctx, config); return e }, "delete": func() error { return s.Delete(ctx, config.Name) }, "flush": s.Flush, "sync": s.Sync, "compact": s.Compact} {
				if e := call(); !errors.Is(e, chunkstore.ErrReadOnly) {
					t.Fatalf("%s error: %v", name, e)
				}
			}
			st := s.Stats()
			if st.Flushes != 0 || st.Compactions != 0 || st.FlushWriteBytes != 0 || st.CompactionWriteBytes != 0 {
				t.Fatalf("Pebble writes: %+v", st)
			}
			for kind := range chunkstore.FSKindNames {
				for _, op := range []int{chunkstore.FSWrite, chunkstore.FSSync, chunkstore.FSCreate, chunkstore.FSRemove} {
					if st.FS.Ops[op][kind] != 0 || st.FS.Bytes[op][kind] != 0 {
						t.Fatalf("filesystem mutation %s/%s: %+v", chunkstore.FSOpNames[op], chunkstore.FSKindNames[kind], st.FS)
					}
				}
			}
			if err = s.Close(); err != nil {
				t.Fatal(err)
			}
			if after := diskTree(t, dir); !reflect.DeepEqual(before, after) {
				for p, b := range before {
					if after[p] != b {
						t.Errorf("changed %s: %+v -> %+v", p, b, after[p])
					}
				}
				t.Fatal("read-only open/read/close changed directory")
			}
		})
	}
}
func TestReadOnlyMissingStoreDoesNotCreate(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "missing")
	before := diskTree(t, root)
	s, err := chunkstore.Open(dir, chunkstore.Options{ReadOnly: true})
	if err == nil {
		s.Close()
		t.Fatal("created missing store")
	}
	if !reflect.DeepEqual(before, diskTree(t, root)) {
		t.Fatal("missing read-only open wrote files")
	}
}
func TestReadOnlyDoesNotDisturbLiveHandle(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "store")
	s, err := chunkstore.Open(dir, chunkstore.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	before := diskTree(t, dir)
	other, err := chunkstore.Open(dir, chunkstore.Options{ReadOnly: true})
	if err == nil {
		other.Close()
		t.Fatal("opened twice")
	}
	if !reflect.DeepEqual(before, diskTree(t, dir)) {
		t.Fatal("failed read-only open changed live files")
	}
}

func TestReadOnlyUnflushedWAL(t *testing.T) {
	const childKey = "GO_CARBON_READONLY_WAL_CHILD"
	if dir := os.Getenv(childKey); dir != "" {
		s, err := chunkstore.Open(dir, chunkstore.Options{WALDir: filepath.Join(filepath.Dir(dir), "wal"), Now: func() time.Time { return time.Unix(1794700800, 0) }})
		if err != nil {
			t.Fatal(err)
		}
		c := chunkstore.MetricConfig{Name: "wal.metric", Retentions: []chunkstore.Retention{{Step: 1, Count: 3600}}, AggregationMethod: chunkstore.Last}
		if _, err = s.Create(context.Background(), c); err != nil {
			t.Fatal(err)
		}
		if err = s.UpdateMany(context.Background(), c.Name, []chunkstore.Point{{Timestamp: 1794700799, Value: 13}}); err != nil {
			t.Fatal(err)
		}
		// Exit without Close/Flush: the reader must replay this synced WAL in memory.
		os.Exit(0)
	}
	root := t.TempDir()
	dir := filepath.Join(root, "store")
	if err := os.Mkdir(filepath.Join(root, "wal"), 0700); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(os.Args[0], "-test.run=^TestReadOnlyUnflushedWAL$")
	cmd.Env = append(os.Environ(), childKey+"="+dir)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("writer: %v %s", err, out)
	}
	before := diskTree(t, root)
	s, err := chunkstore.Open(dir, chunkstore.Options{ReadOnly: true, Now: func() time.Time { return time.Unix(1794700800, 0) }})
	if err != nil {
		t.Fatal(err)
	}
	got, err := s.Fetch(context.Background(), "wal.metric", 1794700798, 1794700800)
	if err != nil {
		t.Fatal(err)
	}
	if got == nil || len(got.Values) < 1 || got.Values[0] != 13 {
		t.Fatalf("WAL value missing: %+v", got)
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(before, diskTree(t, root)) {
		t.Fatal("WAL replay changed source files")
	}
}
