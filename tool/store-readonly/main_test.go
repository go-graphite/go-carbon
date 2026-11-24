package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/vfs"
	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/internal/whisperio"
)

type fixtureFS struct{ vfs.FS }

func (fixtureFS) GetDiskUsage(string) (vfs.DiskUsage, error) {
	return vfs.DiskUsage{}, vfs.ErrUnsupported
}
func fileState(t *testing.T, root string) map[string]string {
	t.Helper()
	result := map[string]string{}
	err := filepath.WalkDir(root, func(path string, e os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		st, err := e.Info()
		if err != nil {
			return err
		}
		if e.IsDir() {
			return nil
		}
		b, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		result[path] = fmt.Sprintf("%d/%d/%x", st.Size(), st.ModTime().UnixNano(), sha256.Sum256(b))
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return result
}
func TestInspectPreservesFilesAndPoints(t *testing.T) {
	original := vfs.Default
	vfs.Default = fixtureFS{original}
	defer func() { vfs.Default = original }()
	for _, cont := range []bool{false, true} {
		t.Run(fmt.Sprint(cont), func(t *testing.T) {
			dir := filepath.Join(t.TempDir(), "store")
			opts := store.Options{ExactBlocks: true, CacheSize: 1 << 20, MemTableSize: 1 << 20}
			if cont {
				opts.Continuation = &whisperio.ContinuationCodec{}
				opts.TimeBuckets = true
			}
			source := filepath.Join(t.TempDir(), "original")
			if err := os.Mkdir(source, 0700); err != nil {
				t.Fatal(err)
			}
			opts.IncrementalSource = source
			db, err := store.Open(dir, opts)
			if err != nil {
				t.Fatal(err)
			}
			now := time.Now().Unix() / 60 * 60
			c := store.MetricConfig{Name: "history.one", AggregationMethod: store.Average, XFilesFactor: .5, Retentions: []store.Retention{{Step: 60, Count: 1440}}}
			if _, err = db.Create(context.Background(), c); err != nil {
				t.Fatal(err)
			}
			if err = db.UpdateMany(context.Background(), c.Name, []store.Point{{Timestamp: now - 180, Value: 0}, {Timestamp: now - 120, Value: 42}}); err != nil {
				t.Fatal(err)
			}
			if err = db.Close(); err != nil {
				t.Fatal(err)
			}
			before := fileState(t, dir)
			var out bytes.Buffer
			q := fmt.Sprintf("{\"name\":\"history.one\",\"ranges\":[{\"from\":%d,\"until\":%d}]}\n{\"name\":\"absent\"}\n", now-180, now-120)
			if err = inspect(dir, bytes.NewBufferString(q), &out); err != nil {
				t.Fatal(err)
			}
			dec := json.NewDecoder(&out)
			var row response
			if err = dec.Decode(&row); err != nil {
				t.Fatal(err)
			}
			if row.Last == nil || row.Last.Timestamp != now-120 || row.Last.Bits != "4045000000000000" || row.LastUpdate == 0 || len(row.Archives) != 1 || row.Archives[0].Zero != 1 || row.Archives[0].Nonzero != 1 || len(row.Archives[0].Points) != 1 || row.Archives[0].Points[0].Bits != "0000000000000000" {
				t.Fatalf("unexpected response %+v", row)
			}
			if err = dec.Decode(&row); err != nil || row.Error != "not found" {
				t.Fatalf("missing metric: %+v %v", row, err)
			}
			if after := fileState(t, dir); !reflect.DeepEqual(before, after) {
				t.Fatalf("file size/mtime/SHA256 changed: before %v after %v", before, after)
			}
			t.Logf("PROVEN: %d files retain identical size, mtime and SHA256 after read-only open/snapshot/close", len(before))
		})
	}
}
func TestMissingStoreDoesNotCreate(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "absent")
	if err := inspect(dir, bytes.NewBufferString(""), &bytes.Buffer{}); err == nil {
		t.Fatal("missing store accepted")
	}
	if _, err := os.Stat(dir); !os.IsNotExist(err) {
		t.Fatal("created missing store")
	}
}
