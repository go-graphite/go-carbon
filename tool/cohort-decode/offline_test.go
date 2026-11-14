package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"io"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/vfs"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/internal/whisperio"
)

func offlineFileHashes(t *testing.T, dir string) map[string][32]byte {
	t.Helper()
	result := make(map[string][32]byte)
	err := filepath.WalkDir(dir, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		result[path] = sha256.Sum256(data)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return result
}

// Exercise read-only source equivalence independently of the test machine's
// free-space percentage. Real disk operations and exclusive locks remain live.
type offlineFixtureFS struct{ vfs.FS }

func (offlineFixtureFS) GetDiskUsage(string) (vfs.DiskUsage, error) {
	return vfs.DiskUsage{}, vfs.ErrUnsupported
}

func TestOfflineNativeMatchesLiveAndPreservesEveryFile(t *testing.T) {
	original := vfs.Default
	vfs.Default = offlineFixtureFS{original}
	defer func() { vfs.Default = original }()
	for _, continuation := range []bool{false, true} {
		t.Run(map[bool]string{false: "exact", true: "continuation"}[continuation], func(t *testing.T) {
			ctx := context.Background()
			dir := filepath.Join(t.TempDir(), "store")
			opts := store.Options{ExactBlocks: true, CacheSize: 1 << 20, MemTableSize: 1 << 20}
			if continuation {
				opts.Continuation = &whisperio.ContinuationCodec{}
				opts.TimeBuckets = true
			}
			db, err := store.Open(dir, opts)
			if err != nil {
				t.Fatal(err)
			}
			config := store.MetricConfig{Name: "audit.zero", AggregationMethod: store.Average, XFilesFactor: .5, Retentions: []store.Retention{{Step: 10, Count: 100}}}
			if _, err = db.Create(ctx, config); err != nil {
				t.Fatal(err)
			}
			now := time.Now().Unix() / 10 * 10
			if err = db.UpdateMany(ctx, config.Name, []store.Point{{Timestamp: now - 40, Value: 0}, {Timestamp: now - 30, Value: math.Copysign(0, -1)}, {Timestamp: now - 20, Value: 3}}); err != nil {
				t.Fatal(err)
			}
			pin, err := db.PinNativeMergeSource("test-live")
			if err != nil {
				t.Fatal(err)
			}
			var expected bytes.Buffer
			if err = pin.Write(ctx, []string{config.Name, "absent"}, &expected); err != nil {
				t.Fatal(err)
			}
			pin.Close()
			if err = offlineNative(dir, bytes.NewBufferString(""), io.Discard); err == nil {
				t.Fatal("live store admitted")
			}
			if err = db.Close(); err != nil {
				t.Fatal(err)
			}
			before := offlineFileHashes(t, dir)
			var output bytes.Buffer
			input := bytes.NewBufferString("{\"op\":\"list\"}\n{\"op\":\"list\",\"after\":\"audit.zero\"}\n{\"op\":\"export\",\"names\":[\"audit.zero\",\"absent\"]}\n")
			if err = offlineNative(dir, input, &output); err != nil {
				t.Fatal(err)
			}
			reader := bufio.NewReader(&output)
			for _, want := range [][]string{{config.Name}, {}} {
				line, err := reader.ReadBytes('\n')
				if err != nil {
					t.Fatal(err)
				}
				var row struct {
					Names []string `json:"names"`
				}
				if err = json.Unmarshal(line, &row); err != nil {
					t.Fatal(err)
				}
				if !reflect.DeepEqual(row.Names, want) {
					t.Fatalf("catalogue %v != %v", row.Names, want)
				}
			}
			line, err := reader.ReadBytes('\n')
			if err != nil {
				t.Fatal(err)
			}
			var header struct{ Status, Bytes int }
			if err = json.Unmarshal(line, &header); err != nil {
				t.Fatal(err)
			}
			wire, err := io.ReadAll(reader)
			if err != nil {
				t.Fatal(err)
			}
			if header.Status != 200 || header.Bytes != len(wire) || !bytes.Equal(expected.Bytes(), wire) {
				t.Fatal("offline native wire differs from live committed export")
			}
			if !reflect.DeepEqual(before, offlineFileHashes(t, dir)) {
				t.Fatal("offline reads changed database files, including LOCK")
			}
		})
	}
}

func TestOfflineRejectsMissingLockAndWriteRequests(t *testing.T) {
	dir := t.TempDir()
	before := offlineFileHashes(t, dir)
	if err := offlineNative(dir, bytes.NewBufferString(""), io.Discard); err == nil {
		t.Fatal("missing LOCK admitted")
	}
	if !reflect.DeepEqual(before, offlineFileHashes(t, dir)) {
		t.Fatal("created database artifacts")
	}
	if err := offlineRequests(nil, nil, bytes.NewBufferString("{\"op\":\"import\"}\n"), io.Discard); err == nil {
		t.Fatal("write operation admitted")
	}
	var bounded boundedNativeWire
	bounded.Grow(64 << 20)
	bounded.Buffer.Write(make([]byte, 64<<20))
	if _, err := bounded.Write([]byte{1}); err == nil || bounded.Len() != 64<<20 {
		t.Fatal("native wire exceeded bound")
	}
}
