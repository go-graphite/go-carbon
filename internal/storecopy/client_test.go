package storecopy

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/vfs"

	"github.com/go-graphite/go-carbon/internal/chunkstore"
)

func offlineFixture(t *testing.T) (http.Handler, *Manifest) {
	t.Helper()
	root := t.TempDir()
	s, err := chunkstore.Open(filepath.Join(root, "live"), chunkstore.Options{ExactBlocks: true, CacheSize: 1 << 20, MemTableSize: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Create(context.Background(), chunkstore.MetricConfig{Name: "fixture.metric", AggregationMethod: chunkstore.Average, Retentions: []chunkstore.Retention{{Step: 1, Count: 60}}}); err != nil {
		t.Fatal(err)
	}
	if err := s.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := s.Checkpoint(filepath.Join(root, "snapshot")); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	handler, manifest, err := OpenOffline(filepath.Join(root, "snapshot"))
	if err != nil {
		t.Fatal(err)
	}
	return handler, manifest
}

func TestPullDryRunResumeRepairAndLease(t *testing.T) {
	handler, manifest := offlineFixture(t)
	var requests atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == Endpoint+"/file" {
			requests.Add(1)
		}
		handler.ServeHTTP(w, r)
	}))
	defer server.Close()
	client := &Client{URL: server.URL}
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	dest := filepath.Join(root, "copy")
	options := PullOptions{Workers: 4, Chunk: 1 << 20}
	if err := client.Pull(context.Background(), dest, manifest.ID, options); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(dest); !os.IsNotExist(err) {
		t.Fatal("dry run wrote destination")
	}
	options.Apply = true
	if err := client.Pull(context.Background(), dest, manifest.ID, options); err != nil {
		t.Fatal(err)
	}
	initial := requests.Load()
	options.ReuseVerified = true
	if err := client.Pull(context.Background(), dest, manifest.ID, options); err != nil {
		t.Fatal(err)
	}
	if requests.Load() != initial {
		t.Fatal("verified resume redownloaded ranges")
	}
	file := filepath.Join(dest, "store", "CHUNKSTORE")
	beforeInfo, err := os.Stat(file)
	if err != nil {
		t.Fatal(err)
	}
	f, err := os.OpenFile(file, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteAt([]byte("damage"), 0); err != nil {
		t.Fatal(err)
	}
	f.Close()
	if err := os.Chtimes(file, beforeInfo.ModTime(), beforeInfo.ModTime()); err != nil {
		t.Fatal(err)
	}
	if err := client.Pull(context.Background(), dest, manifest.ID, options); err != nil {
		t.Fatal(err)
	}
	if requests.Load() != initial+1 {
		t.Fatal("resume did not repair exactly damaged range")
	}
	lease := filepath.Join(root, "lease.json")
	data, _ := json.Marshal(Limits{Workers: 2, Rate: 10000000, Expires: time.Now().Add(-time.Second)})
	if err := os.WriteFile(lease, data, 0600); err != nil {
		t.Fatal(err)
	}
	options.Lease = lease
	if err := os.WriteFile(file, []byte("damage"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := client.Pull(context.Background(), dest, manifest.ID, options); err == nil {
		t.Fatal("expired lease accepted")
	}
}

func TestCopyFileRejectsTraversalAndUnboundedRanges(t *testing.T) {
	handler, manifest := offlineFixture(t)
	for _, query := range []string{"name=../secret&offset=0&length=1", "name=store/CHUNKSTORE&offset=-1&length=1", "name=store/CHUNKSTORE&offset=0&length=999999999", "name=store/CHUNKSTORE&offset=0&length=1&id=wrong"} {
		r := httptest.NewRequest(http.MethodGet, Endpoint+"/file?id="+manifest.ID+"&"+query, nil)
		if query[len(query)-8:] == "id=wrong" {
			r.URL.RawQuery = "name=store/CHUNKSTORE&offset=0&length=1&id=wrong"
		}
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, r)
		if w.Code == http.StatusOK {
			t.Fatal("invalid file request accepted", query)
		}
	}
}

type corruptWriter struct{ http.ResponseWriter }

func (w corruptWriter) Write(data []byte) (int, error) {
	copy := append([]byte(nil), data...)
	if len(copy) > 0 {
		copy[0] ^= 1
	}
	return w.ResponseWriter.Write(copy)
}

func TestPullRejectsTransportCorruption(t *testing.T) {
	handler, manifest := offlineFixture(t)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == Endpoint+"/file" {
			handler.ServeHTTP(corruptWriter{w}, r)
		} else {
			handler.ServeHTTP(w, r)
		}
	}))
	defer server.Close()
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := (&Client{URL: server.URL}).Pull(context.Background(), filepath.Join(root, "copy"), manifest.ID, PullOptions{Workers: 2, Chunk: 1 << 20, Apply: true, Output: io.Discard}); err == nil {
		t.Fatal("damaged stream accepted")
	}
}

func TestExpiredLeaseCancelsRateWait(t *testing.T) {
	handler, manifest := offlineFixture(t)
	server := httptest.NewServer(handler)
	defer server.Close()
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	lease := filepath.Join(root, "lease.json")
	data, _ := json.Marshal(Limits{Workers: 4, Rate: 1, Expires: time.Now().Add(500 * time.Millisecond)})
	if err := os.WriteFile(lease, data, 0600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err = (&Client{URL: server.URL}).Pull(ctx, filepath.Join(root, "copy"), manifest.ID, PullOptions{Workers: 4, Chunk: 1 << 20, Apply: true, Lease: lease})
	if err == nil {
		t.Fatal("expired lease accepted")
	}
	if ctx.Err() != nil {
		t.Fatal("worker waited for rate reservation after lease expiry")
	}
}

func TestPullStaleLockAndRunningStore(t *testing.T) {
	handler, manifest := offlineFixture(t)
	server := httptest.NewServer(handler)
	defer server.Close()
	client := &Client{URL: server.URL}
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	dest := filepath.Join(root, "copy")
	if err := os.Mkdir(dest, 0700); err != nil {
		t.Fatal(err)
	}
	// A killed worker leaves the pathname; the kernel ownership is authoritative.
	if err := os.WriteFile(filepath.Join(dest, "copy.lock"), []byte("stale PID"), 0600); err != nil {
		t.Fatal(err)
	}
	options := PullOptions{Workers: 1, Chunk: 1 << 20, Apply: true, ReuseVerified: true}
	if err := client.Pull(context.Background(), dest, manifest.ID, options); err != nil {
		t.Fatal("stale lock resume", err)
	}
	lock, err := os.OpenFile(filepath.Join(dest, "copy.lock"), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		t.Fatal(err)
	}
	if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		t.Fatal(err)
	}
	if err := client.Pull(context.Background(), dest, manifest.ID, options); err == nil {
		t.Fatal("concurrent pull accepted")
	}
	lock.Close()
	os.Remove(filepath.Join(dest, "copy.lock"))
	running, err := chunkstore.Open(filepath.Join(dest, "store"), chunkstore.Options{ExactBlocks: true, CacheSize: 1 << 20, MemTableSize: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	defer running.Close()
	if err := client.Pull(context.Background(), dest, manifest.ID, options); err == nil {
		t.Fatal("running Pebble destination overwritten")
	}
}

func TestPrepareRequiresSealedVerifiedBytesAndPebbleLock(t *testing.T) {
	handler, manifest := offlineFixture(t)
	server := httptest.NewServer(handler)
	defer server.Close()
	client := &Client{URL: server.URL}
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	dest := filepath.Join(root, "copy")
	options := PullOptions{Workers: 1, Chunk: 1 << 20, Apply: true, ReuseVerified: true}
	if err := client.Pull(context.Background(), dest, manifest.ID, options); err != nil {
		t.Fatal(err)
	}
	if err := Prepare(dest, false); err == nil {
		t.Fatal("unsealed copy prepared")
	}
	manifest.Sealed = true
	if err := client.Pull(context.Background(), dest, manifest.ID, options); err != nil {
		t.Fatal(err)
	}
	// Hold the same lock primitive as Pebble, including its in-process registry.
	lock, err := vfs.Default.Lock(filepath.Join(dest, "store", "LOCK"))
	if err != nil {
		t.Fatal(err)
	}
	if err := Prepare(dest, true); err == nil {
		t.Fatal("running store prepared")
	}
	lock.Close()
	marker := filepath.Join(dest, "store", "CHUNKSTORE")
	original, err := os.ReadFile(marker)
	if err != nil {
		t.Fatal(err)
	}
	changed := append([]byte(nil), original...)
	changed[0] ^= 1
	if err := os.WriteFile(marker, changed, 0600); err != nil {
		t.Fatal(err)
	}
	if err := Prepare(dest, true); err == nil {
		t.Fatal("changed bytes bypassed checksum via saved stamp")
	}
	if _, err := os.Stat(filepath.Join(dest, "PREPARED")); !os.IsNotExist(err) {
		t.Fatal("failed prepare published marker", err)
	}
	if err := client.Pull(context.Background(), dest, manifest.ID, options); err != nil {
		t.Fatal("repair", err)
	}
	if err := Prepare(dest, false); err != nil {
		t.Fatal(err)
	}
	if err := Prepare(dest, true); err != nil {
		t.Fatal("prepare restart", err)
	}
	if err := client.Pull(context.Background(), dest, manifest.ID, options); err == nil {
		t.Fatal("prepared clone overwritten")
	}
}
