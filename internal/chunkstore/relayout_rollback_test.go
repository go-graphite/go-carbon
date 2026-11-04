package chunkstore

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/vfs"
)

// Copy this file into a checkout from before the relayout reclaim journal when
// building the old test binary. The helper uses only APIs and durable records
// present there.
func TestRelayoutRollbackLegacyHelper(t *testing.T) {
	dir := os.Getenv("RELAYOUT_ROLLBACK_DIR")
	if dir == "" {
		t.Skip("rollback subprocess helper")
	}
	s, _ := openTestStore(t, dir, Options{ExactBlocks: true, TimeBuckets: true, Continuation: &controlledRelayoutCodec{bytes: func(string) int { return 20 }}, RelayoutReclaimBytes: 20})
	defer closeTestStore(t, s)
	keys, _, n, err := s.readRelayoutReclaims(context.Background())
	if err != nil || len(keys) != 2 || n != 40 {
		t.Fatalf("old decoder: %d/%d %v", len(keys), n, err)
	}
	if cursor, done, err := s.relayoutCursor(); err != nil || cursor != 2 || done {
		t.Fatalf("old cursor: %d/%v %v", cursor, done, err)
	}
	if err := s.RelayoutContinuation(context.Background(), 0); err != nil {
		t.Fatal(err)
	}
	if st := s.RelayoutStats(); !st.Done || st.Metrics != 6 || st.Bytes != 120 || st.UnreclaimedBytes != 0 || st.ReclaimedBytes != 160 {
		t.Fatalf("old resume: %+v", st)
	}
	for i := 0; i < 8; i++ {
		m, err := s.Metadata(context.Background(), fmt.Sprintf("rollback.%d", i))
		if err != nil {
			t.Fatal(err)
		}
		io := s.newContinuationIO(context.Background(), s.db, nil, m)
		if value, err := io.Get(ContinuationBucketKey(0, []byte("c"))); err != nil || string(value) != "group" {
			t.Fatalf("old live copy: %q %v", value, err)
		}
		if _, err := io.Get([]byte("c")); !errors.Is(err, ErrNotFound) {
			t.Fatalf("old source remains: %v", err)
		}
	}
}

func TestRelayoutRollbackToBaseBinary(t *testing.T) {
	base := os.Getenv("RELAYOUT_BASE_BINARY")
	if base == "" {
		t.Skip("set base test binary for cross-version rollback")
	}
	dir := t.TempDir()
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	fs := &relayoutCancelFS{FS: vfs.Default, onOutput: func() { close(entered); <-release }}
	codec := &controlledRelayoutCodec{bytes: func(string) int { return 20 }}
	s, _ := openTestStore(t, dir, Options{fs: fs, ExactBlocks: true, TimeBuckets: true, Continuation: codec, RelayoutReclaimBytes: 20})
	fs.store.Store(s)
	ctx, cancel := context.WithCancel(context.Background())
	var items []relayoutItem
	for i := 0; i < 8; i++ {
		m := createTestMetric(t, s, fmt.Sprintf("rollback.%d", i))
		p, err := codec.RelayoutPlan(ctx, s.newContinuationIO(ctx, s.db, nil, m))
		if err != nil {
			t.Fatal(err)
		}
		items = append(items, relayoutItem{m: m, plan: p})
	}
	fs.armed.Store(true)
	done := make(chan error, 1)
	go func() { done <- s.relayoutGroup(ctx, items[:1]) }()
	joined := false
	defer func() {
		cancel()
		unblock()
		if !joined {
			<-done
		}
	}()
	select {
	case <-entered:
	case <-time.After(time.Minute):
		t.Fatal("reclaim did not start")
	}
	second := make(chan error, 1)
	go func() { second <- s.relayoutGroup(ctx, items[1:2]) }()
	select {
	case err := <-second:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Minute):
		t.Fatal("next batch did not publish")
	}
	if err := s.setRelayoutCursor(2, false); err != nil {
		t.Fatal(err)
	}
	cancel()
	unblock()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	joined = true
	closeTestStore(t, s)
	cmd := exec.Command(base, "-test.run=^TestRelayoutRollbackLegacyHelper$", "-test.timeout=1m", "-test.v")
	cmd.Env = append(os.Environ(), "RELAYOUT_ROLLBACK_DIR="+dir)
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("base rollback: %v\n%s", err, output)
	} else {
		t.Log(string(output))
	}
}

// A cross-binary oracle hashes every durable key and value, including revisions
// and cursor bytes, after the real K=1 scheduler finishes a multi-batch run.
func TestRelayoutSerialOutputOracle(t *testing.T) {
	codec := &controlledRelayoutCodec{bytes: func(string) int { return 20 }}
	s, _ := openTestStore(t, "/store", Options{fs: vfs.NewMem(), ExactBlocks: true, TimeBuckets: true, Continuation: codec, RelayoutReclaimBytes: 60})
	defer closeTestStore(t, s)
	for i := 0; i < 32; i++ {
		createTestMetric(t, s, fmt.Sprintf("oracle.%02d", i))
	}
	if err := s.RelayoutContinuation(context.Background(), 0); err != nil {
		t.Fatal(err)
	}
	it, err := s.db.NewIter(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer it.Close()
	h := sha256.New()
	for it.First(); it.Valid(); it.Next() {
		for _, data := range [][]byte{it.Key(), it.Value()} {
			_, _ = h.Write(binary.BigEndian.AppendUint64(nil, uint64(len(data))))
			_, _ = h.Write(data)
		}
	}
	if err := it.Error(); err != nil {
		t.Fatal(err)
	}
	// Captured with an unmodified test binary built from before the reclaim journal.
	const releaseSHA = "5bdc738ec85ea7871167b35a1a18e3bf7a69030c33eaca3a2e894ef4d33f4955"
	if got := fmt.Sprintf("%x", h.Sum(nil)); got != releaseSHA {
		t.Fatalf("durable K=1 output changed: %s", got)
	}
	t.Logf("SERIAL_SHA256 %x", h.Sum(nil))
}
