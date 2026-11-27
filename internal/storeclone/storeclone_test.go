package storeclone

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/cockroachdb/pebble"
)

func openSource(t *testing.T, dir string) *pebble.DB {
	t.Helper()
	opts := &pebble.Options{Logger: quietLogger{}, MemTableSize: 256 << 10, L0CompactionThreshold: 2}
	opts.Levels = make([]pebble.LevelOptions, 7)
	for i := range opts.Levels {
		opts.Levels[i].TargetFileSize = 64 << 10
	}
	m := *pebble.DefaultMerger
	m.Name = "test.merger" // the clone must not need the store's merge operator
	opts.Merger = &m
	db, err := pebble.Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	return db
}

func churn(t *testing.T, db *pebble.DB, tag string) {
	t.Helper()
	for i := 0; i < 2000; i++ {
		if err := db.Set([]byte(fmt.Sprintf("h/%s/%06d", tag, i)), bytes.Repeat([]byte{byte(i)}, 64), pebble.NoSync); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := db.Compact([]byte("a"), []byte("z"), true); err != nil {
		t.Fatal(err)
	}
}

func transfer(t *testing.T, staging string, files []File, target string) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	got := make(chan error, 1)
	go func() {
		_, err := Receive(context.Background(), ln, target, nil)
		got <- err
	}()
	if err := Send(context.Background(), staging, files, ln.Addr().String(), 4, nil, nil); err != nil {
		t.Fatal(err)
	}
	if err := <-got; err != nil {
		t.Fatal(err)
	}
}

func contents(t *testing.T, dir string) map[string]string {
	t.Helper()
	db, err := openReadOnly(dir, "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	it, err := db.NewIter(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer it.Close()
	kv := map[string]string{}
	for it.First(); it.Valid(); it.Next() {
		kv[string(it.Key())] = string(it.Value())
	}
	return kv
}

// cloneUnderLoad stages a store while a writer and forced compactions run.
// The hooks compact between the link pass and the manifest copy (new live
// tables the first pass missed) and again after the copy (a table the copy
// names is deleted), so both closure branches run.
func cloneUnderLoad(t *testing.T, skipClosure bool) error {
	root := t.TempDir()
	source := filepath.Join(root, "source")
	db := openSource(t, source)
	var written atomic.Int64
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(2)
	go func() { // writer: one key per batch in order, so any flushed state is a prefix
		defer wg.Done()
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			if err := db.Set([]byte(fmt.Sprintf("k/%09d", i)), []byte(strconv.Itoa(i)), pebble.NoSync); err != nil {
				return
			}
			written.Store(int64(i + 1))
		}
	}()
	var once sync.Once
	stopAll := func() { once.Do(func() { close(stop); wg.Wait() }) }
	t.Cleanup(func() { stopAll(); _ = db.Close() })
	go func() { // forced compactions
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			_ = db.Flush()
			_ = db.Compact([]byte("k/"), []byte("k0"), true)
		}
	}()
	for written.Load() < 20000 {
	}
	if err := db.Flush(); err != nil {
		t.Fatal(err)
	}
	flushedBefore := written.Load()
	staging := filepath.Join(root, "staging")
	files, err := stage(source, staging, stageHooks{
		afterLink:     func() { churn(t, db, "a") },
		beforeClosure: func() { churn(t, db, "b") },
		skipClosure:   skipClosure,
	})
	if err != nil {
		t.Fatal(err)
	}
	target := filepath.Join(root, "clone")
	transfer(t, staging, files, target)
	_, verr := Verify(target, 4)
	if verr != nil {
		return verr
	}
	kv := contents(t, target)
	n := int64(0)
	for ; ; n++ {
		v, ok := kv[fmt.Sprintf("k/%09d", n)]
		if !ok {
			break
		}
		if v != strconv.FormatInt(n, 10) {
			t.Fatalf("key %d has %q", n, v)
		}
	}
	for k := range kv {
		if k[0] == 'k' {
			if i, _ := strconv.ParseInt(k[2:], 10, 64); i >= n {
				t.Fatalf("clone is not a flushed prefix: has %s beyond %d", k, n)
			}
		}
	}
	if n < flushedBefore {
		t.Fatalf("clone has %d keys, %d were flushed before staging", n, flushedBefore)
	}
	t.Logf("clone under load: %d files, prefix %d keys (flushed before %d)", len(files), n, flushedBefore)

	// Quiescent: the clone equals a Pebble checkpoint of the same flushed state.
	stopAll()
	if err := db.Flush(); err != nil {
		t.Fatal(err)
	}
	staging2, target2, checkpoint := filepath.Join(root, "staging2"), filepath.Join(root, "clone2"), filepath.Join(root, "checkpoint")
	files2, err := Stage(source, staging2)
	if err != nil {
		t.Fatal(err)
	}
	transfer(t, staging2, files2, target2)
	if err := db.Checkpoint(checkpoint, pebble.WithFlushedWAL()); err != nil {
		t.Fatal(err)
	}
	if _, err := Verify(target2, 4); err != nil {
		t.Fatal(err)
	}
	want, got := contents(t, checkpoint), contents(t, target2)
	if len(want) == 0 || len(want) != len(got) {
		t.Fatalf("checkpoint %d keys, clone %d", len(want), len(got))
	}
	for k, v := range want {
		if got[k] != v {
			t.Fatalf("key %s: checkpoint %q clone %q", k, v, got[k])
		}
	}
	return nil
}

func TestCloneUnderLoadEqualsCheckpoint(t *testing.T) {
	if err := cloneUnderLoad(t, false); err != nil {
		t.Fatal(err)
	}
}

// Without the closure loop the tables created after the link pass are
// missing, and verification must catch it.
func TestCloneWithoutClosureFails(t *testing.T) {
	err := cloneUnderLoad(t, true)
	if err == nil {
		t.Fatal("clone without the closure loop verified")
	}
	t.Log(err)
}

func TestReceiveRejectsBadSum(t *testing.T) {
	if !validName("000001.sst") || validName("../x") || validName("a/b") || validName("") {
		t.Fatal("name validation")
	}
	if err := checkList([]File{{Name: "a", Size: 1, SHA256: "x"}}, map[string]File{"a": {Name: "a", Size: 1, SHA256: "y"}}); err == nil {
		t.Fatal("checksum mismatch accepted")
	}
}
