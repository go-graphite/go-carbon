package downsize

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"
)

type discoveryFixture struct {
	*fakeSync
	catalogs map[string][]string
}

func (f *discoveryFixture) List(ctx context.Context, store, prefix, after string, limit int) ([]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	names := f.catalogs[store]
	i := sort.Search(len(names), func(i int) bool { return names[i] > after })
	return names[i:min(i+397, len(names))], nil
}

// A merge must checkpoint consumed cursors, not pages prefetched by listers.
// Exercise mismatched pages, overlap, filtered names, graceful cancellation and
// a crash with uncommitted bytes in both files and a torn final journal record.
func TestSyncDiscoveryResumeDifferential(t *testing.T) {
	f := newFakeSync()
	for i := 0; i < 33013; i++ {
		n := fmt.Sprintf("metric.%06d", i)
		for k, store := range []string{syncT, syncD1, syncD2} {
			if (i+k)%3 != 0 {
				f.store(store).archives[n] = nil
			}
		}
	}
	catalogs := map[string][]string{}
	for store, data := range f.stores {
		for n := range data.archives {
			catalogs[store] = append(catalogs[store], n)
		}
		sort.Strings(catalogs[store])
	}
	backend := &discoveryFixture{fakeSync: f, catalogs: catalogs}
	owner := func(n string) bool { return !strings.HasSuffix(n, "7") }
	discover := func(state string, cut int) (*syncer, error) {
		j, err := openSyncJournal(filepath.Join(state, "sync.jsonl"), "fixture")
		if err != nil {
			t.Fatal(err)
		}
		defer j.close()
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		count := 0
		s := &syncer{o: syncOptions{state: state, to: syncT, from: []string{syncD1, syncD2}}, b: backend, j: j, sum: syncSummary{phases: map[string]*syncPhase{}}}
		s.owner = func(n string) bool {
			count++
			if count == cut {
				cancel()
			}
			return owner(n)
		}
		return s, s.discover(ctx)
	}
	baseline := t.TempDir()
	want, err := discover(baseline, 0)
	if err != nil {
		t.Fatal(err)
	}
	rng := rand.New(rand.NewSource(8144))
	for trial := 0; trial < 5; trial++ {
		t.Run(fmt.Sprint(trial), func(t *testing.T) {
			state := t.TempDir()
			cut := 1 + rng.Intn(10000)
			if _, err := discover(state, cut); !errors.Is(err, context.Canceled) {
				t.Fatalf("cut %d: %v", cut, err)
			}
			journal := filepath.Join(state, "sync.jsonl")
			// Simulate death after writes but before the next journal commit.
			for _, file := range []string{"union.tsv", "missing.tsv"} {
				f, err := os.OpenFile(filepath.Join(state, file), os.O_APPEND|os.O_WRONLY, 0600)
				if err != nil {
					t.Fatal(err)
				}
				_, _ = f.WriteString("uncommitted\tbad\n")
				_ = f.Close()
			}
			jf, _ := os.OpenFile(journal, os.O_APPEND|os.O_WRONLY, 0600)
			_, _ = jf.WriteString(`{"kind":"discover_checkpoint"`)
			_ = jf.Close()
			got, err := discover(state, 0)
			if err != nil {
				t.Fatal(err)
			}
			if got.sum.Discovered != want.sum.Discovered || got.sum.Missing != want.sum.Missing || got.both != want.both {
				t.Fatalf("summary differs: %+v vs %+v", got.sum, want.sum)
			}
			for _, file := range []string{"union.tsv", "missing.tsv"} {
				a, _ := os.ReadFile(filepath.Join(baseline, file))
				b, _ := os.ReadFile(filepath.Join(state, file))
				if string(a) != string(b) {
					t.Fatalf("%s differs", file)
				}
			}
		})
	}
}

func TestSyncVerifyResumeDifferential(t *testing.T) {
	baseline, o, _ := threeSources(t)
	o.streams = 1
	if err := runSync(context.Background(), o, baseline); err != nil {
		t.Fatal(err)
	}
	expected := baseline.store(syncT)
	wantRows := semanticSyncRows(t, o.state)
	rng := rand.New(rand.NewSource(1212))
	for trial := 0; trial < 4; trial++ {
		t.Run(fmt.Sprint(trial), func(t *testing.T) {
			f, o, _ := threeSources(t)
			o.streams = 1
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			f.cancel = cancel
			f.failAfter = 1 + int64(rng.Intn(7))
			err := runSync(ctx, o, f)
			if err == nil {
				t.Fatal("expected mid-verify interruption")
			}
			f.failAfter = 0
			if err := runSync(context.Background(), o, f); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(f.store(syncT), expected) {
				t.Fatal("final target differs")
			}
			if got := semanticSyncRows(t, o.state); !reflect.DeepEqual(got, wantRows) {
				t.Fatalf("holes/fills differ\ngot %v\nwant %v", got, wantRows)
			}
			var progress progressSnapshot
			data, err := os.ReadFile(filepath.Join(o.state, "progress.json"))
			if err != nil {
				t.Fatal(err)
			}
			if err = json.Unmarshal(data, &progress); err != nil {
				t.Fatal(err)
			}
			if progress.Status != "complete" || progress.Discovered == 0 || progress.Verified == 0 || progress.Updated.IsZero() {
				t.Fatalf("bad progress: %+v", progress)
			}
		})
	}
}
func semanticSyncRows(t *testing.T, state string) []string {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(state, "sync.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	var result []string
	for _, line := range strings.Split(strings.TrimSpace(string(data)), "\n") {
		var row syncRow
		if err = json.Unmarshal([]byte(line), &row); err != nil {
			t.Fatal(err)
		}
		if row.Kind != "verified" && row.Kind != "copied" {
			continue
		}
		row.Time = ""
		blob, _ := json.Marshal(row)
		result = append(result, string(blob))
	}
	sort.Strings(result)
	return result
}

func TestCopyCatalogResumeDifferential(t *testing.T) {
	names := copyNames("metric.", 23451)
	var mu sync.Mutex
	var cursors []string
	failAfter := ""
	fetch := func(_ context.Context, after string) ([]string, error) {
		mu.Lock()
		defer mu.Unlock()
		cursors = append(cursors, after)
		if failAfter != "" && after >= failAfter {
			return nil, context.Canceled
		}
		i := sort.SearchStrings(names, after)
		if i < len(names) && names[i] == after {
			i++
		}
		return names[i:min(i+1777, len(names))], nil
	}
	path := filepath.Join(t.TempDir(), "catalog")
	c, err := openCopyCatalog(path, fetch, nil)
	if err != nil {
		t.Fatal(err)
	}
	next, closeReader, err := c.reader(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2001; i++ {
		if _, err = next(); err != nil {
			t.Fatal(err)
		}
	}
	closeReader()
	saved := c.checkpoint
	_ = c.file.Close()
	// A crash may leave data after the committed cursor, which must be discarded.
	tail, _ := os.OpenFile(path+".names", os.O_APPEND|os.O_WRONLY, 0600)
	_, _ = tail.WriteString("torn")
	_ = tail.Close()
	cursors = nil
	c, err = openCopyCatalog(path, fetch, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer c.file.Close()
	next, closeReader, err = c.reader(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer closeReader()
	var got []string
	for {
		n, err := next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		got = append(got, n)
	}
	if !reflect.DeepEqual(got, names) {
		t.Fatal("replayed catalog differs")
	}
	if len(cursors) == 0 || cursors[0] != saved.After {
		t.Fatalf("listing restarted from %v, expected %s", cursors, saved.After)
	}
	if !c.checkpoint.Done || c.checkpoint.Names != int64(len(names)) {
		t.Fatalf("checkpoint %+v", c.checkpoint)
	}
}

func TestProgressFileUpdatesWithoutJournalWrites(t *testing.T) {
	path := filepath.Join(t.TempDir(), "progress.json")
	stop := startProgressFile(context.Background(), path, func() progressSnapshot {
		return progressSnapshot{Phase: "discover", Discovered: 123, Cursors: map[string]string{"source": "last.name"}}
	}, nil)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var first progressSnapshot
	if err = json.Unmarshal(data, &first); err != nil {
		t.Fatal(err)
	}
	// A short final write proves atomic replacement and the interrupted state.
	if err = stop(context.Canceled); err != nil {
		t.Fatal(err)
	}
	data, _ = os.ReadFile(path)
	var last progressSnapshot
	if err = json.Unmarshal(data, &last); err != nil {
		t.Fatal(err)
	}
	if !last.Updated.After(first.Updated) || last.Status != "interrupted" || last.Cursors["source"] != "last.name" {
		t.Fatalf("%+v", last)
	}
	if time.Since(last.Updated) > time.Second {
		t.Fatal("stale progress")
	}
}

type noListingSync struct{ *fakeSync }

func (f noListingSync) List(context.Context, string, string, string, int) ([]string, error) {
	return nil, errors.New("completed legacy discovery must not list again")
}

func TestSyncLegacyCompletedStateStillResumes(t *testing.T) {
	f, o, _ := threeSources(t)
	if err := runSync(context.Background(), o, f); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(o.state, "sync.jsonl")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var old []string
	for _, line := range strings.Split(strings.TrimSpace(string(data)), "\n") {
		var row syncRow
		if err = json.Unmarshal([]byte(line), &row); err != nil {
			t.Fatal(err)
		}
		if row.Kind != "discover_checkpoint" {
			old = append(old, line)
		}
	}
	if err = os.WriteFile(path, []byte(strings.Join(old, "\n")+"\n"), 0600); err != nil {
		t.Fatal(err)
	}
	before := len(f.copies)
	if err = runSync(context.Background(), o, noListingSync{f}); err != nil {
		t.Fatal(err)
	}
	if len(f.copies) != before {
		t.Fatal("legacy resume repeated fills")
	}
}

func TestResumeStateRejectsConcurrentWriters(t *testing.T) {
	for _, kind := range []string{"sync", "copy"} {
		t.Run(kind, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "state")
			if kind == "sync" {
				j, err := openSyncJournal(path, "identity")
				if err != nil {
					t.Fatal(err)
				}
				defer j.close()
				if other, err := openSyncJournal(path, "identity"); err == nil {
					other.close()
					t.Fatal("second sync opened same state")
				}
			}
			if kind == "copy" {
				j, err := openCopyJournal(path, "identity")
				if err != nil {
					t.Fatal(err)
				}
				defer j.close()
				if other, err := openCopyJournal(path, "identity"); err == nil {
					other.close()
					t.Fatal("second copy opened same state")
				}
			}
		})
	}
}
