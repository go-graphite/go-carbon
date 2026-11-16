package downsize

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

type interruptAfterFill struct {
	*fakeSync
	cancel context.CancelFunc
}

func (f *interruptAfterFill) Copy(ctx context.Context, source, target string, names []string, journal string) (syncCopyResult, error) {
	r, err := f.fakeSync.Copy(ctx, source, target, names, journal)
	if err == nil && f.cancel != nil {
		f.cancel()
		f.cancel = nil
	}
	return r, err
}

func TestSyncFillInterruptedObservationReusesReceipt(t *testing.T) {
	t.Parallel()
	f := &interruptAfterFill{fakeSync: newFakeSync()}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	f.cancel = cancel
	for _, host := range []string{syncT, syncD1} {
		f.put(host, "metric", []syncArchive{{3600, 240}})
	}
	delete(f.store(syncT).points["metric"][3600], syncTestNow-5*3600)
	o := syncOptions{to: syncT, from: []string{syncD1}, state: t.TempDir(), batch: 2, streams: 1, now: func() time.Time { return time.Unix(syncTestNow, 0) }}
	if err := runSync(ctx, o, f); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if len(f.copies) != 1 {
		t.Fatal(f.copies)
	}
	if err := runSync(context.Background(), o, f); err != nil {
		t.Fatal(err)
	}
	if len(f.copies) != 1 {
		t.Fatal("durable fill was imported again", f.copies)
	}
}

func TestSyncFillNewFrontierHoleGetsNewImport(t *testing.T) {
	t.Parallel()
	for _, sameName := range []bool{false, true} {
		t.Run(map[bool]string{false: "different name", true: "same name new mutation"}[sameName], func(t *testing.T) {
			f, o := frontierFixture(t)
			o.noCopy = false
			o.maxRounds = 0
			if err := runSync(context.Background(), o, f); err != nil {
				t.Fatal(err)
			}
			firstCopies := len(f.copies)
			name := "metric.02"
			if sameName {
				name = "metric.19"
			}
			delete(f.store(syncT).points[name][3600], syncTestNow-5*3600)
			if err := runSync(context.Background(), o, f); err != nil {
				t.Fatal(err)
			}
			if _, ok := f.store(syncT).points[name][3600][syncTestNow-5*3600]; !ok {
				t.Fatal("new hole reused an old fill receipt")
			}
			if len(f.copies) <= firstCopies {
				t.Fatal("no new import for new hole", f.copies)
			}
			found := false
			for _, call := range f.copies[firstCopies:] {
				if strings.Contains(call, " "+name) {
					found = true
				}
			}
			if !found {
				t.Fatal("new import selected wrong names", f.copies[firstCopies:])
			}
		})
	}
}

func TestSyncFillIdentityScopeAndObservation(t *testing.T) {
	t.Parallel()
	j, s := checkpointFixture(t)
	defer j.close()
	s.o.now = func() time.Time { return time.Unix(syncTestNow, 0) }
	s.o.windows = syncRanges{{Name: "gap", From: 100, Until: 200}}
	add := func(name string) {
		t.Helper()
		if err := j.append(syncRow{Kind: "verified", Round: 1, Holes: []syncHole{{Name: name, Source: syncD1, First: 100, Last: 110, Step: 10, Points: 2}}}); err != nil {
			t.Fatal(err)
		}
	}
	add("a")
	add("b")
	get := func(names ...string) string {
		t.Helper()
		id, err := s.fillIdentity(1, 0, names)
		if err != nil {
			t.Fatal(err)
		}
		return id
	}
	first := get("a", "b")
	if first != get("b", "a") {
		t.Fatal("name ordering changed identity")
	}
	if first == get("a", "c") {
		t.Fatal("selection was not bound")
	}
	s.o.windows[0].Until++
	if first == get("a", "b") {
		t.Fatal("window was not bound")
	}
	s.o.windows[0].Until--
	s.frontier = &syncFrontier{key: "generation one"}
	generation := get("a", "b")
	s.frontier.key = "generation two"
	if generation == get("a", "b") {
		t.Fatal("generation was not bound")
	}
	s.frontier = nil
	if first != get("a", "b") {
		t.Fatal("stable observation did not resume")
	}
	add("a")
	if first == get("a", "b") {
		t.Fatal("new observation reused old import")
	}
}

func TestSyncFillChildJournalBindsObservation(t *testing.T) {
	t.Parallel()
	f := newFakeSync()
	for _, host := range []string{syncT, syncD1} {
		f.put(host, "a", []syncArchive{{3600, 240}})
	}
	j, s := checkpointFixture(t)
	defer j.close()
	s.b = f
	s.o.now = func() time.Time { return time.Unix(syncTestNow, 0) }
	record := func() {
		t.Helper()
		if err := j.append(syncRow{Kind: "verified", Round: 1, Holes: []syncHole{{Name: "a", Source: syncD1, First: 1, Last: 2, Points: 1}}}); err != nil {
			t.Fatal(err)
		}
	}
	record()
	if _, err := s.copyFrom(context.Background(), "fill", 1, 0, []string{"a"}); err != nil {
		t.Fatal(err)
	}
	files := func() []string {
		t.Helper()
		paths, err := filepath.Glob(filepath.Join(s.o.state, "fill-*.names"))
		if err != nil {
			t.Fatal(err)
		}
		return paths
	}
	first := files()
	if len(first) != 1 {
		t.Fatal(first)
	}
	before, err := os.ReadFile(first[0])
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.copyFrom(context.Background(), "fill", 1, 0, []string{"a"}); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(first, files()) {
		t.Fatal("same observation lost partial child journal")
	}
	record()
	if _, err := s.copyFrom(context.Background(), "fill", 1, 0, []string{"a"}); err != nil {
		t.Fatal(err)
	}
	if len(files()) != 2 {
		t.Fatal("new observation reused child import", files())
	}
	after, err := os.ReadFile(first[0])
	if err != nil || string(after) != string(before) {
		t.Fatal("old names file changed", err)
	}
}
