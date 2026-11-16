package downsize

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"
)

type inspectFillReplay struct {
	*fakeSync
	inspect func(string)
}

func (b inspectFillReplay) Copy(_ context.Context, source, _ string, _ []string, _ string) (syncCopyResult, error) {
	b.inspect(source)
	return syncCopyResult{}, context.Canceled
}

// Resume a completed donor's fill, then block before another donor does new
// work. Replayed receipts must advance Done without creating a throughput burst.
func TestSyncFillReplayDoesNotAdvertiseFreshWork(t *testing.T) {
	t.Parallel()
	f := newFakeSync()
	o := convergeOptions(t)
	o.from = []string{syncD1, syncD2}
	schema := []syncArchive{{60, 10000}}
	ts := int64(syncTestNow - 6000)
	f.put(syncD1, "first", schema)
	f.put(syncD2, "second", schema)
	f.put(syncT, "first", schema, ts)
	f.put(syncT, "second", schema, ts)
	j, err := openSyncJournal(filepath.Join(o.state, "sync.jsonl"), "replay-rate")
	if err != nil {
		t.Fatal(err)
	}
	defer j.close()
	s := &syncer{o: o, j: j, started: time.Now(), copied: map[string]bool{}, forbid: map[string]bool{}}
	s.sum.phases = map[string]*syncPhase{}
	inspected := false
	s.b = inspectFillReplay{fakeSync: f, inspect: func(source string) {
		inspected = true
		if source != syncD2 {
			t.Fatalf("replayed donor was recopied: %s", source)
		}
		if done, replayed := s.prog.done.Load(), s.prog.replayed.Load(); done != 1 || replayed != 1 {
			t.Fatalf("replay inflated live work: done=%d replayed=%d", done, replayed)
		}
	}}
	if err := s.discover(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := s.verifyRound(context.Background(), 1, s.unionSource()); err != nil {
		t.Fatal(err)
	}
	identity, err := s.fillIdentity(1, 0, []string{"first"})
	if err != nil {
		t.Fatal(err)
	}
	if err := j.append(syncRow{Kind: "copied", Identity: identity, Phase: "fill", Round: 1, Source: syncD1, Names: 1, Copied: 1}); err != nil {
		t.Fatal(err)
	}
	if err := s.verifyAndFill(context.Background()); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if !inspected {
		t.Fatal("never reached fresh donor")
	}
}
