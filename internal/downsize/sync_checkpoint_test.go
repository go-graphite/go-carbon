package downsize

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func checkpointFixture(t *testing.T) (*syncJournal, *syncer) {
	t.Helper()
	dir := t.TempDir()
	o := syncOptions{state: dir, from: []string{syncD1}, to: syncT, now: time.Now}
	identity, err := syncIdentity(o)
	if err != nil {
		t.Fatal(err)
	}
	j, err := openSyncJournal(filepath.Join(dir, "sync.jsonl"), identity)
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"union.tsv", "missing.tsv"} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte("a\t1\t1\nb\t1\t1\n"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	s := &syncer{o: o, j: j, copied: map[string]bool{}}
	s.sum.Discovered = 2
	return j, s
}

func TestSyncCheckpointResumesVerificationAndTail(t *testing.T) {
	t.Parallel()
	j, s := checkpointFixture(t)
	if err := j.append(syncRow{Kind: "window-selected", Round: 1, Selected: []string{strings.Repeat("old-selection-payload", 10000)}}); err != nil {
		t.Fatal(err)
	}
	first := syncRow{Kind: "verified", Round: 1, First: "a", Last: "a", Names: 1}
	if err := j.append(first); err != nil {
		t.Fatal(err)
	}
	s.startVerificationCheckpoint()
	if j.checkpoint == nil {
		t.Fatal("phase boundary not checkpointed")
	}
	// Model a kill after a durable row and a torn append, without running close's
	// optional cache refresh. The old checkpoint plus the tail must suffice.
	if err := j.append(syncRow{Kind: "verified", Round: 1, First: "b", Last: "b", Names: 1}); err != nil {
		t.Fatal(err)
	}
	if _, err := j.file.WriteString(`{"kind":"verified"`); err != nil {
		t.Fatal(err)
	}
	path, identity := j.file.Name(), j.checkpoint.Identity
	j.file.Close()
	j.lock.Close()
	resumed, err := openSyncJournal(path, identity)
	if err != nil {
		t.Fatal(err)
	}
	defer resumed.close()
	if resumed.checkpoint == nil || len(resumed.rows) != 3 || resumed.progress.Verified != 2 {
		t.Fatalf("checkpoint/tail lost progress: cache=%v rows=%d progress=%+v", resumed.checkpoint != nil, len(resumed.rows), resumed.progress)
	}
	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if strings.HasSuffix(string(after), `{"kind":"verified"`) {
		t.Fatal("torn tail retained")
	}
	// The original audit prefix is still present; only the loaded resume state
	// discarded the completed selection payload.
	if !strings.Contains(string(after), "old-selection-payload") {
		t.Fatal("audit journal rewritten")
	}
}

func TestSyncCheckpointInvalidationFallsBackToJournal(t *testing.T) {
	t.Parallel()
	for _, change := range []string{"catalog", "corrupt cache", "journal replacement", "header", "tail"} {
		t.Run(change, func(t *testing.T) {
			j, s := checkpointFixture(t)
			if err := j.append(syncRow{Kind: "discovered", Names: 2}); err != nil {
				t.Fatal(err)
			}
			s.startVerificationCheckpoint()
			path, identity := j.file.Name(), j.checkpoint.Identity
			j.close()
			switch change {
			case "catalog":
				if err := os.WriteFile(s.unionPath(), []byte("different catalog"), 0600); err != nil {
					t.Fatal(err)
				}
			case "corrupt cache":
				if err := os.WriteFile(path+".checkpoint.json", []byte(`{"SHA256":"bad","Payload":{}}`), 0600); err != nil {
					t.Fatal(err)
				}
			case "journal replacement":
				data, err := os.ReadFile(path)
				if err != nil {
					t.Fatal(err)
				}
				if err = os.WriteFile(path+".new", data, 0600); err != nil {
					t.Fatal(err)
				}
				if err = os.Rename(path+".new", path); err != nil {
					t.Fatal(err)
				}
			case "header":
				data, err := os.ReadFile(path)
				if err != nil {
					t.Fatal(err)
				}
				data = []byte(strings.Replace(string(data), "http://target", "http://others", 1))
				if err = os.WriteFile(path, data, 0600); err != nil {
					t.Fatal(err)
				}
			case "tail":
				f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0)
				if err != nil {
					t.Fatal(err)
				}
				_, err = f.WriteString("bad complete row\n")
				f.Close()
				if err != nil {
					t.Fatal(err)
				}
			}
			got, err := openSyncJournal(path, identity)
			if change == "header" || change == "tail" {
				if err == nil {
					got.close()
					t.Fatal("invalid journal accepted")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			defer got.close()
			if got.checkpoint != nil || len(got.rows) != 2 {
				t.Fatalf("did not replay intact journal: cache=%v rows=%d", got.checkpoint != nil, len(got.rows))
			}
		})
	}
}

type checkpointNoDiscovery struct{ *fakeSync }

func (b checkpointNoDiscovery) List(context.Context, string, string, string, int) ([]string, error) {
	return nil, errors.New("checkpoint repeated discovery")
}

func TestSyncCheckpointInterruptedRunKeepsCopies(t *testing.T) {
	t.Parallel()
	f, o, _ := threeSources(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	f.cancel, f.failAfter = cancel, 6
	if err := runSync(ctx, o, f); err == nil {
		t.Fatal("expected interruption")
	}
	path := filepath.Join(o.state, "sync.jsonl.checkpoint.json")
	if _, err := os.Stat(path); err != nil {
		t.Fatal("no checkpoint", err)
	}
	copies := len(f.copies)
	f.failAfter = 0
	f.fetches.Store(0)
	if err := runSync(context.Background(), o, checkpointNoDiscovery{f}); err != nil {
		t.Fatal(err)
	}
	for _, call := range f.copies[copies:] {
		if strings.Contains(call, "d.missing") || strings.Contains(call, "e.peeronly") {
			t.Fatal("repeated completed copy", call)
		}
	}
}

func TestSyncCheckpointPreservesMutationChecks(t *testing.T) {
	t.Parallel()
	f, o := frontierFixture(t)
	// Begin clean so this test isolates the checkpoint from legacy fill-receipt
	// reuse across completed verification generations.
	f.store(syncT).points["metric.19"][3600][syncTestNow-5*3600] = 1
	o.noCopy = false
	o.maxRounds = 4
	if err := runSync(context.Background(), o, f); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(o.state, "sync.jsonl.checkpoint.json")); err != nil {
		t.Fatal(err)
	}
	ts := int64(syncTestNow - 5*3600)
	delete(f.store(syncT).points["metric.02"][3600], ts)
	if err := runSync(context.Background(), o, f); err != nil {
		t.Fatal(err)
	}
	if _, ok := f.store(syncT).points["metric.02"][3600][ts]; !ok {
		t.Fatal("checkpoint bypassed mutation validation")
	}
}

// Explicit remote capacity check; keep normal unit tests small.
func TestSyncCheckpointScale160Million(t *testing.T) {
	if os.Getenv("STORECTL_CHECKPOINT_SCALE") != "1" {
		t.Skip("explicit capacity check")
	}
	j, s := checkpointFixture(t)
	const names = 160000000
	s.sum.Discovered = names
	j.progress.Verified = names
	// Model a fully clean first pass, at the default 500-name batch size.
	for id := 0; id < names; id += 500 {
		j.rows = append(j.rows, syncRow{Kind: "verified", Round: 1, First: fmt.Sprintf("metric.%09d", id), Last: fmt.Sprintf("metric.%09d", id+499), Names: 500})
	}
	// The audit prefix may contain large, already-completed selection payloads;
	// its content is independent of the compact verification record count.
	s.startVerificationCheckpoint()
	p := j.file.Name() + ".checkpoint.json"
	st, err := os.Stat(p)
	if err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	cp, err := readSyncCheckpoint(j.file, j.checkpoint.Identity)
	if err != nil {
		t.Fatal(err)
	}
	if cp == nil || len(cp.Rows) != names/500+1 {
		t.Fatal("scale state incomplete")
	}
	t.Logf("catalog_names=%d checkpoint_bytes=%d load=%s rows=%d", names, st.Size(), time.Since(started), len(cp.Rows))
	j.file.Close()
	j.lock.Close()
}

func TestSyncCheckpointOptionChangesUseJournal(t *testing.T) {
	j, s := checkpointFixture(t)
	s.startVerificationCheckpoint()
	path, identity := j.file.Name(), j.checkpoint.Identity
	j.close()
	s.o.noCopy = true
	got, err := openSyncJournal(path, identity, syncCheckpointOptions(s.o))
	if err != nil {
		t.Fatal(err)
	}
	defer got.close()
	if got.checkpoint != nil {
		t.Fatal("changed copy options reused phase boundary")
	}
}

func TestSyncCheckpointDoesNotSealFailedCopies(t *testing.T) {
	j, s := checkpointFixture(t)
	defer j.close()
	s.sum.residual = []syncResidual{{syncHole: syncHole{Name: "a"}, Reason: "copy_failed"}}
	s.startVerificationCheckpoint()
	if j.checkpoint != nil {
		t.Fatal("failed copy hidden behind phase checkpoint")
	}
}
