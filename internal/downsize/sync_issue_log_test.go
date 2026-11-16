package downsize

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestSyncIssueCrashRecovery(t *testing.T) {
	if state := os.Getenv("SYNC_ISSUE_CRASH_STATE"); state != "" {
		q, err := openSyncIssues(state)
		if err != nil {
			panic(err)
		}
		for i := 0; i < 100; i++ {
			if err = q.record([]syncResidual{{syncHole: syncHole{Name: fmt.Sprint(i)}, Reason: "failure"}}, "copy", i%2 == 0); err != nil {
				panic(err)
			}
		}
		os.Exit(23) // Deliberately bypass checkpoints and all deferred cleanup.
	}
	state := t.TempDir()
	cmd := exec.Command(os.Args[0], "-test.run=^TestSyncIssueCrashRecovery$")
	cmd.Env = append(os.Environ(), "SYNC_ISSUE_CRASH_STATE="+state)
	if err := cmd.Run(); err == nil {
		t.Fatal("child did not crash")
	}
	q, err := openSyncIssues(state)
	if err != nil {
		t.Fatal(err)
	}
	if a, b := q.counts(); a != 50 || b != 50 {
		t.Fatal(a, b)
	}
	// Crash during a later append must retain every acknowledged transaction.
	log := filepath.Join(state, "policy-skipped.json.jsonl")
	data, err := os.ReadFile(log)
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(log, append(data, []byte(`[{"name":"torn`)...), 0644); err != nil {
		t.Fatal(err)
	}
	q, err = openSyncIssues(state)
	if err != nil {
		t.Fatal(err)
	}
	if a, b := q.counts(); a != 50 || b != 50 {
		t.Fatal(a, b)
	}
	before := q.policy
	if err = q.checkpoint(); err != nil {
		t.Fatal(err)
	}
	// Crash after snapshot rename but before journal deletion: replay duplicates.
	if err = os.WriteFile(log, data, 0644); err != nil {
		t.Fatal(err)
	}
	q, err = openSyncIssues(state)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(before, q.policy) {
		t.Fatal("snapshot/journal replay changed exceptions")
	}
	if err = q.record([]syncResidual{{syncHole: syncHole{Name: "later"}, Reason: "failure"}}, "verify", true); err != nil {
		t.Fatal(err)
	}
	q, err = openSyncIssues(state)
	if err != nil || !q.excluded("later") {
		t.Fatal(err)
	}
	if err = os.WriteFile(log, []byte("corrupt\n"), 0644); err != nil {
		t.Fatal(err)
	}
	if _, err = openSyncIssues(state); err == nil {
		t.Fatal("complete corrupt record accepted")
	}
}

func TestSyncIssueHundredThousand(t *testing.T) {
	if os.Getenv("SYNC_ISSUE_PERF") == "" {
		t.Skip("opt-in disk persistence comparison")
	}
	// Keep all 100k production-sized names, measuring actual journal/snapshot
	// bytes. The old algorithm's exact JSON byte count is accumulated without
	// issuing tens of terabytes of deliberately redundant disk writes.
	state := t.TempDir()
	q, err := openSyncIssues(state)
	if err != nil {
		t.Fatal(err)
	}
	sink, err := os.OpenFile(os.DevNull, os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	saved := os.Stdout
	os.Stdout = sink
	defer func() { os.Stdout = saved; sink.Close() }()
	start := time.Now()
	var oldBytes int64
	var cumulative int64
	for i := 0; i < 100000; i++ {
		name := fmt.Sprintf("%06d.%s", i, strings.Repeat("x", 4097))
		if err = q.record([]syncResidual{{syncHole: syncHole{Name: name}, Reason: "policy"}}, "discover", true); err != nil {
			t.Fatal(err)
		}
		row := q.policy[name]
		data, e := json.Marshal(row)
		if e != nil {
			t.Fatal(e)
		}
		cumulative += int64(len(data))
		oldBytes += cumulative + int64(i) + 3
	}
	log := filepath.Join(state, "policy-skipped.json.jsonl")
	st, err := os.Stat(log)
	if err != nil {
		t.Fatal(err)
	}
	journalBytes := st.Size()
	reopened, err := openSyncIssues(state)
	if err != nil {
		t.Fatal(err)
	}
	if a, b := reopened.counts(); a != 0 || b != 100000 {
		t.Fatal(a, b)
	}
	if err = reopened.checkpoint(); err != nil {
		t.Fatal(err)
	}
	st, err = os.Stat(filepath.Join(state, "policy-skipped.json"))
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("100000 names: wall=%s journal_bytes=%d snapshot_bytes=%d total_bytes=%d old_exact_json_bytes=%d", time.Since(start), journalBytes, st.Size(), journalBytes+st.Size(), oldBytes)
	if journalBytes+st.Size() > 100000*10000 {
		t.Fatal("superlinear persistence")
	}
}
