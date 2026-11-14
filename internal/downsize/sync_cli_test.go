package downsize

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func syncCLIArgs(t *testing.T, extra ...string) []string {
	t.Helper()
	return append([]string{"--source", syncD1, "--to", syncT, "--state", filepath.Join(t.TempDir(), "state")}, extra...)
}

func TestSyncCommandModesAndFlags(t *testing.T) {
	tests := []struct {
		name string
		args []string
		want string
	}{
		{"default", nil, ""},
		{"windows", []string{"--window", "closed:100:200", "--window", "dump:300:400"}, ""},
		{"full", []string{"--window", "100:200", "--full-history", "--max-batch-bytes", "65536"}, ""},
		{"native bytes", []string{"--window", "100:200", "--max-batch-bytes", "65536"}, "--max-batch-bytes has no effect"},
		{"native streams", []string{"--window", "100:200", "--copy-streams", "0"}, "--copy-streams has no effect"},
		{"native attempts", []string{"--window", "100:200", "--copy-attempts", "2"}, "--copy-attempts has no effect"},
		{"independent budget", []string{"--streams", "8", "--copy-streams", "2"}, "no longer sets an independent pool"},
		{"verify separate", []string{"--streams", "8", "--verify-streams", "16"}, "no longer sets an independent budget"},
		{"verify same", []string{"--streams", "8", "--verify-streams", "8"}, ""},
		{"same budget", []string{"--streams", "8", "--copy-streams", "8"}, ""},
		{"removed flag", []string{"--transport-retries", "2"}, "flag provided but not defined"},
		{"copy only", []string{"--phase", "copy"}, ""},
		{"copy window", []string{"--phase", "copy", "--window", "100:200"}, "--window has no effect"},
		{"copy sample", []string{"--phase", "copy", "--sample-every", "1"}, "--sample-every has no effect"},
		{"phase", []string{"--phase", "verify"}, "--phase must"},
		{"negative", []string{"--streams", "-1"}, "invalid streams"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c, err := parseSyncCommand(syncCLIArgs(t, tt.args...))
			if tt.want != "" {
				if err == nil || !strings.Contains(err.Error(), tt.want) {
					t.Fatalf("got %v, want %s", err, tt.want)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			p := c.policy(16)
			if p.Streams != 16 || p.VerifyStreams > 16 || p.CopyStreams > 16 {
				t.Fatalf("budget %+v", p)
			}
			if len(c.sync.windows) > 0 && !c.sync.fullHistory && (p.Scope != "windows-only" || p.Engine != "native-window" || p.CopyStreams != 1 || p.CopyBatchBytes != 0) {
				t.Fatalf("native %+v", p)
			}
			if c.sync.fullHistory && (p.Scope != "windows+full-history" || p.Engine != "metric") {
				t.Fatalf("full %+v", p)
			}
		})
	}
	if _, err := parseSyncCommand([]string{"--help"}); !errors.Is(err, flag.ErrHelp) {
		t.Fatal(err)
	}
}

func TestSyncSourceAliasAndNamesFormat(t *testing.T) {
	state := filepath.Join(t.TempDir(), "state")
	canonical := []string{"--source", syncD1 + "," + syncD2, "--to", syncT, "--state", state}
	compat := []string{"--from", syncD1, "--source", syncD2, "--to", syncT, "--state", state}
	a, err := parseSyncCommand(canonical)
	if err != nil {
		t.Fatal(err)
	}
	b, err := parseSyncCommand(compat)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(a.sync.from, b.sync.from) {
		t.Fatal(a.sync.from, b.sync.from)
	}
	names := filepath.Join(t.TempDir(), "names")
	for _, text := range []string{"[\"a\",\"b\"]\n", "b\na\n", "a\na\n"} {
		if err := os.WriteFile(names, []byte(text), 0600); err != nil {
			t.Fatal(err)
		}
		if _, err := parseSyncCommand(syncCLIArgs(t, "--names", names)); err == nil || !strings.Contains(err.Error(), "one metric name per line") {
			t.Fatalf("%q: %v", text, err)
		}
	}
	if err := os.WriteFile(names, []byte("a\nb\n"), 0600); err != nil {
		t.Fatal(err)
	}
	c, err := parseSyncCommand(syncCLIArgs(t, "--names", names))
	if err != nil {
		t.Fatal(err)
	}
	if c.sync.namesFile != names || c.sync.namesHash == "" {
		t.Fatal(c.sync)
	}
}

func TestSyncCopyAliasResumesLegacyJournal(t *testing.T) {
	f, src, dst := newFakeCopy(t, []string{"a", "b", "c"})
	o := copyTestOptions(t, src, dst)
	if err := planFreeCopy(context.Background(), o); err != nil {
		t.Fatal(err)
	}
	before := f.attempts
	args := []string{"--phase", "copy", "--source", src, "--to", dst, "--state", o.state, "--streams", "4"}
	if err := PlanFreeCopyMain(context.Background(), args); err != nil {
		t.Fatal(err)
	}
	args[2] = "--from"
	if err := SyncMain(context.Background(), args); err != nil {
		t.Fatal(err)
	}
	if f.attempts != before {
		t.Fatalf("reimported completed ranges: %d -> %d", before, f.attempts)
	}
	var snap progressSnapshot
	data, err := os.ReadFile(o.state + ".progress.json")
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(data, &snap); err != nil {
		t.Fatal(err)
	}
	if snap.Copied != 3 || snap.Status != "complete" || snap.Effective == nil || snap.Effective.Streams != 4 || snap.Effective.CopyBatchBytes != f.maxBody {
		t.Fatalf("snapshot %+v effective %+v", snap, snap.Effective)
	}
	c, err := parseSyncCommand([]string{"--source", src, "--to", dst, "--state", o.state})
	if err != nil {
		t.Fatal(err)
	}
	if !c.legacyJournal || c.sync.state != o.state+".sync" || c.phase != "all" {
		t.Fatalf("legacy transition %+v", c)
	}
}

// Several windows in one run must produce exactly the same target float bits
// as the retained single-window orchestration, including target corrections.
func TestSyncRepeatedWindowsDifferential2000(t *testing.T) {
	windows := syncRanges{{Name: "closed", From: syncTestNow - 600, Until: syncTestNow - 480}, {Name: "shard", From: syncTestNow - 360, Until: syncTestNow - 240}, {Name: "dump", From: syncTestNow - 120, Until: syncTestNow}}
	makeBackend := func() *fakeSync {
		b := newFakeSync()
		schema := []syncArchive{{Step: 60, Points: 10000}}
		for i := range 2000 {
			n := fmt.Sprintf("metric.%04d", i)
			for _, host := range []string{syncD1, syncT} {
				st := b.store(host)
				st.archives[n] = schema
				st.points[n] = map[int64]map[int64]float64{60: {}}
			}
			values := []float64{math.Copysign(0, -1), math.SmallestNonzeroFloat64, math.MaxFloat64}
			for j, w := range windows {
				b.store(syncD1).points[n][60][w.From] = values[(i+j)%len(values)]
				b.store(syncD1).points[n][60][w.From+60] = 17
				b.store(syncT).points[n][60][w.From+60] = 42 // target wins
			}
			b.store(syncD1).points[n][60][windows[0].From-60] = 99 // outside every window
		}
		return b
	}
	combined, separate := makeBackend(), makeBackend()
	base := syncOptions{to: syncT, from: []string{syncD1}, batch: 500, streams: 4, maxRounds: 3, now: func() time.Time { return time.Unix(syncTestNow, 0) }}
	o := base
	o.state = t.TempDir()
	o.windows = windows
	if err := runSync(context.Background(), o, combined); err != nil {
		t.Fatal(err)
	}
	before := len(combined.copies)
	if err := runSync(context.Background(), o, combined); err != nil {
		t.Fatal(err)
	}
	if len(combined.copies) != before {
		t.Fatal("resume repeated copies")
	}
	for _, w := range windows {
		o := base
		o.state = t.TempDir()
		o.windows = syncRanges{w}
		if err := runSync(context.Background(), o, separate); err != nil {
			t.Fatal(err)
		}
	}
	for n, steps := range combined.store(syncT).points {
		for step, pts := range steps {
			want := separate.store(syncT).points[n][step]
			if len(pts) != len(want) {
				t.Fatalf("%s count %d != %d", n, len(pts), len(want))
			}
			for ts, v := range pts {
				if math.Float64bits(v) != math.Float64bits(want[ts]) {
					t.Fatalf("%s %d bits changed", n, ts)
				}
			}
			if _, ok := pts[windows[0].From-60]; ok {
				t.Fatalf("%s copied outside windows", n)
			}
		}
	}
}

func TestSyncNamesDiscoveryStreamingAndResume(t *testing.T) {
	b := newFakeSync()
	for _, n := range []string{"a", "b", "c", "d", "e"} {
		b.store(syncD1).archives[n] = syncTestSchema
		b.store(syncT).archives[n] = syncTestSchema
	}
	file := filepath.Join(t.TempDir(), "names")
	if err := os.WriteFile(file, []byte("b\ne\n"), 0600); err != nil {
		t.Fatal(err)
	}
	hash, err := readSyncNames(file)
	if err != nil {
		t.Fatal(err)
	}
	o := syncOptions{to: syncT, from: []string{syncD1}, state: t.TempDir(), namesFile: file, namesHash: hash, batch: 2, streams: 2, maxRounds: 2, now: func() time.Time { return time.Unix(syncTestNow, 0) }}
	if err := runSync(context.Background(), o, b); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(o.state, "union.tsv"))
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "b\t1\t1\ne\t1\t1\n" {
		t.Fatalf("scope: %s", data)
	}
	if err := runSync(context.Background(), o, b); err != nil {
		t.Fatal(err)
	}
	o.namesHash = "changed"
	if err := runSync(context.Background(), o, b); err == nil {
		t.Fatal("changed scope resumed journal")
	}
}

func TestSyncInactiveSamplingWarns(t *testing.T) {
	for _, extra := range [][]string{{"--sample-every", "0"}, {"--window", "100:200", "--sample-every", "1"}, {"--full-history"}} {
		old := os.Stdout
		r, w, err := os.Pipe()
		if err != nil {
			t.Fatal(err)
		}
		os.Stdout = w
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		runErr := SyncMain(ctx, syncCLIArgs(t, append(extra, "--streams", "1")...))
		os.Stdout = old
		_ = w.Close()
		var events []map[string]any
		dec := json.NewDecoder(r)
		for {
			var e map[string]any
			if dec.Decode(&e) != nil {
				break
			}
			events = append(events, e)
		}
		_ = r.Close()
		if runErr == nil {
			t.Fatal("cancelled run unexpectedly succeeded")
		}
		found := false
		for _, e := range events {
			if e["event"] == "inactive_flag" && e["level"] == "warn" {
				found = true
			}
		}
		if !found {
			t.Fatalf("missing loud warning: %v", events)
		}
	}
}

func TestSyncAliasEndToEndAndLegacyTransition(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/transfer/status":
			_ = json.NewEncoder(w).Encode(copyStatus{MaxTransfers: 8, MaxBodyBytes: 1 << 20})
		case "/v1/transfer/list":
			fmt.Fprintln(w, "[]")
		default:
			http.NotFound(w, r)
		}
	})
	source, target := httptest.NewServer(handler), httptest.NewServer(handler)
	defer source.Close()
	defer target.Close()
	for _, legacy := range []bool{false, true} {
		t.Run(fmt.Sprint(legacy), func(t *testing.T) {
			state := filepath.Join(t.TempDir(), "state")
			if legacy {
				state += ".jsonl"
				o := copyTestOptions(t, source.URL, target.URL)
				o.state = state
				if err := planFreeCopy(context.Background(), o); err != nil {
					t.Fatal(err)
				}
			}
			args := []string{"--source", source.URL, "--to", target.URL, "--state", state}
			if err := Main(append([]string{"copy"}, args...)); err != nil {
				t.Fatal(err)
			}
			if err := Main(append([]string{"sync"}, args...)); err != nil {
				t.Fatal(err)
			}
			if legacy {
				state += ".sync"
			}
			data, err := os.ReadFile(filepath.Join(state, "progress.json"))
			if err != nil {
				t.Fatal(err)
			}
			var p progressSnapshot
			if err := json.Unmarshal(data, &p); err != nil {
				t.Fatal(err)
			}
			if p.Status != "complete" || p.Effective == nil || p.Effective.Phase != "all" || p.Effective.Streams != 8 {
				t.Fatalf("%s", data)
			}
		})
	}
}
