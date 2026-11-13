package downsize

import (
	"bufio"
	"context"
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestProgressMetricsWireAndSyncPhases(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	lines := make(chan string, 30)
	go func() {
		for {
			c, e := listener.Accept()
			if e != nil {
				return
			}
			s := bufio.NewScanner(c)
			for s.Scan() {
				lines <- s.Text()
			}
			c.Close()
		}
	}()
	p := &progressMetrics{addr: listener.Addr().String(), prefix: "carbon.store_copy.runner.run.sync", phase: "starting", active: 1}
	for index, phase := range []string{"copy", "fill", "verify round 2"} {
		p.observe("state/progress.json", progressSnapshot{Phase: phase, Done: 12, Total: 20, Rate: 3.5, Status: "running"})
		// Internal copy progress must not replace parent sync phase or totals.
		p.observe("nested.progress.json", progressSnapshot{Done: 999, Total: 999})
		p.send()
		got := map[string]string{}
		count := 5
		if index > 0 {
			count += 2
		}
		for i := 0; i < count; i++ {
			select {
			case line := <-lines:
				parts := strings.Fields(line)
				got[parts[0]] = parts[1]
			case <-time.After(time.Second):
				t.Fatal("missing Graphite sample")
			}
		}
		prefix := p.prefix + "." + metricToken(phase) + "."
		if got[prefix+"done"] != "12" || got[prefix+"total"] != "20" || got[prefix+"names_per_second"] != "3.5" || got[prefix+"active"] != "1" {
			t.Fatal(got)
		}
	}
	p.observe("state/progress.json", progressSnapshot{Phase: "verify round 2", Status: "incomplete"})
	for i := 0; i < 5; i++ {
		select {
		case line := <-lines:
			if strings.Contains(line, ".active ") && !strings.Contains(line, ".active 0 ") {
				t.Fatal(line)
			}
		case <-time.After(time.Second):
			t.Fatal("missing final sample")
		}
	}
}

func TestProgressMetricsTokens(t *testing.T) {
	if got := metricToken("runner.prod\nother path"); got != "runner_prod_other_path" {
		t.Fatal(got)
	}
}

func TestProgressMetricsUsesProgressFileSnapshot(t *testing.T) {
	path := filepath.Join(t.TempDir(), "progress.json")
	var observed progressSnapshot
	outputMu.Lock()
	progressObserver = func(gotPath string, p progressSnapshot) {
		if gotPath != path {
			t.Errorf("path = %s", gotPath)
		}
		observed = p
	}
	outputMu.Unlock()
	defer func() { outputMu.Lock(); progressObserver = nil; outputMu.Unlock() }()
	stop := startProgressFile(context.Background(), path, func() progressSnapshot {
		return progressSnapshot{Phase: "discover", Done: 15, Total: 30, Discovered: 15}
	}, nil)
	if err := stop(context.Canceled); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var stored progressSnapshot
	if err := json.Unmarshal(data, &stored); err != nil {
		t.Fatal(err)
	}
	if !stored.Updated.Equal(observed.Updated) {
		t.Fatal("snapshot timestamps differ")
	}
	observed.Updated = stored.Updated
	if !reflect.DeepEqual(stored, observed) {
		t.Fatalf("JSON %+v differs from Graphite snapshot %+v", stored, observed)
	}
	if stored.Status != "interrupted" || stored.Done != 15 || stored.Total != 30 {
		t.Fatal(stored)
	}
}

func TestProgressMetricsSourceBatchPreservesControllerPhase(t *testing.T) {
	dir := t.TempDir()
	j, err := openSyncJournal(filepath.Join(dir, "sync.jsonl"), "phase")
	if err != nil {
		t.Fatal(err)
	}
	defer j.close()
	f := newFakeSync()
	f.forbidden[syncD1] = true
	s := &syncer{o: syncOptions{state: dir, to: syncT, from: []string{syncD1}}, b: f, j: j, forbid: map[string]bool{}}
	s.prog.begin("window retry", 1000)
	s.prog.done.Store(150)
	if _, err := s.copyFrom(context.Background(), "window-retry-immutable-batch", 1, 0, []string{"metric"}); err != nil {
		t.Fatal(err)
	}
	if s.prog.phase != "window retry" || s.prog.total != 1000 || s.prog.done.Load() != 150 {
		t.Fatalf("batch replaced controller progress: %s %d/%d", s.prog.phase, s.prog.done.Load(), s.prog.total)
	}
}
