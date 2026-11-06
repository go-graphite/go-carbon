package downsize

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/internal/chunkstore"
)

func maxDecisionFixture(t *testing.T) (string, string) {
	t.Helper()
	value := maximumSpeedDecisionFixtureValue()
	raw, _ := json.Marshal(value)
	path := filepath.Join(t.TempDir(), "decision.json")
	if err := os.WriteFile(path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(raw)
	return path, hex.EncodeToString(sum[:])
}

func TestMaximumSpeedBootstrapPreservesJournalAndFrozenBaselines(t *testing.T) {
	m, _, original := nativeMaxPolicyFixture(t)
	path, sum := maxDecisionFixture(t)
	t.Setenv("RESIZE_MAX_SPEED_DECISION", path)
	t.Setenv("RESIZE_MAX_SPEED_DECISION_SHA256", sum)
	t.Setenv("RESIZE_MAX_SPEED_WRITE_OK", filepath.Join(m.planData.root, "WRITE-OK"))
	t.Setenv("RESIZE_MAX_SPEED_WRITE_OK_SHA256", original.ApprovalSHA256)
	saved := &admissionState{Policy: defaultBacklogPolicy(), Hosts: map[string]hostAdmission{}}
	for host := range m.tuner.hosts {
		saved.Hosts[host] = hostAdmission{Baseline: &backlogBaseline{Max: 123}, DebtBaseline: &backlogBaseline{Max: 456}, LatencyBaseline: &backlogBaseline{Max: 789}, ReadBaseline: 22.7, MaxSpeed: &maxSpeedHostState{ReadPaused: true, ReadHighSince: 100, WriteQuietUntil: 300}}
	}
	if err := m.journal.saveAdmission(saved); err != nil {
		t.Fatal(err)
	}
	operation := m.journal.operation
	for range 2 {
		enabled, err := m.configureMaximumSpeed(context.Background(), nil)
		if err != nil || !enabled {
			t.Fatal(enabled, err)
		}
	}
	if m.journal.operation != operation || !m.journal.maxSpeed || m.journal.maxSpeedDecisionSHA != sum {
		t.Fatal("journal identity/authority changed")
	}
	for host, h := range m.tuner.hosts {
		old := saved.Hosts[host]
		if h.baseline.Max != old.Baseline.Max || h.debtBaseline.Max != old.DebtBaseline.Max || h.readBaseline != old.ReadBaseline || !h.maxSpeed.ReadPaused || h.maxSpeed.WriteQuietUntil != 300 {
			t.Fatal("lost frozen normal or timer", host)
		}
	}
	if err := m.journal.appendRow(journalRow{Operation: operation, Plan: m.journal.plan, SkippedIntegrity: &integritySkip{Name: "bad", Reason: "digest mismatch"}}); err != nil {
		t.Fatal(err)
	}
	if err := m.journal.compact(); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(m.journal.path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(raw, []byte(sum)) || !bytes.Contains(raw, []byte("digest mismatch")) || !bytes.Contains(raw, []byte("ReadPaused")) {
		t.Fatal("compaction lost authority/skip/timers")
	}
	t.Setenv("RESIZE_MAX_SPEED_WRITE_OK_SHA256", strings.Repeat("0", 64))
	if _, err := m.configureMaximumSpeed(context.Background(), nil); err == nil {
		t.Fatal("wrong original approval SHA accepted")
	}
}

func TestMaximumSpeedSourcePauseCannotStarveOtherSources(t *testing.T) {
	copied := make(chan string, 20)
	dst := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/transfer/pull":
			var req struct {
				Source string
				Names  []string
			}
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				t.Error(err)
				return
			}
			var proofs []chunkstore.TransferProof
			for _, name := range req.Names {
				proofs = append(proofs, chunkstore.TransferProof{Source: chunkstore.TransferVersion{Name: name}, Target: chunkstore.TransferVersion{Name: name}, Digest: "fixture"})
				copied <- name
			}
			json.NewEncoder(w).Encode(proofs)
		case "/v1/transfer/verify":
			w.Write([]byte(`{"verified":1}`))
		default:
			t.Errorf("unexpected mutation %s", r.URL.Path)
			http.Error(w, "bad", 400)
		}
	}))
	defer dst.Close()
	root := t.TempDir()
	p := &manifest{root: root, PlanID: strings.Repeat("a", 64)}
	for sourceIndex, source := range []string{"http://source-a:4242", "http://source-b:4242"} {
		var data bytes.Buffer
		encoder := json.NewEncoder(&data)
		encoder.Encode(planRow{Type: "header", Format: "go-carbon-ring", Version: 1, PlanID: p.PlanID, Source: source, Destination: "dst"})
		for i := range 20 {
			encoder.Encode(planRow{Type: "move", Metric: fmt.Sprintf("source%d.name.%d", sourceIndex, i), EstimatedBytes: 1024})
		}
		encoder.Encode(planRow{Type: "end", Metrics: 20, EstimatedBytes: 20480})
		sum := sha256.Sum256(data.Bytes())
		name := fmt.Sprintf("%x.jsonl", sum)
		if err := os.WriteFile(filepath.Join(root, name), data.Bytes(), 0600); err != nil {
			t.Fatal(err)
		}
		p.Streams = append(p.Streams, route{Path: name, Source: source, Destination: "dst", sourceURL: source, targetURL: dst.URL, Metrics: 20, EstimatedBytes: 20480, SHA256: fmt.Sprintf("%x", sum)})
	}
	j, err := openJournal(filepath.Join(root, "state"), p.PlanID)
	if err != nil {
		t.Fatal(err)
	}
	defer j.close()
	m := &mover{options: options{streams: 4, tick: time.Second}, planData: p, journal: j, client: dst.Client(), tuner: newTuner([]string{"http://source-a:4242", "http://source-b:4242", dst.URL}, capacity{4, 1, 1 << 20, 1000}, true, 0, 2)}
	j.maxSpeed = true
	m.tuner.enableMaxSpeed([]string{dst.URL}, "decision")
	m.tuner.hosts["http://source-a:4242"].Streams = 0
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- m.runCopyWorkers(ctx, func(error) { cancel() }) }()
	select {
	case name := <-copied:
		if !strings.HasPrefix(name, "source1.") {
			t.Fatal("paused source copied", name)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("healthy source starved behind paused source")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("worker pools leaked on cancellation")
	}
}

func TestMaximumSpeedUpstream429ReleasesDestinationForOtherSources(t *testing.T) {
	copied := make(chan struct{}, 1)
	dst := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/transfer/verify" {
			_, _ = w.Write([]byte(`{"verified":1}`))
			return
		}
		var request struct {
			Source string
			Names  []string
		}
		_ = json.NewDecoder(r.Body).Decode(&request)
		if request.Source == "http://source-a:4242" {
			http.Error(w, "source export: 429 Too Many Requests", 429)
			return
		}
		var proofs []chunkstore.TransferProof
		for _, name := range request.Names {
			proofs = append(proofs, chunkstore.TransferProof{Source: chunkstore.TransferVersion{Name: name}, Target: chunkstore.TransferVersion{Name: name}, Digest: "fixture"})
		}
		_ = json.NewEncoder(w).Encode(proofs)
		select {
		case copied <- struct{}{}:
		default:
		}
	}))
	defer dst.Close()
	root := t.TempDir()
	p := &manifest{root: root, PlanID: strings.Repeat("a", 64)}
	for i, source := range []string{"http://source-a:4242", "http://source-b:4242"} {
		var data bytes.Buffer
		encoder := json.NewEncoder(&data)
		_ = encoder.Encode(planRow{Type: "header", Format: "go-carbon-ring", Version: 1, PlanID: p.PlanID, Source: source, Destination: "dst"})
		_ = encoder.Encode(planRow{Type: "move", Metric: fmt.Sprintf("source%d.metric", i), EstimatedBytes: 1024})
		_ = encoder.Encode(planRow{Type: "end", Metrics: 1, EstimatedBytes: 1024})
		sum := sha256.Sum256(data.Bytes())
		path := fmt.Sprintf("%x.jsonl", sum)
		if err := os.WriteFile(filepath.Join(root, path), data.Bytes(), 0600); err != nil {
			t.Fatal(err)
		}
		p.Streams = append(p.Streams, route{Path: path, Source: source, Destination: "dst", sourceURL: source, targetURL: dst.URL, Metrics: 1, EstimatedBytes: 1024, SHA256: fmt.Sprintf("%x", sum)})
	}
	j, err := openJournal(filepath.Join(root, "state"), p.PlanID)
	if err != nil {
		t.Fatal(err)
	}
	defer j.close()
	j.maxSpeed = true
	m := &mover{options: options{streams: 2, tick: time.Millisecond}, planData: p, journal: j, client: dst.Client(), tuner: newTuner([]string{"http://source-a:4242", "http://source-b:4242", dst.URL}, capacity{1, 1, 1 << 20, 100}, true, 0, 2)}
	m.tuner.enableMaxSpeed([]string{dst.URL}, "decision")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- m.runCopyWorkers(ctx, func(error) { cancel() }) }()
	select {
	case <-copied:
	case <-time.After(time.Second):
		t.Fatal("source429 retained sole destination admission and starved healthy source")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("429 retry leaked workers on cancellation")
	}
}

func TestMaximumSpeedSourceProof429BacksOffSourceOnly(t *testing.T) {
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(429) }))
	defer source.Close()
	m := &mover{client: source.Client(), tuner: newTuner([]string{source.URL, "http://target:4242"}, capacity{16, 32, 1 << 20, 100}, false, 0, 2)}
	m.tuner.enableMaxSpeed([]string{"http://target:4242"}, "decision")
	response, err := m.sourceAbsenceRequest(context.Background(), http.MethodHead, source.URL+"/metrics/name")
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if m.tuner.hosts[source.URL].Streams != 8 || m.tuner.hosts["http://target:4242"].Streams != 16 {
		t.Fatal("proof429 throttled wrong endpoint")
	}
}
