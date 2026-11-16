package downsize

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"reflect"
	"sync/atomic"
	"testing"
	"time"
)

func TestWindowProofPresenceCompatibility(t *testing.T) {
	for _, tc := range []struct {
		name   string
		found  any
		proof  string
		absent bool
	}{
		{"absent", false, "sealed", true}, {"present", true, "sealed", false},
		{"old response", nil, "sealed", false}, {"unknown ownership", false, "", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				row := map[string]any{"Name": "metric", "Mode": "proof-v2", "Proof": tc.proof}
				if tc.found != nil {
					row["Found"] = tc.found
				}
				_ = json.NewEncoder(w).Encode([]any{row})
			}))
			defer server.Close()
			b := newSyncLiveBackend(syncLiveOptions{hostWait: time.Second})
			proofs, absent, err := b.WindowProofsPresence(context.Background(), server.URL, []string{"metric"}, 1, 2)
			if err != nil || len(proofs) != 1 || len(absent) != 1 || absent[0] != tc.absent {
				t.Fatalf("%v %v %v", proofs, absent, err)
			}
		})
	}
}

// Matches a captured 500-name batch with sixteen holders: almost every donor
// lacks the batch. Compare the exact all-holder and donor seals with the old
// path, including unknown ownership and a name present on only one holder.
type presenceFixture struct {
	syncBackend
	infoNames  atomic.Int64
	proofNames atomic.Int64
}

func (f *presenceFixture) Info(_ context.Context, host string, names []string) (map[string][]syncArchive, error) {
	f.infoNames.Add(int64(len(names)))
	out := map[string][]syncArchive{}
	for _, n := range names {
		if host == syncT || host == syncD1 || n == "metric.0000" || n == "metric.0001" {
			out[n] = []syncArchive{{60, 10080}}
		}
	}
	return out, nil
}
func (f *presenceFixture) Proofs(context.Context, string, []string) ([]string, error) {
	return nil, errSyncNoProof
}
func (f *presenceFixture) WindowProofs(ctx context.Context, host string, names []string, from, until int64) ([]string, error) {
	p, _, e := f.WindowProofsPresence(ctx, host, names, from, until)
	return p, e
}
func (f *presenceFixture) WindowProofsPresence(_ context.Context, host string, names []string, from, until int64) ([]string, []bool, error) {
	f.proofNames.Add(int64(len(names)))
	p := make([]string, len(names))
	absent := make([]bool, len(names))
	for i, n := range names {
		if n == "metric.0001" {
			continue
		} // no absence inference for unknown ownership
		p[i] = fmt.Sprintf("%s/%s/%d/%d", host, n, from, until)
		absent[i] = host != syncT && host != syncD1 && n != "metric.0000"
	}
	return p, absent, nil
}

type presenceOracle struct{ syncBackend }

func (f presenceOracle) Proofs(ctx context.Context, h string, n []string) ([]string, error) {
	return f.syncBackend.(syncProofBackend).Proofs(ctx, h, n)
}
func (f presenceOracle) WindowProofs(ctx context.Context, h string, n []string, a, b int64) ([]string, error) {
	return f.syncBackend.(syncWindowProofBackend).WindowProofs(ctx, h, n, a, b)
}

func TestProofPresenceDifferential(t *testing.T) {
	names := make([]string, 500)
	for i := range names {
		names[i] = fmt.Sprintf("metric.%04d", i)
	}
	o := syncOptions{to: syncT, from: []string{syncD1}, now: func() time.Time { return time.Unix(syncTestNow, 0) }}
	for i := range 14 {
		o.from = append(o.from, fmt.Sprintf("http://donor-%d:4242", i))
	}
	f := &presenceFixture{}
	s := &syncer{o: o, b: presenceOracle{f}, frontier: &syncFrontier{key: "captured-scope"}}
	s.frontier.scope.Now = syncTestNow
	var oldDonors, newDonors []syncProof
	old, ov, err := s.proofStampsVersion(context.Background(), names, &oldDonors)
	if err != nil {
		t.Fatal(err)
	}
	baseline := f.infoNames.Swap(0)
	s.b = f
	next, nv, err := s.proofStampsVersion(context.Background(), names, &newDonors)
	if err != nil {
		t.Fatal(err)
	}
	if ov != nv || !reflect.DeepEqual(old, next) || !reflect.DeepEqual(oldDonors, newDonors) {
		t.Fatal("presence filtering changed proof bytes")
	}
	if got := f.infoNames.Load(); got != 1013 {
		t.Fatalf("queried %d schema names, want 1013; old=%d", got, baseline)
	}
	t.Logf("exact proof parity: schema names %d -> %d", baseline, f.infoNames.Load())
}

// Opt-in network benchmark. Both paths adopt identical completed observations;
// the reference hides only the presence extension and exercises the original
// all-holder Info/404 recursion. No store data or journal is modified by hand.
func TestAdoptionPresenceThroughput(t *testing.T) {
	if os.Getenv("STORECTL_ADOPT_BENCH") == "" {
		t.Skip("set STORECTL_ADOPT_BENCH for network throughput comparison")
	}
	var infos atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/info/":
			infos.Add(1)
			http.NotFound(w, r)
		case "/v1/transfer/digest":
			var req struct {
				Names []string
				Mode  string
			}
			_ = json.NewDecoder(r.Body).Decode(&req)
			rows := make([]map[string]any, len(req.Names))
			for i, n := range req.Names {
				rows[i] = map[string]any{"Name": n, "Mode": req.Mode, "Found": false, "Proof": "durable-absence"}
			}
			_ = json.NewEncoder(w).Encode(rows)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	u, _ := url.Parse(server.URL)
	o := syncOptions{to: server.URL, now: func() time.Time { return time.Unix(syncTestNow, 0) }}
	for range 15 {
		o.from = append(o.from, server.URL)
	}
	var reference []syncRow
	for _, optimized := range []bool{false, true} {
		b := newSyncLiveBackend(syncLiveOptions{hostWait: time.Second, carbonPort: mustAtoi(t, u.Port())})
		s := &syncer{o: o, b: presenceOracle{b}, frontier: &syncFrontier{key: "captured-500-name-batch"}}
		if optimized {
			s.b = b
		}
		s.frontier.scope.Now = syncTestNow
		file, err := os.CreateTemp(t.TempDir(), "frontier")
		if err != nil {
			t.Fatal(err)
		}
		defer file.Close()
		s.frontier.file = file
		infos.Store(0)
		started := time.Now()
		const total = 4000
		var got []syncRow
		for start := 0; start < total; start += 500 {
			names := make([]string, 500)
			ids := make([]int64, 500)
			for i := range names {
				names[i] = fmt.Sprintf("captured.metric.%08d", start+i)
				ids[i] = int64(start + i)
			}
			prior := syncRow{Kind: "verified", Round: 1, Names: len(names), First: names[0], Last: names[len(names)-1], Time: "2026-11-15T12:00:00Z", Identity: fmt.Sprintf("old-%d", start)}
			row, err := s.verifyFrontierBatch(context.Background(), 1, names, ids, prior)
			if err != nil {
				t.Fatal(err)
			}
			got = append(got, row)
		}
		elapsed := time.Since(started)
		if !optimized {
			reference = got
		} else if !reflect.DeepEqual(reference, got) {
			t.Fatal("optimized adoption differs from reference")
		}
		t.Logf("optimized=%v names=%d elapsed=%s adoption_names_per_second=%.0f info_rpc=%d", optimized, total, elapsed, total/elapsed.Seconds(), infos.Load())
	}
}

// The captured observation had no reusable donor fence. Keep that important
// property: a later scan cannot turn unknown before metadata into a seal.
type capturedPresenceFixture struct{ *presenceFixture }

func (f capturedPresenceFixture) WindowProofsPresence(ctx context.Context, host string, names []string, from, until int64) ([]string, []bool, error) {
	proofs, absent, err := f.presenceFixture.WindowProofsPresence(ctx, host, names, from, until)
	if host != syncT && host != syncD1 {
		clear(proofs)
		clear(absent)
	}
	return proofs, absent, err
}

func (f capturedPresenceFixture) WindowProofs(ctx context.Context, host string, names []string, from, until int64) ([]string, error) {
	proofs, _, err := f.WindowProofsPresence(ctx, host, names, from, until)
	return proofs, err
}

func TestCapturedLegacyAdoptionDifferential(t *testing.T) {
	raw, err := os.ReadFile("testdata/legacy_adoption.json")
	if err != nil {
		t.Fatal(err)
	}
	var prior syncRow
	if err := json.Unmarshal(raw, &prior); err != nil {
		t.Fatal(err)
	}
	names := make([]string, 500)
	ids := make([]int64, 500)
	for i := range names {
		names[i] = fmt.Sprintf("metric.%04d", i)
		ids[i] = int64(i)
	}
	o := syncOptions{to: syncT, from: []string{syncD1}, now: func() time.Time { return time.Unix(syncTestNow, 0) }}
	for i := range 14 {
		o.from = append(o.from, fmt.Sprintf("http://donor-%d:4242", i))
	}
	var reference syncRow
	for _, optimized := range []bool{false, true} {
		f := capturedPresenceFixture{&presenceFixture{}}
		s := &syncer{o: o, b: presenceOracle{f}, frontier: &syncFrontier{key: "captured-scope"}}
		if optimized {
			s.b = f
		}
		file, err := os.CreateTemp(t.TempDir(), "frontier")
		if err != nil {
			t.Fatal(err)
		}
		defer file.Close()
		s.frontier.file = file
		s.frontier.scope.Now = syncTestNow
		got, err := s.verifyFrontierBatch(context.Background(), 1, names, ids, prior)
		if err != nil {
			t.Fatal(err)
		}
		if !optimized {
			reference = got
		} else if !reflect.DeepEqual(reference, got) {
			t.Fatal("captured adoption result changed")
		}
		if got.LegacyReverified != 0 || got.Reused != 500 {
			t.Fatalf("reopened captured observation: %+v", got)
		}
		if got := f.proofNames.Load(); got != 8000 {
			t.Fatalf("repeated unusable after scan: %d proof names, want 8000", got)
		}
	}
}
