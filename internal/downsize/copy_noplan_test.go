package downsize

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/internal/downsize/ringplan"
)

// fakeCopy serves a source catalog and a target pull/verify endpoint with
// per-name byte sizes: a pull above MaxBytes is rejected with 413, like the
// real source export.
type fakeCopy struct {
	mu       sync.Mutex
	catalog  []string
	sizes    map[string]int64
	maxBody  int64
	page     int
	attempts int
	inject   func(attempt int, names []string) int // HTTP status to return instead, 0 = serve
	block    func(attempt int) bool                // hang until the request is cancelled
	batches  [][]string
	imported map[string]int
	maxBytes []int64
}

func newFakeCopy(t *testing.T, catalog []string) (*fakeCopy, string, string) {
	t.Helper()
	f := &fakeCopy{catalog: catalog, sizes: map[string]int64{}, maxBody: 1 << 20, page: 300, imported: map[string]int{}}
	status := func(w http.ResponseWriter) {
		_ = json.NewEncoder(w).Encode(copyStatus{MaxTransfers: 8, MaxBodyBytes: f.maxBody})
	}
	src := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/transfer/status":
			status(w)
		case "/v1/transfer/list":
			prefix, after := r.URL.Query().Get("prefix"), r.URL.Query().Get("after")
			limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
			page := []string{}
			for _, name := range f.catalog {
				if strings.HasPrefix(name, prefix) && name > after && len(page) < min(limit, f.page) {
					page = append(page, name)
				}
			}
			_ = json.NewEncoder(w).Encode(page)
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(src.Close)
	dst := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/transfer/status":
			status(w)
		case "/v1/transfer/pull":
			var request struct {
				Source   string
				Names    []string
				MaxBytes int64
			}
			if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
				http.Error(w, err.Error(), 400)
				return
			}
			f.mu.Lock()
			f.attempts++
			attempt := f.attempts
			f.maxBytes = append(f.maxBytes, request.MaxBytes)
			f.mu.Unlock()
			if f.block != nil && f.block(attempt) {
				<-r.Context().Done()
				return
			}
			if f.inject != nil {
				if code := f.inject(attempt, request.Names); code != 0 {
					http.Error(w, "injected", code)
					return
				}
			}
			var total int64
			for _, name := range request.Names {
				size := f.sizes[name]
				if size == 0 {
					size = 1000
				}
				total += size
			}
			if total > request.MaxBytes || request.MaxBytes > f.maxBody {
				http.Error(w, "transfer exceeds byte budget", 413)
				return
			}
			proofs := make([]store.TransferProof, len(request.Names))
			f.mu.Lock()
			f.batches = append(f.batches, append([]string(nil), request.Names...))
			for i, name := range request.Names {
				f.imported[name]++
				proofs[i] = store.TransferProof{Source: store.TransferVersion{Name: name}, Target: store.TransferVersion{Name: name}, Digest: "d"}
			}
			f.mu.Unlock()
			w.Header().Set("X-Transfer-Bytes", strconv.FormatInt(total, 10))
			_ = json.NewEncoder(w).Encode(proofs)
		case "/v1/transfer/verify":
			_ = json.NewEncoder(w).Encode(map[string]int{"verified": 1})
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(dst.Close)
	return f, src.URL, dst.URL
}

func copyTestOptions(t *testing.T, from, to string) copyOptions {
	return copyOptions{froms: []string{from}, to: to, stallTimeout: time.Minute, state: filepath.Join(t.TempDir(), "copy.jsonl"), engine: "metric", tokenEnv: "NO_SUCH_TOKEN_ENV", streams: 4, maxBytes: 1 << 20, maxNames: 8192, maxFailures: 100, tick: time.Millisecond, maxBackoff: 5 * time.Millisecond, transportRetries: 3, transferIdle: time.Minute}
}

func copyNames(prefix string, n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = fmt.Sprintf("%s%05d", prefix, i)
	}
	return out
}

func TestPlanFreeCopyDiscoversPagedPrefixAndBatchesByBytes(t *testing.T) {
	wanted := copyNames("a.", 3000)
	catalog := append(append([]string{}, wanted...), copyNames("b.", 500)...)
	sort.Strings(catalog)
	f, src, dst := newFakeCopy(t, catalog)
	o := copyTestOptions(t, src, dst)
	o.streams = 1
	o.prefix = "a."
	if err := planFreeCopy(context.Background(), o); err != nil {
		t.Fatal(err)
	}
	if len(f.imported) != len(wanted) {
		t.Fatalf("imported %d names, want %d", len(f.imported), len(wanted))
	}
	for _, name := range wanted {
		if f.imported[name] != 1 {
			t.Fatalf("%s imported %d times", name, f.imported[name])
		}
	}
	// 1000-byte names under a 1 MiB budget: the first batch uses the 16 KiB
	// default, then batches grow to ~786 names from the observed bytes.
	if len(f.batches) > 20 {
		t.Fatalf("%d batches for %d small names; batching ignored real bytes", len(f.batches), len(wanted))
	}
	largest := 0
	for _, b := range f.batches {
		largest = max(largest, len(b))
	}
	if largest < 500 {
		t.Fatalf("largest batch %d names, want hundreds", largest)
	}
}

func TestPlanFreeCopyNamesFileMustBeSorted(t *testing.T) {
	_, src, dst := newFakeCopy(t, nil)
	o := copyTestOptions(t, src, dst)
	o.names = filepath.Join(t.TempDir(), "names")
	if err := os.WriteFile(o.names, []byte("b\na\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := planFreeCopy(context.Background(), o); err == nil || !strings.Contains(err.Error(), "byte-sorted") {
		t.Fatal("unsorted names accepted:", err)
	}
}

func TestPlanFreeCopyRingKeepsOnlyTargetOwnedNames(t *testing.T) {
	all := copyNames("ring.", 900)
	f, src, dst := newFakeCopy(t, all)
	ring := filepath.Join(t.TempDir(), "ring.json")
	topology := fmt.Sprintf(`{"hash":"jump_fnv1a","nodes":[
		{"id":"a","node":"store-a:2003=a","endpoint":"http://store-a:4242"},
		{"id":"b","node":"store-b:2003=b","endpoint":%q},
		{"id":"c","node":"store-c:2003=c","endpoint":"http://store-c:4242"}]}`, dst)
	if err := os.WriteFile(ring, []byte(topology), 0o644); err != nil {
		t.Fatal(err)
	}
	owners := map[string]func(string) bool{}
	for _, endpoint := range []string{"http://store-a:4242", dst, "http://store-c:4242"} {
		owner, err := ringplan.TargetOwner(ring, endpoint)
		if err != nil {
			t.Fatal(err)
		}
		owners[endpoint] = owner
	}
	want := map[string]bool{}
	for _, name := range all {
		count := 0
		for _, owner := range owners {
			if owner(name) {
				count++
			}
		}
		if count != 1 {
			t.Fatalf("%s owned by %d members", name, count)
		}
		if owners[dst](name) {
			want[name] = true
		}
	}
	if len(want) < 200 || len(want) > 400 {
		t.Fatalf("target owns %d of 900 names; jump hash spread is off", len(want))
	}
	o := copyTestOptions(t, src, dst)
	o.ring = ring
	if err := planFreeCopy(context.Background(), o); err != nil {
		t.Fatal(err)
	}
	if len(f.imported) != len(want) {
		t.Fatalf("imported %d, target owns %d", len(f.imported), len(want))
	}
	for name := range f.imported {
		if !want[name] {
			t.Fatal("copied a name the target does not own:", name)
		}
	}
	if _, err := ringplan.TargetOwner(ring, "http://elsewhere:4242"); err == nil {
		t.Fatal("accepted a target outside the ring")
	}
}

func TestPlanFreeCopySendsOversizedNameAloneAndSkipsImpossibleOne(t *testing.T) {
	all := copyNames("x.", 20)
	f, src, dst := newFakeCopy(t, all)
	f.maxBody = 4 << 20
	f.sizes["x.00010"] = 2 << 20 // above the 1 MiB batch cap, below max_body_bytes
	f.sizes["x.00015"] = 5 << 20 // above max_body_bytes: can never be imported
	o := copyTestOptions(t, src, dst)
	o.streams = 1
	err := planFreeCopy(context.Background(), o)
	if err == nil || !strings.Contains(err.Error(), "1 names failed") {
		t.Fatal("want one journaled failure, got", err)
	}
	for _, name := range all {
		want := 1
		if name == "x.00015" {
			want = 0
		}
		if f.imported[name] != want {
			t.Fatalf("%s imported %d times, want %d", name, f.imported[name], want)
		}
	}
	alone := false
	for _, b := range f.batches {
		if len(b) == 1 && b[0] == "x.00010" {
			alone = true
		}
		if len(b) > 1 && strings.Contains(strings.Join(b, ","), "x.00010") {
			t.Fatal("oversized name shared a batch", b)
		}
	}
	if !alone {
		t.Fatal("oversized name was not sent alone")
	}
	for _, budget := range f.maxBytes {
		if budget != 1<<20 && budget != 4<<20 {
			t.Fatal("unexpected byte budget", budget)
		}
	}
	data, err := os.ReadFile(o.state)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(data), `"kind":"failed","name":"x.00015","status":413`) {
		t.Fatalf("failure not journaled:\n%s", data)
	}
	// A rerun retries only the failed name.
	f.mu.Lock()
	f.batches = nil
	f.mu.Unlock()
	if err := planFreeCopy(context.Background(), o); err == nil {
		t.Fatal("impossible name succeeded on rerun")
	}
	for _, b := range f.batches {
		t.Fatal("rerun imported", b)
	}
}

func TestPlanFreeCopyResumesAfterKillWithoutLosingOrDuplicating(t *testing.T) {
	all := copyNames("r.", 600)
	f, src, dst := newFakeCopy(t, all)
	f.maxBody = 64 << 20
	o := copyTestOptions(t, src, dst)
	o.streams = 1
	o.maxBytes = 64 << 20
	o.maxNames = 50
	ctx, cancel := context.WithCancel(context.Background())
	f.block = func(attempt int) bool {
		if attempt == 4 {
			cancel() // killed while the 4th transfer is in flight
			return true
		}
		return false
	}
	if err := planFreeCopy(ctx, o); !errors.Is(err, context.Canceled) {
		t.Fatal("killed run returned", err)
	}
	if len(f.batches) != 3 {
		t.Fatalf("%d batches before the kill, want 3", len(f.batches))
	}
	// A torn final append from the kill must be ignored.
	file, err := os.OpenFile(o.state, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = file.WriteString(`{"kind":"done","first":"r.00000","la`)
	file.Close()
	f.block = nil
	f.batches = nil
	if err := planFreeCopy(context.Background(), o); err != nil {
		t.Fatal(err)
	}
	if f.batches[0][0] != "r.00150" {
		t.Fatal("resume started at", f.batches[0][0])
	}
	for _, name := range all {
		if f.imported[name] != 1 {
			t.Fatalf("%s imported %d times across kill+resume", name, f.imported[name])
		}
	}
	// A third run finds nothing left.
	f.batches = nil
	if err := planFreeCopy(context.Background(), o); err != nil || len(f.batches) != 0 {
		t.Fatal("completed copy redid work", err, len(f.batches))
	}
	o.prefix = "r.001"
	if err := planFreeCopy(context.Background(), o); err == nil || !strings.Contains(err.Error(), "another copy") {
		t.Fatal("journal reused for a different copy:", err)
	}
}

func TestPlanFreeCopyRetriesTransientGuards(t *testing.T) {
	all := copyNames("g.", 200)
	f, src, dst := newFakeCopy(t, all)
	f.inject = func(attempt int, _ []string) int {
		switch attempt {
		case 1, 2, 4:
			return http.StatusServiceUnavailable
		case 3:
			return http.StatusTooManyRequests
		}
		return 0
	}
	o := copyTestOptions(t, src, dst)
	o.streams = 1
	if err := planFreeCopy(context.Background(), o); err != nil {
		t.Fatal("transient guard aborted the run:", err)
	}
	for _, name := range all {
		if f.imported[name] != 1 {
			t.Fatalf("%s imported %d times", name, f.imported[name])
		}
	}
	for _, b := range f.batches {
		if len(b) < 2 {
			t.Fatal("a guard response split a batch", len(b))
		}
	}
}

func TestPlanFreeCopyEndToEndTargetWins(t *testing.T) {
	ctx := context.Background()
	source, target := localStore(t), localStore(t)
	all := copyNames("e2e.", 40)
	for i, name := range all {
		addPoints(t, source, name, float64(i+2))
	}
	addPoints(t, target, all[0], 999)
	src := httptest.NewServer(localService(t, source))
	t.Cleanup(src.Close)
	dst := httptest.NewServer(localService(t, target))
	t.Cleanup(dst.Close)
	o := copyTestOptions(t, src.URL, dst.URL)
	o.names = filepath.Join(t.TempDir(), "names")
	if err := os.WriteFile(o.names, []byte(strings.Join(all, "\n")+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := planFreeCopy(ctx, o); err != nil {
		t.Fatal(err)
	}
	for i, name := range all {
		snapshot, err := target.Snapshot(ctx, name)
		if err != nil {
			t.Fatal("target missing", name, err)
		}
		want := float64(i + 2)
		if i == 0 {
			want = 999
		}
		if got := snapshot.Archives[0].Points[1].Value; got != want {
			t.Fatalf("%s last value %v, want %v", name, got, want)
		}
	}
}

// store-copy fills the transfer slots both ends advertise instead of a fixed
// guess: the old 64 ceiling held copies at a fraction of the hosts' caps.
func TestDefaultCopyStreams(t *testing.T) {
	for _, tc := range []struct{ source, target, want int }{
		{128, 128, 128}, {128, 192, 128}, {192, 128, 128}, {59, 128, 59},
		{0, 160, 160}, {160, 0, 160}, {0, 0, 64},
	} {
		if got := defaultCopyStreams(tc.source, tc.target); got != tc.want {
			t.Errorf("defaultCopyStreams(%d, %d) = %d, want %d", tc.source, tc.target, got, tc.want)
		}
	}
}
