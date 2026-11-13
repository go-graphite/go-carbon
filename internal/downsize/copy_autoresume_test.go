package downsize

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
)

// restartable serves a handler on a fixed port and can go away (connections
// refused, in-flight requests cut) and come back, like a go-carbon restart.
type restartable struct {
	t       *testing.T
	addr    string
	handler http.Handler
	mu      sync.Mutex
	srv     *http.Server
	down    atomic.Bool
}

func newRestartable(t *testing.T, handler http.Handler) *restartable {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	r := &restartable{t: t, addr: ln.Addr().String(), handler: handler}
	r.serve(ln)
	t.Cleanup(r.stop)
	return r
}

func (r *restartable) url() string { return "http://" + r.addr }

func (r *restartable) serve(ln net.Listener) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.srv = &http.Server{Handler: r.handler, ReadHeaderTimeout: 5 * time.Second}
	go func(srv *http.Server) { _ = srv.Serve(ln) }(r.srv)
}

func (r *restartable) stop() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.srv != nil {
		_ = r.srv.Close()
		r.srv = nil
	}
}

// restart takes the server away for down, then rebinds the same port.
func (r *restartable) restart(down time.Duration) {
	r.down.Store(true)
	r.stop()
	time.Sleep(down)
	for i := 0; ; i++ {
		ln, err := net.Listen("tcp", r.addr)
		if err == nil {
			r.serve(ln)
			r.down.Store(false)
			return
		}
		if i > 100 {
			r.t.Errorf("rebind %s: %v", r.addr, err)
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
}

type restartFixture struct {
	mu       sync.Mutex
	catalogs map[string][]string // source origin -> names
	sources  map[string]*restartable
	imported map[string]int // "source|name" -> imports
	pulls    atomic.Int64
	onPull   func(n int64)
	pullCode func() int
}

func newRestartFixture(t *testing.T, catalogs ...[]string) (*restartFixture, []string, *restartable) {
	t.Helper()
	f := &restartFixture{catalogs: map[string][]string{}, sources: map[string]*restartable{}, imported: map[string]int{}}
	status := func(w http.ResponseWriter) {
		_ = json.NewEncoder(w).Encode(copyStatus{MaxTransfers: 4, MaxBodyBytes: 1 << 20})
	}
	var froms []string
	for _, catalog := range catalogs {
		catalog := catalog
		src := newRestartable(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			switch r.URL.Path {
			case "/v1/transfer/status":
				status(w)
			case "/v1/transfer/list":
				after := r.URL.Query().Get("after")
				page := []string{}
				for _, name := range catalog {
					if name > after && len(page) < 700 {
						page = append(page, name)
					}
				}
				_ = json.NewEncoder(w).Encode(page)
			default:
				http.NotFound(w, r)
			}
		}))
		f.catalogs[src.url()] = catalog
		f.sources[src.url()] = src
		froms = append(froms, src.url())
	}
	dst := newRestartable(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/transfer/status":
			status(w)
		case "/v1/transfer/pull":
			var request struct {
				Source string
				Names  []string
			}
			if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
				http.Error(w, err.Error(), 400)
				return
			}
			n := f.pulls.Add(1)
			if f.onPull != nil {
				f.onPull(n)
			}
			if f.pullCode != nil {
				if code := f.pullCode(); code != 0 {
					http.Error(w, "injected", code)
					return
				}
			}
			// The real target answers 500 when its source export is refused.
			if src := f.sources[request.Source]; src == nil || src.down.Load() {
				http.Error(w, fmt.Sprintf(`source export: Post "%s/v1/transfer/export": dial tcp %s: connect: connection refused`, request.Source, strings.TrimPrefix(request.Source, "http://")), 500)
				return
			}
			known := map[string]bool{}
			for _, name := range f.catalogs[request.Source] {
				known[name] = true
			}
			proofs := make([]store.TransferProof, len(request.Names))
			f.mu.Lock()
			for i, name := range request.Names {
				if !known[name] {
					f.mu.Unlock()
					http.Error(w, "not found", 404)
					return
				}
				f.imported[request.Source+"|"+name]++
				proofs[i] = store.TransferProof{Source: store.TransferVersion{Name: name}, Target: store.TransferVersion{Name: name}, Digest: "d"}
			}
			f.mu.Unlock()
			w.Header().Set("X-Transfer-Bytes", fmt.Sprint(1000*len(request.Names)))
			_ = json.NewEncoder(w).Encode(proofs)
		case "/v1/transfer/verify":
			_ = json.NewEncoder(w).Encode(map[string]int{"verified": 1})
		default:
			http.NotFound(w, r)
		}
	}))
	return f, froms, dst
}

// One command, two donors: the target goes away mid-batch (in-flight pull cut,
// connections refused for longer than --stall-timeout), then a donor goes away
// (the target answers 500 "connection refused"). The copy waits, re-pulls and
// finishes with every name copied, no failure and no abort; a rerun resumes.
func TestPlanFreeCopyRidesThroughSourceAndTargetRestarts(t *testing.T) {
	a, b := copyNames("a.", 1500), copyNames("b.", 1500)
	f, froms, dst := newRestartFixture(t, a, b)
	var wg sync.WaitGroup
	var targetRestarted, sourceRestarted atomic.Bool
	f.onPull = func(n int64) {
		switch n {
		case 5:
			targetRestarted.Store(true)
			wg.Add(1)
			go func() { defer wg.Done(); dst.restart(600 * time.Millisecond) }()
			time.Sleep(50 * time.Millisecond) // the restart cuts this in-flight pull
		case 15:
			sourceRestarted.Store(true)
			wg.Add(1)
			go func() { defer wg.Done(); f.sources[froms[0]].restart(600 * time.Millisecond) }()
			time.Sleep(20 * time.Millisecond)
		}
	}
	o := copyTestOptions(t, froms[0], dst.url())
	o.froms = froms
	o.maxNames = 100
	o.tick, o.maxBackoff = 10*time.Millisecond, 50*time.Millisecond
	o.stallTimeout = 200 * time.Millisecond // shorter than each outage: down hosts mean wait
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := planFreeCopy(ctx, o); err != nil {
		t.Fatalf("copy aborted across restarts: %v", err)
	}
	wg.Wait()
	if !targetRestarted.Load() || !sourceRestarted.Load() {
		t.Fatalf("restarts not exercised: target=%v source=%v pulls=%d", targetRestarted.Load(), sourceRestarted.Load(), f.pulls.Load())
	}
	f.mu.Lock()
	for i, catalog := range [][]string{a, b} {
		for _, name := range catalog {
			if f.imported[froms[i]+"|"+name] == 0 {
				t.Fatalf("%s from %s never copied", name, froms[i])
			}
		}
	}
	f.mu.Unlock()
	journal, err := os.ReadFile(o.state)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(journal), `"kind":"failed"`) {
		t.Fatalf("restart journaled name failures:\n%s", journal)
	}
	if !strings.Contains(string(journal), `"source":"`+froms[1]+`"`) {
		t.Fatal("several-donor journal rows carry no source")
	}
	// Rerunning the same command resumes: nothing left to copy.
	before := f.pulls.Load()
	f.onPull = nil
	if err := planFreeCopy(ctx, o); err != nil {
		t.Fatal(err)
	}
	if f.pulls.Load() != before {
		t.Fatalf("resume re-pulled %d batches", f.pulls.Load()-before)
	}
}

// Every host answers its status endpoint but the pull keeps failing
// transiently: that is a genuine stall and fails loudly after --stall-timeout.
func TestPlanFreeCopyStallFailsOnlyWithHostsUp(t *testing.T) {
	f, froms, dst := newRestartFixture(t, copyNames("a.", 50))
	f.pullCode = func() int { return http.StatusServiceUnavailable }
	o := copyTestOptions(t, froms[0], dst.url())
	o.tick, o.maxBackoff = 5*time.Millisecond, 20*time.Millisecond
	o.stallTimeout = 150 * time.Millisecond
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	err := planFreeCopy(ctx, o)
	if !errors.Is(err, errCopyStall) {
		t.Fatalf("want stall error, got %v", err)
	}
}

func TestPlanFreeCopyRejectsNamesWithSeveralDonors(t *testing.T) {
	o := copyTestOptions(t, "http://a:4242", "http://c:4242")
	o.froms = []string{"http://a:4242", "http://b:4242"}
	names := t.TempDir() + "/names"
	if err := os.WriteFile(names, []byte("x\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	o.names = names
	if err := planFreeCopy(context.Background(), o); err == nil || !strings.Contains(err.Error(), "single --from") {
		t.Fatalf("want --names/--from error, got %v", err)
	}
}

func TestOriginListRepeatsAndSplits(t *testing.T) {
	var l originList
	_ = l.Set("http://a:4242, http://b:4242")
	_ = l.Set("http://c:4242")
	if got := l.String(); got != "http://a:4242,http://b:4242,http://c:4242" {
		t.Fatal(got)
	}
}

// A donor in its restart window answers export with 409 while every status
// endpoint is up (seen on a production donor restart: the previous binary
// split down to per-name failures and aborted above --max-failures). The
// copy re-pulls the whole batch until the window closes; nothing fails.
func TestPlanFreeCopySettlesThroughSourceRestartConflicts(t *testing.T) {
	f, froms, dst := newRestartFixture(t, copyNames("a.", 400))
	until := time.Now().Add(150 * time.Millisecond)
	var conflicts atomic.Int64
	f.pullCode = func() int {
		if time.Now().Before(until) {
			conflicts.Add(1)
			return http.StatusConflict
		}
		return 0
	}
	o := copyTestOptions(t, froms[0], dst.url())
	o.maxNames, o.maxFailures = 50, 0
	o.tick, o.maxBackoff = 20*time.Millisecond, 100*time.Millisecond
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := planFreeCopy(ctx, o); err != nil {
		t.Fatalf("copy failed through a source restart window: %v", err)
	}
	if conflicts.Load() == 0 {
		t.Fatal("restart window not exercised")
	}
	journal, _ := os.ReadFile(o.state)
	if strings.Contains(string(journal), `"kind":"failed"`) {
		t.Fatalf("restart window journaled failures:\n%s", journal)
	}
}
