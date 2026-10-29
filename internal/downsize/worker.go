package downsize

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"syscall"
	"time"

	"github.com/go-graphite/go-carbon/internal/storecopy"
)

type workerRequest struct {
	PlanID, Target, Source, Checkpoint string
	Identity                           map[string]any
}
type workerLease struct {
	storecopy.Limits
	PlanID     string `json:"plan_id"`
	Checkpoint string `json:"checkpoint"`
}
type workerState struct {
	VerifiedBytes, TotalBytes int64
	Request                   workerRequest
	Phase, Error              string
	Prepared, ReplayComplete  bool
}
type workerOptions struct {
	Bind, Directory, Preflight, Helper, Target, TokenEnv, ControlEnv, PIDFile string
	LocalLab                                                                  bool
}
type worker struct {
	options workerOptions
	mu      sync.Mutex
	wg      sync.WaitGroup
	state   workerState
	cancel  context.CancelFunc
	http    *http.Client
	proxy   http.Handler
	secret  string
}

func newWorker(o workerOptions) (*worker, error) {
	if !filepath.IsAbs(o.Directory) || filepath.Clean(o.Directory) != o.Directory || o.Target == "" {
		return nil, errors.New("worker needs a fixed absolute staging directory and target ID")
	}
	secret := os.Getenv(o.ControlEnv)
	if secret == "" {
		return nil, errors.New("empty worker control secret")
	}
	w := &worker{options: o, secret: secret, http: &http.Client{Timeout: 10 * time.Minute, Transport: &http.Transport{MaxIdleConns: 128, MaxIdleConnsPerHost: 64}, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}}
	if o.Helper != "" {
		u, err := url.Parse(o.Helper)
		if err != nil {
			return nil, err
		}
		if _, err := origin(o.Helper); err != nil {
			return nil, err
		}
		w.proxy = httputil.NewSingleHostReverseProxy(u)
	}
	data, err := os.ReadFile(w.path("state"))
	if err == nil {
		if err := json.Unmarshal(data, &w.state); err != nil {
			return nil, err
		}
		if w.state.Phase == "copying" || w.state.Phase == "preparing" {
			w.state.Phase = "interrupted"
			w.state.Error = "worker restarted; resume the same checkpoint"
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	if w.state.Request.Checkpoint != "" {
		var marker struct {
			ID string `json:"id"`
		}
		data, err := os.ReadFile(filepath.Join(o.Directory, "PREPARED"))
		if err == nil && json.Unmarshal(data, &marker) == nil && marker.ID == w.state.Request.Checkpoint {
			if _, err := os.Stat(filepath.Join(o.Directory, "store/NEEDS-CATCHUP")); errors.Is(err, os.ErrNotExist) {
				w.state.Prepared = true
				w.state.Phase = "prepared"
				w.state.Error = ""
			}
		}
	}
	return w, nil
}
func (w *worker) path(kind string) string { return w.options.Directory + ".resize-" + kind + ".json" }
func (w *worker) persist() error          { return atomicState(w.path("state"), w.state) }
func atomicState(path string, value any) error {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	f, err := os.OpenFile(path+".next", os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	_, err = f.Write(append(data, '\n'))
	if err := errors.Join(err, f.Sync(), f.Close()); err != nil {
		return err
	}
	if err := os.Rename(path+".next", path); err != nil {
		return err
	}
	return syncDirectory(filepath.Dir(path))
}
func (w *worker) snapshot() workerState {
	w.mu.Lock()
	defer w.mu.Unlock()
	state := w.state
	if state.Prepared {
		_, err := os.Stat(filepath.Join(w.options.Directory, "input-log/REQUIRE-COMPLETE-REPLAY"))
		state.ReplayComplete = errors.Is(err, os.ErrNotExist)
	}
	return state
}
func (w *worker) ServeHTTP(out http.ResponseWriter, r *http.Request) {
	if subtle.ConstantTimeCompare([]byte(r.Header.Get("X-Move-Control")), []byte(w.secret)) != 1 {
		http.Error(out, "forbidden", 403)
		return
	}
	fail := func(err error) { http.Error(out, err.Error(), 409) }
	reply := func(value any) {
		out.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(out).Encode(value)
	}
	if r.URL.Path == "/sample" || r.URL.Path == "/reads" {
		if w.proxy != nil {
			w.proxy.ServeHTTP(out, r)
			return
		}
		if !w.options.LocalLab {
			http.Error(out, "worker requires a host telemetry helper", 503)
			return
		}
		if r.URL.Path == "/reads" {
			fail(errors.New("local lab cannot prove read isolation"))
			return
		}
		reply(hostSample{Time: float64(time.Now().Unix()), Generation: "local-worker", StoragePhase: "shared", DiskFreeBytes: 500 << 30, DiskBytes: 1 << 40, PhysicalCores: runtime.NumCPU(), MemoryBytes: 16 << 30, AvailableBytes: 8 << 30, NICCapacityBytes: 1e9, DiskWriteCapacity: 2e9, ReadsBlocked: true})
		return
	}
	if r.URL.Path == "/storage" && r.Method == http.MethodGet {
		host, _ := os.Hostname()
		reply(storageSample{Hostname: host, MaxTransfers: 64, CPULimit: runtime.GOMAXPROCS(0), L0StopWrites: 12, MaxBodyBytes: 160 << 20, Protocol: 1})
		return
	}
	if r.URL.Path == "/status" && r.Method == http.MethodGet {
		reply(w.snapshot())
		return
	}
	if r.Method != http.MethodPost {
		http.Error(out, "POST required", 405)
		return
	}
	r.Body = http.MaxBytesReader(out, r.Body, 1<<20)
	if r.URL.Path == "/limits" {
		var limits workerLease
		if err := json.NewDecoder(r.Body).Decode(&limits); err != nil {
			fail(err)
			return
		}
		if limits.Workers < 0 || limits.Workers > 64 || limits.Chunk < 1<<20 || limits.Chunk > 64<<20 || limits.Rate < 0 || !limits.Expires.After(time.Now()) || limits.Expires.After(time.Now().Add(90*time.Second)) {
			fail(errors.New("invalid expiring worker limits"))
			return
		}
		w.mu.Lock()
		if len(limits.PlanID) != 64 || limits.Checkpoint == "" || w.state.Request.PlanID != "" && (w.state.Request.PlanID != limits.PlanID || w.state.Request.Checkpoint != limits.Checkpoint) {
			w.mu.Unlock()
			fail(errors.New("worker lease belongs to another plan/checkpoint"))
			return
		}
		err := atomicState(w.path("lease"), limits)
		w.mu.Unlock()
		if err != nil {
			fail(err)
			return
		}
		reply(limits)
		return
	}
	var request workerRequest
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		fail(err)
		return
	}
	if len(request.PlanID) != 64 || request.Target != w.options.Target || request.Checkpoint == "" {
		fail(errors.New("invalid worker plan/target/checkpoint"))
		return
	}
	if _, err := origin(request.Source); err != nil {
		fail(err)
		return
	}
	if w.options.LocalLab && !loopbackOrigin(request.Source) {
		fail(errors.New("local lab source must be loopback"))
		return
	}
	w.mu.Lock()
	if w.state.Request.PlanID != "" && (w.state.Request.PlanID != request.PlanID || w.state.Request.Checkpoint != request.Checkpoint || w.state.Request.Source != request.Source) {
		w.mu.Unlock()
		fail(errors.New("worker staging belongs to another plan/checkpoint"))
		return
	}
	if r.URL.Path == "/stop" {
		if w.cancel != nil {
			w.cancel()
		}
		w.mu.Unlock()
		reply(map[string]bool{"stopped": true})
		return
	}
	if r.URL.Path != "/copy" && r.URL.Path != "/prepare" {
		w.mu.Unlock()
		http.NotFound(out, r)
		return
	}
	if w.state.Prepared {
		state := w.state
		w.mu.Unlock()
		reply(state)
		return
	}
	if w.cancel != nil {
		state := w.state
		w.mu.Unlock()
		reply(state)
		return
	}
	if w.state.Request.PlanID == "" {
		entries, err := os.ReadDir(w.options.Directory)
		if err != nil && !errors.Is(err, os.ErrNotExist) || err == nil && len(entries) > 0 {
			w.mu.Unlock()
			fail(errors.New("new worker staging directory must be empty"))
			return
		}
	}
	w.state.Request = request
	w.state.Error = ""
	phase := "copying"
	if data, err := os.ReadFile(filepath.Join(w.options.Directory, "PREPARED")); err == nil {
		var marker struct {
			ID string `json:"id"`
		}
		if json.Unmarshal(data, &marker) == nil && marker.ID == request.Checkpoint {
			phase = "preparing"
		}
	}
	if r.URL.Path == "/prepare" {
		phase = "preparing"
	}
	w.state.Phase = phase
	if err := w.persist(); err != nil {
		w.mu.Unlock()
		fail(err)
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	w.cancel = cancel
	w.wg.Add(1)
	w.mu.Unlock()
	go func() {
		defer w.wg.Done()
		var err error
		if phase == "copying" {
			err = w.pull(ctx, request)
		} else {
			err = storecopy.Prepare(w.options.Directory, true)
		}
		w.mu.Lock()
		defer w.mu.Unlock()
		w.cancel = nil
		switch {
		case err != nil:
			w.state.Phase = "failed"
			w.state.Error = err.Error()
		case phase == "copying":
			w.state.Phase = "copied"
		default:
			w.state.Phase = "prepared"
			w.state.Prepared = true
		}
		if saveErr := w.persist(); saveErr != nil {
			w.state.Phase = "failed"
			w.state.Error = saveErr.Error()
		}
	}()
	reply(w.snapshot())
}
func (w *worker) pull(ctx context.Context, request workerRequest) error {
	client := &storecopy.Client{URL: request.Source, Token: os.Getenv(w.options.TokenEnv), HTTP: w.http}
	m, err := client.Manifest(ctx, "status", request.Checkpoint)
	if err != nil {
		return err
	}
	reserve := int64(0)
	if !w.options.LocalLab {
		if err := checkPreflight(w.options.Preflight, w.options.Directory); err != nil {
			return err
		}
		var preflight struct {
			ConfigPath string `json:"config_path"`
			Reserve    int64  `json:"disk_reserve_bytes"`
		}
		data, err := os.ReadFile(w.options.Preflight)
		if err != nil {
			return err
		}
		if err := json.Unmarshal(data, &preflight); err != nil {
			return err
		}
		expected := *m
		expected.Metadata = make(map[string]any)
		for key, value := range m.Metadata {
			expected.Metadata[key] = value
		}
		for key, value := range request.Identity {
			if key != "node" && key != "nodes" && key != "hash" && key != "replicas" {
				return errors.New("unsupported target identity field")
			}
			expected.Metadata[key] = value
		}
		if err := checkIdentity(preflight.ConfigPath, w.options.Directory, &expected); err != nil {
			return err
		}
		reserve = preflight.Reserve
	}
	return client.Pull(ctx, w.options.Directory, request.Checkpoint, storecopy.PullOptions{Workers: 64, Chunk: 16 << 20, Rate: 1e9, Lease: w.path("lease"), Apply: true, Reserve: reserve, ReuseVerified: true, Progress: func(progress storecopy.PullProgress) {
		w.mu.Lock()
		w.state.VerifiedBytes, w.state.TotalBytes = progress.Bytes, progress.Total
		w.mu.Unlock()
	}})
}
func serveWorker(ctx context.Context, o workerOptions) error {
	lock, err := os.OpenFile(o.Directory+".resize-worker.lock", os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return err
	}
	defer lock.Close()
	if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		return errors.New("another worker owns this staging directory")
	}
	w, err := newWorker(o)
	if err != nil {
		return err
	}
	defer func() {
		w.mu.Lock()
		if w.cancel != nil {
			w.cancel()
		}
		w.mu.Unlock()
		w.wg.Wait()
	}()
	if o.PIDFile != "" {
		if err := os.WriteFile(o.PIDFile, []byte(fmt.Sprint(os.Getpid())+"\n"), 0600); err != nil {
			return err
		}
	}
	listener, err := net.Listen("tcp", o.Bind)
	if err != nil {
		return err
	}
	server := &http.Server{Handler: w, ReadHeaderTimeout: 10 * time.Second}
	defer server.Close()
	defer w.http.CloseIdleConnections()
	done := make(chan error, 1)
	go func() { done <- server.Serve(listener) }()
	event("worker", map[string]any{"listen": listener.Addr().String(), "target": o.Target, "directory": o.Directory})
	select {
	case <-ctx.Done():
		w.mu.Lock()
		if w.cancel != nil {
			w.cancel()
		}
		w.mu.Unlock()
		return nil
	case err := <-done:
		return err
	}
}
