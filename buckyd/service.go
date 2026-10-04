// Package buckyd exposes the buckytools metric-transfer API over the shared
// metric store. The carbon App owns the store lifecycle; Service never opens or
// closes it.
package buckyd

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/pprof"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/internal/whisperio"
	"github.com/golang-jwt/jwt/v5"
	"github.com/golang/snappy"
	"github.com/grafana/pyroscope-go"
)

const authHeader = "X-Buckyd-Authorization"

type Config struct {
	Enabled       bool     `toml:"enabled"`
	Bind          string   `toml:"bind"`
	TmpDir        string   `toml:"tmpdir"`
	MaxBodyBytes  int64    `toml:"max_body_bytes"`
	MaxTransfers  int      `toml:"max_transfers"`
	JWTSecretFile string   `toml:"auth-jwt-secret-file"`
	Node          string   `toml:"node"`
	Hash          string   `toml:"hash"`
	Replicas      int      `toml:"replicas"`
	Nodes         []string `toml:"nodes"`
	ReadMTime     bool     `toml:"mtime"`
	Sparse        bool     `toml:"sparse"`
	Compressed    bool     `toml:"compressed"`
	Backend       string   `toml:"backend"`
	CachePath     string   `toml:"cache_path"`
	Prefix        string   `toml:"prefix"`
	Timeout       int64    `toml:"timeout"`
	Pprof         string   `toml:"pprof"`
	Pyroscope     string   `toml:"pyroscope"`
}

// Node uses buckytools' wire field names and represents HOST[:PORT][=INSTANCE].
type Node struct {
	Server   string
	Port     int
	Instance string
}

type MetricData struct {
	Name           string
	Size           int64
	Mode           int64
	ModTime        int64
	Encoding       int
	StorageVersion string
	Data           []byte `json:"-"`
}

type MetricHealStats struct {
	Download time.Duration
	Dump     time.Duration
	Fill     time.Duration
	Compress time.Duration
	Copy     time.Duration
}

type ACL struct {
	jwt.RegisteredClaims
	Namespaces []string `json:"namespaces"`
	Ops        []string `json:"ops"`
}

type Service struct {
	config        Config
	store         *chunkstore.Store
	whisperIO     *whisperio.Adapter
	secret        []byte
	server        *http.Server
	listener      net.Listener
	transfers     chan struct{}
	mu            sync.Mutex
	handlers      sync.WaitGroup
	serveErr      chan error
	nodes         []Node
	onChange      func()
	pyroscope     *pyroscope.Profiler
	pprofServer   *http.Server
	pprofListener net.Listener
	pprofErr      chan error
	stopped       bool
}

func New(config Config, metricStore *chunkstore.Store) (*Service, error) {
	if metricStore == nil {
		return nil, errors.New("metric store is required")
	}
	if config.Backend != "" && config.Backend != "shared" {
		return nil, fmt.Errorf("unsupported buckyd backend %q", config.Backend)
	}
	if config.Sparse || config.Compressed || config.ReadMTime {
		return nil, errors.New("sparse, compressed, and mtime options are not supported with shared buckyd storage")
	}
	if config.CachePath != "" || config.Prefix != "" {
		return nil, errors.New("cache_path and prefix are filesystem buckyd options unsupported by shared storage")
	}
	if config.Hash == "" {
		config.Hash = "carbon"
	}
	if config.Hash != "carbon" && config.Hash != "fnv1a" && config.Hash != "jump_fnv1a" {
		return nil, fmt.Errorf("unsupported hash %q", config.Hash)
	}
	if config.Replicas == 0 {
		config.Replicas = 1
	}
	if config.Replicas < 1 {
		return nil, errors.New("replicas must be positive")
	}
	if config.Node == "" {
		config.Node, _ = os.Hostname()
	}
	if config.Timeout == 0 {
		config.Timeout = 3600
	}
	nodes := make([]Node, 0, len(config.Nodes))
	for _, raw := range config.Nodes {
		node, err := parseNode(raw)
		if err != nil {
			return nil, err
		}
		nodes = append(nodes, node)
	}
	if config.TmpDir == "" {
		config.TmpDir = os.TempDir()
	}
	if err := os.MkdirAll(config.TmpDir, 0755); err != nil {
		return nil, fmt.Errorf("create buckyd temporary directory: %w", err)
	}
	if config.MaxBodyBytes <= 0 {
		config.MaxBodyBytes = 160 << 20
	}
	if config.MaxTransfers <= 0 {
		config.MaxTransfers = 4
	}
	if config.JWTSecretFile != "" {
		b, err := os.ReadFile(config.JWTSecretFile)
		if err != nil {
			return nil, fmt.Errorf("read buckyd JWT secret: %w", err)
		}
		configSecret := strings.TrimSpace(string(b))
		if configSecret == "" {
			return nil, errors.New("buckyd JWT secret is empty")
		}
		return &Service{config: config, store: metricStore, whisperIO: whisperio.New(metricStore), secret: []byte(configSecret), transfers: make(chan struct{}, config.MaxTransfers), nodes: nodes}, nil
	}
	return &Service{config: config, store: metricStore, whisperIO: whisperio.New(metricStore), transfers: make(chan struct{}, config.MaxTransfers), nodes: nodes}, nil
}

func parseNode(raw string) (Node, error) {
	var node Node
	parts := strings.SplitN(raw, "=", 2)
	hostPort := parts[0]
	if len(parts) == 2 {
		node.Instance = parts[1]
	}
	host, port, err := net.SplitHostPort(hostPort)
	if err == nil {
		node.Server = host
		node.Port, err = strconv.Atoi(port)
		if err == nil && (node.Port < 0 || node.Port > 65535) {
			err = fmt.Errorf("invalid node port in %q", raw)
		}
		return node, err
	}
	if strings.Count(hostPort, ":") == 1 {
		parts := strings.SplitN(hostPort, ":", 2)
		node.Server = parts[0]
		node.Port, err = strconv.Atoi(parts[1])
		if err == nil && (node.Port < 0 || node.Port > 65535) {
			err = fmt.Errorf("invalid node port in %q", raw)
		}
		return node, err
	}
	if hostPort == "" {
		return Node{}, fmt.Errorf("invalid node %q", raw)
	}
	node.Server = hostPort
	return node, nil
}

func (s *Service) Start() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.server != nil {
		return errors.New("buckyd already started")
	}
	bind := s.config.Bind
	if bind == "" {
		bind = ":4242"
	}
	listener, err := net.Listen("tcp", bind)
	if err != nil {
		return fmt.Errorf("listen buckyd %s: %w", bind, err)
	}
	s.listener = listener
	s.stopped = false
	s.server = &http.Server{Addr: bind, Handler: s.Handler()}
	if s.config.Pprof != "" {
		pprofListener, err := net.Listen("tcp", s.config.Pprof)
		if err != nil {
			_ = listener.Close()
			s.listener = nil
			s.server = nil
			return fmt.Errorf("listen buckyd pprof %s: %w", s.config.Pprof, err)
		}
		mux := http.NewServeMux()
		mux.HandleFunc("/debug/pprof/", pprof.Index)
		mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
		mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
		mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
		mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
		s.pprofListener = pprofListener
		s.pprofServer = &http.Server{Handler: mux}
		s.pprofErr = make(chan error, 1)
		go func(server *http.Server, listener net.Listener, errs chan error) {
			err := server.Serve(listener)
			if err != nil && !errors.Is(err, http.ErrServerClosed) {
				errs <- err
			}
			close(errs)
		}(s.pprofServer, pprofListener, s.pprofErr)
	}
	if s.config.Pyroscope != "" {
		runtime.SetMutexProfileFraction(5)
		runtime.SetBlockProfileRate(5)
		profiler, err := pyroscope.Start(pyroscope.Config{ApplicationName: "buckyd", ServerAddress: s.config.Pyroscope, Logger: nil, ProfileTypes: []pyroscope.ProfileType{pyroscope.ProfileCPU, pyroscope.ProfileAllocObjects, pyroscope.ProfileAllocSpace, pyroscope.ProfileInuseObjects, pyroscope.ProfileInuseSpace, pyroscope.ProfileGoroutines, pyroscope.ProfileMutexCount, pyroscope.ProfileMutexDuration, pyroscope.ProfileBlockCount, pyroscope.ProfileBlockDuration}})
		if err != nil {
			if s.pprofServer != nil {
				_ = s.pprofServer.Close()
				<-s.pprofErr
			}
			_ = listener.Close()
			s.listener = nil
			s.server = nil
			s.pprofServer = nil
			s.pprofListener = nil
			s.pprofErr = nil
			return fmt.Errorf("start buckyd pyroscope: %w", err)
		}
		s.pyroscope = profiler
	}
	s.serveErr = make(chan error, 1)
	serveErr := s.serveErr
	go func(server *http.Server, listener net.Listener, errs chan error) {
		err := server.Serve(listener)
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			errs <- err
		}
		close(errs)
	}(s.server, listener, serveErr)
	return nil
}

// SetOnChange installs an immutable callback invoked after a successful
// mutation has committed. It is intended to refresh carbonserver's catalog.
func (s *Service) SetOnChange(callback func()) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.onChange = callback
}
func (s *Service) changed() {
	s.mu.Lock()
	callback := s.onChange
	s.mu.Unlock()
	if callback != nil {
		callback()
	}
}
func (s *Service) Stop() error {
	s.mu.Lock()
	server := s.server
	s.stopped = true
	listener := s.listener
	serveErr := s.serveErr
	profiler := s.pyroscope
	pprofServer := s.pprofServer
	pprofListener := s.pprofListener
	pprofErr := s.pprofErr
	s.server = nil
	s.listener = nil
	s.serveErr = nil
	s.pyroscope = nil
	s.pprofServer = nil
	s.pprofListener = nil
	s.pprofErr = nil
	s.mu.Unlock()
	if server == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	err := server.Shutdown(ctx)
	if err != nil {
		_ = server.Close()
	}
	done := make(chan struct{})
	go func() { s.handlers.Wait(); close(done) }()
	select {
	case <-done:
	case <-ctx.Done():
		if err == nil {
			err = ctx.Err()
		}
		_ = server.Close()
		<-done
	}
	if listener != nil {
		_ = listener.Close()
	}
	if serveErr != nil {
		if serveError, ok := <-serveErr; ok && serveError != nil && err == nil {
			err = serveError
		}
	}
	if profiler != nil {
		_ = profiler.Stop()
	}
	if pprofServer != nil {
		_ = pprofServer.Close()
	}
	if pprofListener != nil {
		_ = pprofListener.Close()
	}
	if pprofErr != nil {
		if pprofError, ok := <-pprofErr; ok && pprofError != nil && err == nil {
			err = pprofError
		}
	}
	return err
}
func (s *Service) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/metrics", s.list)
	mux.HandleFunc("/metrics/", s.metric)
	mux.HandleFunc("/hashring", s.hashring)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.mu.Lock()
		if s.stopped {
			s.mu.Unlock()
			http.Error(w, "buckyd is stopped", http.StatusServiceUnavailable)
			return
		}
		s.handlers.Add(1)
		s.mu.Unlock()
		defer s.handlers.Done()
		mux.ServeHTTP(w, r)
	})
}

func (s *Service) allowed(metric, op string, r *http.Request) error {
	if len(s.secret) == 0 {
		return nil
	}
	token, err := jwt.ParseWithClaims(r.Header.Get(authHeader), &ACL{}, func(t *jwt.Token) (interface{}, error) {
		if _, ok := t.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, errors.New("unexpected JWT signing method")
		}
		return s.secret, nil
	})
	if err != nil {
		return err
	}
	claims, ok := token.Claims.(*ACL)
	if !ok || !token.Valid {
		return errors.New("token not valid")
	}
	ns := false
	for _, pattern := range claims.Namespaces {
		match, e := filepath.Match(pattern, metric)
		if e != nil {
			return e
		}
		if match {
			ns = true
			break
		}
	}
	if !ns {
		return errors.New("token has no access to metric")
	}
	for _, grant := range claims.Ops {
		if grant == "*" || grant == op {
			return nil
		}
	}
	return fmt.Errorf("token lacks %s permission", op)
}
func version(m chunkstore.Metadata) string {
	return fmt.Sprintf("%d:%d:%d", m.ID, m.Generation, m.Revision)
}
func stat(m chunkstore.Metadata) MetricData {
	size := int64(16 + 12*len(m.Retentions))
	for _, r := range m.Retentions {
		size += int64(r.NumberOfPoints()) * 12
	}
	return MetricData{Name: m.Name, Size: size, Mode: 0644, ModTime: 0, StorageVersion: version(m)}
}
func writeError(w http.ResponseWriter, err error, status int) { http.Error(w, err.Error(), status) }

func (s *Service) list(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" && r.Method != "POST" {
		writeError(w, errors.New("bad request method"), 400)
		return
	}
	if err := s.allowed("*", "read", r); err != nil {
		writeError(w, err, 403)
		return
	}
	prefix := r.FormValue("prefix")
	names := make([]string, 0)
	rx := r.FormValue("regex")
	var re *regexp.Regexp
	if rx != "" {
		compiled, err := regexp.Compile(rx)
		if err != nil {
			writeError(w, err, 400)
			return
		}
		re = compiled
	}
	var filter map[string]struct{}
	if raw := r.FormValue("list"); raw != "" {
		var values []string
		if err := json.Unmarshal([]byte(raw), &values); err != nil {
			writeError(w, err, 400)
			return
		}
		filter = make(map[string]struct{}, len(values))
		for _, value := range values {
			filter[value] = struct{}{}
		}
	}
	after := r.FormValue("after")
	for {
		page, err := s.store.ListPage(r.Context(), prefix, after, 10000)
		if err != nil {
			writeError(w, err, 500)
			return
		}
		for _, m := range page {
			if re == nil || re.MatchString(m.Name) {
				if filter == nil {
					names = append(names, m.Name)
				} else if _, ok := filter[m.Name]; ok {
					names = append(names, m.Name)
				}
			}
		}
		if len(page) < 10000 {
			break
		}
		after = page[len(page)-1].Name
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(names)
}

func (s *Service) metric(w http.ResponseWriter, r *http.Request) {
	name := strings.TrimPrefix(r.URL.Path, "/metrics/")
	if name == "" || strings.Contains(name, "/") {
		writeError(w, errors.New("metric name missing"), 400)
		return
	}
	op := map[string]string{"HEAD": "read", "GET": "read", "POST": "update", "PUT": "replace", "DELETE": "delete"}[r.Method]
	if op == "" {
		writeError(w, errors.New("bad request method"), 400)
		return
	}
	if err := s.allowed(name, op, r); err != nil {
		writeError(w, err, 403)
		return
	}
	switch r.Method {
	case "HEAD":
		s.head(w, r, name)
	case "GET":
		s.get(w, r, name)
	case "POST":
		s.receive(w, r, name, false)
	case "PUT":
		s.receive(w, r, name, true)
	case "DELETE":
		s.delete(w, r, name)
	}
}
func (s *Service) head(w http.ResponseWriter, r *http.Request, name string) {
	m, err := s.store.Metadata(r.Context(), name)
	if err != nil {
		writeError(w, err, 404)
		return
	}
	b, _ := json.Marshal(stat(m))
	w.Header().Set("X-Metric-Stat", string(b))
	w.Header().Set("X-Storage-Version", version(m))
	w.WriteHeader(200)
}
func (s *Service) get(w http.ResponseWriter, r *http.Request, name string) {
	select {
	case s.transfers <- struct{}{}:
		defer func() { <-s.transfers }()
	default:
		writeError(w, errors.New("too many concurrent transfers"), 429)
		return
	}
	snapshot, err := s.store.Snapshot(r.Context(), name)
	if err != nil {
		writeError(w, err, 404)
		return
	}
	file, err := os.CreateTemp(s.config.TmpDir, "buckyd-export-*.wsp")
	if err != nil {
		writeError(w, err, 500)
		return
	}
	path := file.Name()
	_ = file.Close()
	if err := os.Remove(path); err != nil {
		writeError(w, err, 500)
		return
	}
	defer os.Remove(path)
	if err := s.whisperIO.ExportSnapshot(r.Context(), snapshot, path); err != nil {
		writeError(w, err, 500)
		return
	}
	m := stat(snapshot.Metadata)
	info, err := os.Stat(path)
	if err != nil {
		writeError(w, err, 500)
		return
	}
	m.Size = info.Size()
	b, _ := json.Marshal(m)
	w.Header().Set("X-Metric-Stat", string(b))
	w.Header().Set("X-Storage-Version", version(snapshot.Metadata))
	servePath := path
	if strings.Contains(r.Header.Get("Accept-Encoding"), "snappy") {
		w.Header().Set("Content-Encoding", "snappy")
		compressed, err := os.CreateTemp(s.config.TmpDir, "buckyd-snappy-*.wsp")
		if err != nil {
			writeError(w, err, 500)
			return
		}
		defer os.Remove(compressed.Name())
		source, err := os.Open(path)
		if err == nil {
			writer := snappy.NewBufferedWriter(compressed)
			_, err = io.Copy(writer, source)
			closeErr := writer.Close()
			if err == nil {
				err = closeErr
			}
			_ = source.Close()
		}
		closeErr := compressed.Close()
		if err == nil {
			err = closeErr
		}
		if err != nil {
			writeError(w, err, 500)
			return
		}
		servePath = compressed.Name()
	}
	serve, err := os.Open(servePath)
	if err != nil {
		writeError(w, err, 500)
		return
	}
	defer serve.Close()
	http.ServeContent(w, r, name, time.Time{}, serve)
}
func (s *Service) receive(w http.ResponseWriter, r *http.Request, name string, replace bool) {
	select {
	case s.transfers <- struct{}{}:
		defer func() { <-s.transfers }()
	default:
		writeError(w, errors.New("too many concurrent transfers"), http.StatusTooManyRequests)
		return
	}
	if r.URL.Query().Get("fetch_offload") == "true" {
		s.offload(w, r, name, replace)
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, s.config.MaxBodyBytes)
	file, err := os.CreateTemp(s.config.TmpDir, "buckyd-import-*.wsp")
	if err != nil {
		writeError(w, err, 500)
		return
	}
	path := file.Name()
	defer os.Remove(path)
	defer file.Close()
	reader := io.Reader(r.Body)
	if encoding := r.Header.Get("Content-Encoding"); encoding == "snappy" {
		reader = snappy.NewReader(reader)
	} else if encoding != "" && encoding != "identity" {
		writeError(w, errors.New("unsupported content encoding"), 400)
		return
	}
	n, copyErr := io.Copy(file, io.LimitReader(reader, s.config.MaxBodyBytes+1))
	if copyErr != nil || n > s.config.MaxBodyBytes {
		_ = file.Close()
		writeError(w, errors.New("request body exceeds limit or is invalid"), 400)
		return
	}
	if header := r.Header.Get("X-Metric-Stat"); header != "" {
		var remote MetricData
		if err := json.Unmarshal([]byte(header), &remote); err != nil || remote.Size != n {
			_ = file.Close()
			writeError(w, errors.New("metric stat does not match request body"), 400)
			return
		}
	}
	if err := file.Close(); err != nil {
		writeError(w, err, 500)
		return
	}
	var m chunkstore.Metadata
	if replace {
		m, err = s.whisperIO.ImportWSP(r.Context(), name, path, true)
	} else {
		m, err = s.whisperIO.FillWSP(r.Context(), name, path)
	}
	if err != nil {
		if errors.Is(err, chunkstore.ErrConflict) {
			writeError(w, err, 409)
		} else {
			writeError(w, err, 500)
		}
		return
	}
	w.Header().Set("X-Storage-Version", version(m))
	s.changed()
	writeHealStats(w)
}

// offload copies one authorized metric through the same GET/POST wire format.
// Compatibility with buckytools uses a locally minted read token for the
// inter-buckyd GET; the caller is authorized only for the destination update.
func (s *Service) offload(w http.ResponseWriter, r *http.Request, name string, replace bool) {
	server, source := r.FormValue("server"), r.FormValue("metric")
	if source == "" {
		source = name
	}
	if server == "" {
		writeError(w, errors.New("offload server missing"), 400)
		return
	}
	u := url.URL{Scheme: "http", Host: server, Path: "/metrics/" + source}
	request, err := http.NewRequestWithContext(r.Context(), http.MethodGet, u.String(), nil)
	if err != nil {
		writeError(w, err, 400)
		return
	}
	if len(s.secret) > 0 {
		signed, err := s.offloadToken(source)
		if err != nil {
			writeError(w, err, 500)
			return
		}
		request.Header.Set(authHeader, signed)
	}
	request.Header.Set("Accept-Encoding", "snappy")
	client := &http.Client{
		Timeout: 30 * time.Second,
		// The source is supplied by the caller; keep its token on that request.
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}
	response, err := client.Do(request)
	if err != nil {
		writeError(w, fmt.Errorf("offload request: %w", err), 502)
		return
	}
	defer response.Body.Close()
	if response.StatusCode == http.StatusNotFound {
		writeError(w, errors.New("offload source metric not found"), http.StatusNotFound)
		return
	}
	if response.StatusCode != 200 {
		writeError(w, fmt.Errorf("offload source returned %s", response.Status), 502)
		return
	}
	var remote MetricData
	if err := json.Unmarshal([]byte(response.Header.Get("X-Metric-Stat")), &remote); err != nil {
		writeError(w, errors.New("offload source missing metric stat"), 502)
		return
	}
	if remote.Size < 0 || remote.Size > s.config.MaxBodyBytes {
		writeError(w, errors.New("offload source exceeds body limit"), http.StatusBadRequest)
		return
	}
	reader := io.Reader(response.Body)
	if encoding := response.Header.Get("Content-Encoding"); encoding == "snappy" {
		reader = snappy.NewReader(reader)
	} else if encoding != "" && encoding != "identity" {
		writeError(w, errors.New("unsupported offload content encoding"), http.StatusBadRequest)
		return
	}
	file, err := os.CreateTemp(s.config.TmpDir, "buckyd-offload-*.wsp")
	if err != nil {
		writeError(w, err, 500)
		return
	}
	path := file.Name()
	defer os.Remove(path)
	n, err := io.Copy(file, io.LimitReader(reader, s.config.MaxBodyBytes+1))
	closeErr := file.Close()
	if err != nil || closeErr != nil || n != remote.Size || n > s.config.MaxBodyBytes {
		writeError(w, errors.New("offload body does not match metric stat"), 400)
		return
	}
	var metadata chunkstore.Metadata
	if replace {
		metadata, err = s.whisperIO.ImportWSP(r.Context(), name, path, true)
	} else {
		metadata, err = s.whisperIO.FillWSP(r.Context(), name, path)
	}
	if err != nil {
		if errors.Is(err, chunkstore.ErrConflict) {
			writeError(w, err, http.StatusConflict)
		} else {
			writeError(w, err, http.StatusInternalServerError)
		}
		return
	}
	w.Header().Set("X-Storage-Version", version(metadata))
	s.changed()
	writeHealStats(w)
}

func writeHealStats(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(MetricHealStats{})
}

func (s *Service) offloadToken(metric string) (string, error) {
	// ACL namespaces are glob patterns; quote metacharacters to grant one metric.
	pattern := strings.NewReplacer("\\", "\\\\", "*", "\\*", "?", "\\?", "[", "\\[").Replace(metric)
	return jwt.NewWithClaims(jwt.SigningMethodHS256, ACL{
		RegisteredClaims: jwt.RegisteredClaims{ExpiresAt: jwt.NewNumericDate(time.Now().Add(5 * time.Minute))},
		Namespaces:       []string{pattern},
		Ops:              []string{"read"},
	}).SignedString(s.secret)
}
func (s *Service) delete(w http.ResponseWriter, r *http.Request, name string) {
	m, err := s.store.Metadata(r.Context(), name)
	if err != nil {
		writeError(w, err, 404)
		return
	}
	if expected := r.URL.Query().Get("version"); expected != "" && expected != version(m) {
		writeError(w, errors.New("storage version conflict"), 409)
		return
	}
	if err := s.store.DeleteIfUnchanged(r.Context(), name, m); err != nil {
		if errors.Is(err, chunkstore.ErrConflict) {
			writeError(w, err, 409)
		} else {
			writeError(w, err, 500)
		}
		return
	}
	s.changed()
	w.WriteHeader(http.StatusOK)
}
func (s *Service) hashring(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		writeError(w, errors.New("bad request method"), 400)
		return
	}
	if err := s.allowed("*", "read", r); err != nil {
		writeError(w, err, 403)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(struct {
		Name     string
		Nodes    []Node
		Algo     string
		Replicas int
	}{s.config.Node, s.nodes, s.config.Hash, s.config.Replicas})
}
