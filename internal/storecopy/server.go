// Package storecopy serves a whole-store checkpoint and its retained input log.
// It is mounted behind Buckyd's existing whole-store admin ACL.
package storecopy

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/internal/inputlog"
)

const Endpoint = "/admin/store-copy"
const MaxChunk = 64 << 20

type File struct {
	Name string `json:"name"`
	Size int64  `json:"size"`
}

type fileSource struct {
	path string
	size int64
}

type Manifest struct {
	Version         int                `json:"version"`
	ID              string             `json:"id"`
	Created         time.Time          `json:"created"`
	Expires         time.Time          `json:"expires"`
	Sealed          bool               `json:"sealed"`
	Input           inputlog.CopyState `json:"input"`
	CheckpointInput inputlog.CopyState `json:"checkpoint_input"`
	CheckpointWAL   []File             `json:"checkpoint_wal"`
	Files           []File             `json:"files"`
	Metadata        map[string]any     `json:"metadata,omitempty"`
}

type Manager struct {
	mu          sync.RWMutex
	store       *chunkstore.Store
	log         *inputlog.Log
	logDir      string
	parent      string
	ready       func() error
	seal        func() error
	dir         string
	manifest    *Manifest
	paths       map[string]fileSource
	timer       *time.Timer
	slots       chan struct{}
	metadata    map[string]any
	configFiles map[string]string
	prefix      string
	minFree     atomic.Int64
	watchdog    *time.Timer
}

func (m *Manager) SetMinimumFreeBytes(bytes int64) { m.minFree.Store(bytes) }

func (m *Manager) watch(id string) {
	err := CheckFreeSpace(m.parent, m.minFree.Load())
	if stats := m.log.Metrics(); stats.Unlogged != 0 || stats.WriteErrors != 0 {
		err = errors.New("input log durability guard failed")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.manifest == nil || m.manifest.ID != id {
		return
	}
	if err != nil {
		cleanupErr := m.closeLocked()
		log.Printf("store copy %s aborted: %v; cleanup: %v", id, err, cleanupErr)
		return
	}
	m.watchdog = time.AfterFunc(5*time.Second, func() { m.watch(id) })
}

// SetMetadata supplies immutable effective settings and policy files before the
// listener starts. Secrets and host-specific runtime state are not exported.
func (m *Manager) SetMetadata(metadata map[string]any, files map[string]string) {
	m.metadata, m.configFiles = metadata, files
	if source, ok := metadata["store_dir"].(string); ok {
		m.prefix = ".store-copy-" + filepath.Base(source) + "-"
	}
}

func (m *Manager) SetRingIdentity(node, hash string, replicas int, nodes []string) {
	if m.metadata == nil {
		m.metadata = make(map[string]any)
	}
	m.metadata["node"], m.metadata["hash"], m.metadata["replicas"], m.metadata["nodes"] = node, hash, replicas, nodes
}

// CleanupOrphans runs only after the App has obtained the live store's lock.
// A process crash loses its timer; OWNER prevents removing another store's files.
func (m *Manager) CleanupOrphans() error {
	if m.prefix == "" {
		return nil
	}
	entries, err := os.ReadDir(m.parent)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if !entry.IsDir() || !strings.HasPrefix(entry.Name(), m.prefix) {
			continue
		}
		suffix := strings.TrimPrefix(entry.Name(), m.prefix)
		if decoded, err := hex.DecodeString(suffix); err != nil || len(decoded) != 16 {
			continue
		}
		dir := filepath.Join(m.parent, entry.Name())
		owner, err := os.ReadFile(filepath.Join(dir, "OWNER"))
		if err == nil && string(owner) == fmt.Sprint(m.metadata["store_dir"]) {
			if err := os.RemoveAll(dir); err != nil {
				return err
			}
		}
	}
	return nil
}

func New(s *chunkstore.Store, l *inputlog.Log, logDir, parent string, ready, seal func() error) *Manager {
	return &Manager{store: s, log: l, logDir: logDir, parent: parent, ready: ready, seal: seal, slots: make(chan struct{}, 64)}
}

func (m *Manager) Active() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.manifest != nil
}

func (m *Manager) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.closeLocked()
}

func (m *Manager) closeLocked() error {
	if m.watchdog != nil {
		m.watchdog.Stop()
	}
	if m.timer != nil {
		m.timer.Stop()
	}
	var err error
	if m.dir != "" {
		err = os.RemoveAll(m.dir)
	}
	if m.manifest != nil {
		if m.log != nil {
			err = errors.Join(err, m.log.ReleaseCopy())
		}
	}
	m.dir, m.manifest, m.paths = "", nil, nil
	return err
}

func (m *Manager) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	if r.URL.Path == Endpoint+"/file" {
		m.file(w, r)
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	var err error
	if m.manifest != nil && m.log != nil && r.Method != http.MethodDelete {
		if stats := m.log.Metrics(); stats.Unlogged != 0 || stats.WriteErrors != 0 {
			http.Error(w, "input log durability guard failed", http.StatusConflict)
			return
		}
	}
	switch {
	case r.URL.Path == Endpoint && r.Method == http.MethodPost:
		err = m.create()
	case m.manifest == nil:
		http.Error(w, "no active copy; create a new checkpoint", http.StatusGone)
		return
	case r.URL.Query().Get("id") != m.manifest.ID:
		http.Error(w, "copy ID does not match", http.StatusConflict)
		return
	case r.URL.Path == Endpoint && r.Method == http.MethodDelete:
		err = m.closeLocked()
	case (r.URL.Path == Endpoint+"/capture" || r.URL.Path == Endpoint+"/seal") && r.Method == http.MethodPost:
		if r.URL.Path == Endpoint+"/seal" && !m.manifest.Sealed {
			if m.seal == nil {
				err = errors.New("input fencing is unavailable")
			} else {
				err = m.seal()
			}
			if err != nil {
				break
			}
			m.manifest.Sealed = true
		}
		var state inputlog.CopyState
		state, err = m.log.CaptureCopy()
		if err == nil {
			m.setInput(state)
		}
	case r.URL.Path == Endpoint && r.Method == http.MethodGet:
	default:
		http.Error(w, "unsupported copy request", http.StatusMethodNotAllowed)
		return
	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusConflict)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(m.manifest)
}

func (m *Manager) create() (err error) {
	if m.manifest != nil {
		return errors.New("a checkpoint already owns input-log retention")
	}
	if m.log == nil {
		return errors.New("whole-store copy requires enabled input-log")
	}
	if err := CheckFreeSpace(m.parent, m.minFree.Load()); err != nil {
		return err
	}
	if m.ready != nil {
		if err := m.ready(); err != nil {
			return err
		}
	}
	state, err := m.log.PinCopy()
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			_ = m.log.ReleaseCopy()
			_ = os.RemoveAll(m.dir)
			m.dir = ""
		}
	}()
	if stats := m.log.Metrics(); stats.Unlogged != 0 || stats.WriteErrors != 0 {
		return errors.New("input log has unlogged points or write errors; restart only after recovering them")
	}
	id := make([]byte, 16)
	if _, err = rand.Read(id); err != nil {
		return err
	}
	name := hex.EncodeToString(id)
	prefix := m.prefix
	if prefix == "" {
		prefix = ".store-copy-"
	}
	m.dir = filepath.Join(m.parent, prefix+name)
	if err = os.Mkdir(m.dir, 0700); err != nil {
		return err
	}
	if err = os.WriteFile(filepath.Join(m.dir, "OWNER"), []byte(fmt.Sprint(m.metadata["store_dir"])), 0600); err != nil {
		return err
	}
	if err = m.store.Checkpoint(filepath.Join(m.dir, "store")); err != nil {
		return err
	}
	m.paths = make(map[string]fileSource)
	manifest := &Manifest{Version: 1, ID: name, Created: time.Now().UTC(), Metadata: m.metadata, CheckpointInput: state}
	manifest.Expires = manifest.Created.Add(6 * time.Hour)
	entries, err := os.ReadDir(filepath.Join(m.dir, "store"))
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if entry.Name() == "LOCK" {
			continue
		}
		info, e := entry.Info()
		if e != nil {
			return e
		}
		if !info.Mode().IsRegular() {
			return fmt.Errorf("unsupported checkpoint entry %s", entry.Name())
		}
		path := "store/" + entry.Name()
		manifest.Files = append(manifest.Files, File{path, info.Size()})
		if strings.HasSuffix(entry.Name(), ".log") {
			manifest.CheckpointWAL = append(manifest.CheckpointWAL, File{path, info.Size()})
		}
		m.paths[path] = fileSource{filepath.Join(m.dir, filepath.FromSlash(path)), info.Size()}
	}
	if len(m.configFiles) != 0 {
		if err = os.Mkdir(filepath.Join(m.dir, "config"), 0700); err != nil {
			return err
		}
	}
	for name, path := range m.configFiles {
		if path == "" {
			continue
		}
		data, e := os.ReadFile(path)
		if e != nil {
			return e
		}
		file := "config/" + name
		dest := filepath.Join(m.dir, filepath.FromSlash(file))
		if err = os.WriteFile(dest, data, 0600); err != nil {
			return err
		}
		m.paths[file] = fileSource{dest, int64(len(data))}
		manifest.Files = append(manifest.Files, File{file, int64(len(data))})
	}
	m.manifest = manifest
	m.setInput(state)
	m.timer = time.AfterFunc(time.Until(manifest.Expires), func() {
		m.mu.Lock()
		defer m.mu.Unlock()
		if m.manifest != nil && m.manifest.ID == name {
			_ = m.closeLocked()
		}
	})
	m.watchdog = time.AfterFunc(5*time.Second, func() { m.watch(name) })
	return nil
}

func (m *Manager) setInput(state inputlog.CopyState) {
	files := m.manifest.Files[:0]
	for _, file := range m.manifest.Files {
		if !strings.HasPrefix(file.Name, "input-log/") {
			files = append(files, file)
		}
	}
	for _, file := range state.Files {
		name := "input-log/" + file.Name
		files = append(files, File{name, file.Size})
		m.paths[name] = fileSource{filepath.Join(m.logDir, file.Name), file.Size}
	}
	m.manifest.Files, m.manifest.Input = files, state
}

func (m *Manager) file(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "GET required", http.StatusMethodNotAllowed)
		return
	}
	select {
	case m.slots <- struct{}{}:
		defer func() { <-m.slots }()
	case <-r.Context().Done():
		return
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.manifest == nil || r.URL.Query().Get("id") != m.manifest.ID {
		http.Error(w, "copy has expired or ID does not match", http.StatusGone)
		return
	}
	if m.log != nil {
		if stats := m.log.Metrics(); stats.Unlogged != 0 || stats.WriteErrors != 0 {
			http.Error(w, "input log durability guard failed", http.StatusConflict)
			return
		}
	}
	name := r.URL.Query().Get("name")
	source, ok := m.paths[name]
	size := source.size
	offset, e1 := strconv.ParseInt(r.URL.Query().Get("offset"), 10, 64)
	length, e2 := strconv.ParseInt(r.URL.Query().Get("length"), 10, 64)
	if !ok || e1 != nil || e2 != nil || offset < 0 || length <= 0 || length > MaxChunk || length > size || offset > size-length {
		http.Error(w, "unknown file or invalid range", http.StatusBadRequest)
		return
	}
	f, err := os.Open(source.path)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer f.Close()
	controller := http.NewResponseController(w)
	_ = controller.SetWriteDeadline(time.Now().Add(10 * time.Second))
	defer controller.SetWriteDeadline(time.Time{})
	w.Header().Set("Trailer", "X-Content-SHA256")
	w.Header().Set("Content-Type", "application/octet-stream")
	hash := sha256.New()
	n, err := io.CopyBuffer(io.MultiWriter(w, hash), io.NewSectionReader(f, offset, length), make([]byte, 256<<10))
	if err == nil && n == length {
		w.Header().Set("X-Content-SHA256", hex.EncodeToString(hash.Sum(nil)))
	}
}
