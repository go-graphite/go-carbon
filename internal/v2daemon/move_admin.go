package v2daemon

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-graphite/go-carbon/internal/v2core"
	"github.com/go-graphite/go-carbon/internal/v2part"
	"github.com/lomik/zapwriter"
	"go.uber.org/zap"
)

const MoveRecordVersion = 1

type MoveRequest struct {
	Parts []v2core.MovePart
	Now   int64
}
type MoveCopyRequest struct {
	Snapshot v2core.PartSnapshot
	Now      int64
}
type MoveFileStatus struct {
	Name     string
	Size     int64
	Checksum string
	State    string
	Owners   []string
}
type MoveOwnerStatus struct {
	State                 string
	Reason                string
	PartsDone, PartsTotal int
	BytesDone, BytesTotal int64
}
type MoveStatus struct {
	ID                    string
	Done                  bool
	Phase                 string
	PartsDone, PartsTotal int
	BytesDone, BytesTotal int64
	MBps, ETASeconds      float64
	BytesTransferred      int64
	FailureCount          uint64
	VerifyCount           uint64
	Failures              []string
	Owners                []MoveOwnerStatus
	Parts                 []MoveFileStatus `json:",omitempty"`
}
type moveRecord struct {
	Version int
	MoveStatus
	Request           MoveRequest
	Started, Finished int64
}

// The operation mutex excludes concurrent writers. Readers use atomic disk
// snapshots, so status and collection remain available during long transfers.
type MoveAdmin struct {
	mu           sync.Mutex
	root         string
	store        *Store
	onOwner      func(int, func() error) error
	namesVisible func([]string)
	rejected     atomic.Uint64
}

// SetNamesVisible attaches the application's discovery projection. Notifications
// follow successful adoption, including copied names that never enter intake.
func (a *MoveAdmin) SetNamesVisible(notify func([]string)) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.namesVisible = notify
}

func NewMoveAdmin(root string, store *Store, onOwner func(int, func() error) error) *MoveAdmin {
	a := &MoveAdmin{root: filepath.Join(root, "moves"), store: store, onOwner: onOwner}
	entries, _ := os.ReadDir(a.root)
	for _, entry := range entries {
		if !entry.IsDir() || !validMoveID(entry.Name()) {
			continue
		}
		m, e := a.load(entry.Name())
		if errors.Is(e, os.ErrNotExist) {
			if importErr := a.importLegacy(entry.Name()); importErr == nil {
				m, e = a.load(entry.Name())
			} else if !errors.Is(importErr, os.ErrNotExist) {
				zapwriter.Logger("v2-move").Error("legacy move import failed", zap.Error(importErr))
			}
		}
		if e == nil && m.Done {
			_ = a.cleanup(m)
		}
	}
	return a
}
func moveRank(state string) int {
	switch state {
	case "pending":
		return 0
	case "received":
		return 1
	case "verified":
		return 2
	case "staged":
		return 3
	case "active":
		return 4
	case "done":
		return 5
	}
	return -1
}
func validMoveID(id string) bool {
	b, e := hex.DecodeString(id)
	return e == nil && len(b) == 32 && hex.EncodeToString(b) == id
}
func (a *MoveAdmin) load(id string) (*moveRecord, error) {
	if !validMoveID(id) {
		return nil, errors.New("invalid move ID")
	}
	b, e := os.ReadFile(filepath.Join(a.root, id, "record.json"))
	if e != nil {
		return nil, e
	}
	var m moveRecord
	if e = json.Unmarshal(b, &m); e != nil {
		return nil, e
	}
	if m.Version != MoveRecordVersion || m.ID != id {
		return nil, errors.New("unsupported move record version or identity")
	}
	return &m, nil
}
func (a *MoveAdmin) save(m *moveRecord) error {
	if m.Version != MoveRecordVersion || !validMoveID(m.ID) {
		return errors.New("unsupported move record")
	}
	old, e := a.load(m.ID)
	if e == nil {
		regression := moveRank(m.Phase) < moveRank(old.Phase) || m.BytesDone < old.BytesDone || m.PartsDone < old.PartsDone || m.BytesTransferred < old.BytesTransferred || m.VerifyCount < old.VerifyCount || old.Done && !m.Done || m.PartsTotal != old.PartsTotal || m.BytesTotal != old.BytesTotal || m.Request.Now != old.Request.Now
		if !m.Done {
			if len(m.Parts) != len(old.Parts) || len(m.Owners) != len(old.Owners) {
				regression = true
			} else {
				for i, p := range m.Parts {
					q := old.Parts[i]
					if len(p.Owners) != len(q.Owners) {
						regression = true
					}
					if p.Name != q.Name || p.Size != q.Size || p.Checksum != q.Checksum || moveRank(p.State) < moveRank(q.State) {
						regression = true
					}
					for j, s := range p.Owners {
						if j >= len(q.Owners) || moveRank(s) < moveRank(q.Owners[j]) {
							regression = true
						}
					}
				}
				for i, o := range m.Owners {
					if moveRank(o.State) < moveRank(old.Owners[i].State) {
						regression = true
					}
				}
			}
		}
		if regression {
			a.rejected.Add(1)
			zapwriter.Logger("v2-move").Error("move progress regression refused", zap.String("move", m.ID))
			return errors.New("move progress regression refused")
		}
	} else if !errors.Is(e, os.ErrNotExist) {
		return e
	}
	b, e := json.Marshal(m)
	if e != nil {
		return e
	}
	return writeMoveFile(filepath.Join(a.root, m.ID), "record.json", b)
}
func (m *moveRecord) status() MoveStatus {
	s := m.MoveStatus
	s.Owners = append([]MoveOwnerStatus(nil), m.Owners...)
	if len(m.Parts) > 0 {
		for i := range s.Owners {
			s.Owners[i].PartsTotal = m.PartsTotal
			s.Owners[i].BytesTotal = m.BytesTotal
			s.Owners[i].PartsDone = 0
			s.Owners[i].BytesDone = 0
			for _, p := range m.Parts {
				if moveRank(p.Owners[i]) >= 3 {
					s.Owners[i].PartsDone++
					s.Owners[i].BytesDone += p.Size
				}
			}
		}
	}
	end := time.Now().UnixNano()
	if m.Finished != 0 {
		end = m.Finished
	}
	seconds := float64(end-m.Started) / 1e9
	if seconds > 0 {
		s.MBps = float64(m.BytesDone) / 1e6 / seconds
	}
	if s.MBps > 0 && !s.Done {
		s.ETASeconds = float64(s.BytesTotal-s.BytesDone) / 1e6 / s.MBps
	}
	return s
}
func (a *MoveAdmin) create(id string, now int64, files []MoveFileStatus) (*moveRecord, error) {
	if m, e := a.load(id); e == nil {
		if now != 0 && now != m.Request.Now {
			return nil, errors.New("move adoption clock changed")
		}
		return m, nil
	} else if !errors.Is(e, os.ErrNotExist) {
		return nil, e
	}
	if now == 0 {
		now = time.Now().Unix()
	}
	if now < 1 || now > 1<<32-1 {
		return nil, errors.New("invalid move clock")
	}
	m := &moveRecord{Version: MoveRecordVersion, MoveStatus: MoveStatus{ID: id, Phase: "pending", Parts: files, PartsTotal: len(files)}, Request: MoveRequest{Now: now}, Started: time.Now().UnixNano()}
	for range a.store.Core.DBs() {
		m.Owners = append(m.Owners, MoveOwnerStatus{State: "pending"})
	}
	for i := range m.Parts {
		m.BytesTotal += m.Parts[i].Size
		for range m.Owners {
			m.Parts[i].Owners = append(m.Parts[i].Owners, "pending")
		}
	}
	// Upgrade the existing root format before publishing move state. Old
	// daemons already reject this restart version before opening writable stores.
	if e := a.requireMoveVersion(); e != nil {
		return nil, e
	}
	if e := os.MkdirAll(filepath.Join(a.root, id), 0700); e != nil {
		return nil, e
	}
	if e := a.save(m); e != nil {
		return nil, e
	}
	if e := syncDir(filepath.Dir(a.root)); e != nil {
		return nil, e
	}
	return m, nil
}
func (a *MoveAdmin) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	action := strings.TrimPrefix(r.URL.Path, "/admin/v2/move/")
	if action != "status" {
		a.mu.Lock()
		defer a.mu.Unlock()
	}
	var m *moveRecord
	err := func() error {
		if action == "begin" || action == "prepare" {
			if r.Method != http.MethodPost {
				return errors.New("move creation requires POST")
			}
			var files []MoveFileStatus
			var request MoveRequest
			var b []byte
			if action == "begin" {
				var q MoveCopyRequest
				if e := json.NewDecoder(http.MaxBytesReader(w, r.Body, 16<<20)).Decode(&q); e != nil {
					return e
				}
				if e := q.Snapshot.Validate(); e != nil {
					return e
				}
				if len(q.Snapshot.Parts) == 0 {
					return errors.New("empty move inventory")
				}
				request.Now = q.Now
				b, _ = json.Marshal(q.Snapshot)
				for _, p := range q.Snapshot.Parts {
					files = append(files, MoveFileStatus{Name: p.ID, Size: p.Bytes, Checksum: p.SHA256, State: "pending"})
				}
			} else {
				if e := json.NewDecoder(http.MaxBytesReader(w, r.Body, 16<<20)).Decode(&request); e != nil {
					return e
				}
				if len(request.Parts) == 0 {
					return errors.New("empty move inventory")
				}
				b, _ = json.Marshal(request)
				for _, p := range request.Parts {
					if e := p.Descriptor.Validate(); e != nil {
						return e
					}
					st, e := os.Stat(p.Path)
					if e != nil {
						return e
					}
					id := fmt.Sprintf("%x", p.Descriptor.SHA256)
					files = append(files, MoveFileStatus{Name: id, Size: st.Size(), Checksum: id, State: "pending"})
				}
			}
			digest := sha256.Sum256(b)
			id := hex.EncodeToString(digest[:])
			var e error
			m, e = a.create(id, request.Now, files)
			if e != nil {
				return e
			}
			if m.Done {
				return a.cleanup(m)
			}
			if action == "prepare" {
				request.Now = m.Request.Now
				if len(m.Request.Parts) == 0 {
					m.Request = request
					m.Request.Parts = append([]v2core.MovePart(nil), request.Parts...)
					for i := range m.Request.Parts {
						m.Request.Parts[i].Path = filepath.Join(a.root, id, files[i].Name+".part")
					}
					if e = a.save(m); e != nil {
						return e
					}
				}
				for i, p := range request.Parts {
					if moveRank(m.Parts[i].State) >= 2 {
						continue
					}
					target := filepath.Join(a.root, id, m.Parts[i].Name+".part")
					if e = os.Link(p.Path, target); e != nil && !errors.Is(e, os.ErrExist) {
						return e
					}
					if e = syncDir(filepath.Dir(target)); e != nil {
						return e
					}
					m.Parts[i].State = "received"
					if m.Phase == "pending" {
						m.Phase = "received"
					}
					for j := range m.Parts[i].Owners {
						m.Parts[i].Owners[j] = "received"
					}
					if e = a.save(m); e != nil {
						return e
					}
					if e = v2core.VerifyMovePart(r.Context(), target, p.Descriptor); e != nil {
						return e
					}
					m.VerifyCount++
					if e = a.verified(m, i); e != nil {
						return e
					}
				}
				for i := range request.Parts {
					request.Parts[i].Path = filepath.Join(a.root, id, files[i].Name+".part")
				}
				m.Request.Parts = request.Parts
				if e = a.save(m); e != nil {
					return e
				}
				// Preserve prepare's original boundary: build the offline read index
				// before returning, outside the serial owner's activation window.
				prepared, e := v2core.PrepareVerifiedMove(r.Context(), filepath.Join(a.root, id), m.Request.Parts)
				if e != nil {
					return e
				}
				return prepared.Close()
			}
			return nil
		}
		var e error
		m, e = a.load(r.URL.Query().Get("id"))
		if e != nil {
			return e
		}
		switch action {
		case "status":
			if r.Method != http.MethodGet {
				return errors.New("status requires GET")
			}
			return nil
		case "part":
			if r.Method != http.MethodPut {
				return errors.New("part requires PUT")
			}
			if m.Done {
				return a.cleanup(m)
			}
			for i, p := range m.Parts {
				if p.Name == r.URL.Query().Get("part") {
					if moveRank(p.State) >= 2 {
						return nil
					}
					return a.receive(r.Context(), m, i, r.Body)
				}
			}
			return errors.New("unknown move part")
		case "adopt":
			if r.Method != http.MethodPost {
				return errors.New("adopt requires POST")
			}
			if m.Done {
				return a.cleanup(m)
			}
			if m.PartsDone != m.PartsTotal {
				return errors.New("move still has unverified parts")
			}
			if len(m.Request.Parts) == 0 {
				return errors.New("no move descriptors")
			}
			p, e := v2core.PrepareVerifiedMove(r.Context(), filepath.Join(a.root, m.ID), m.Request.Parts)
			if e != nil {
				return e
			}
			p.SkipOwner = func(owner int) bool { return m.Owners[owner].State == "active" }
			p.Progress = func(owner int, checksum [32]byte, state string) error {
				if state == "active" {
					m.Owners[owner].State = "active"
					m.Owners[owner].Reason = ""
					for i := range m.Parts {
						m.Parts[i].Owners[owner] = "active"
					}
				} else {
					name := fmt.Sprintf("%x", checksum)
					for i := range m.Parts {
						if m.Parts[i].Name == name {
							m.Parts[i].Owners[owner] = state
						}
					}
					m.Owners[owner].State = "staged"
				}
				for i := range m.Parts {
					state := "active"
					for _, ownerState := range m.Parts[i].Owners {
						if moveRank(ownerState) < moveRank(state) {
							state = ownerState
						}
					}
					if moveRank(state) > moveRank(m.Parts[i].State) {
						m.Parts[i].State = state
					}
				}
				allActive := true
				for _, o := range m.Owners {
					if o.State != "active" {
						allActive = false
					}
				}
				if allActive {
					m.Phase = "active"
				}
				if moveRank(m.Phase) < 3 {
					m.Phase = "staged"
				}
				if e := a.save(m); e != nil {
					return e
				}
				if state == "active" {
					a.boundary("move-owner-active")
				} else {
					a.boundary("move-progress-staged")
				}
				return nil
			}
			e = a.adopt(r.Context(), p, m.Request.Now)
			e = errors.Join(e, p.Close())
			if e != nil {
				return e
			}
			m.MoveStatus = m.status()
			m.Done = true
			m.Phase = "done"
			m.Finished = time.Now().UnixNano()
			m.Parts = nil
			m.Request.Parts = nil
			if e = a.save(m); e != nil {
				return e
			}
			return a.cleanup(m)
		default:
			return errors.New("unknown move action")
		}
	}()
	if err != nil {
		if m != nil && action != "status" {
			m.FailureCount++
			m.Failures = append(m.Failures, err.Error())
			if len(m.Failures) > 16 {
				m.Failures = m.Failures[len(m.Failures)-16:]
			}
			for i := range m.Owners {
				if m.Owners[i].State != "active" {
					m.Owners[i].Reason = err.Error()
					break
				}
			}
			_ = a.save(m)
		}
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(m.status())
}
func (a *MoveAdmin) verified(m *moveRecord, i int) error {
	m.Parts[i].State = "verified"
	for j := range m.Parts[i].Owners {
		m.Parts[i].Owners[j] = "verified"
	}
	m.PartsDone++
	m.BytesDone += m.Parts[i].Size
	if m.PartsDone == m.PartsTotal {
		m.Phase = "verified"
		for i := range m.Owners {
			m.Owners[i].State = "verified"
		}
	}
	if e := a.save(m); e != nil {
		return e
	}
	a.boundary("move-verified-part")
	return nil
}
func (a *MoveAdmin) boundary(stage string) {
	if a.store.cfg.Hooks.Stage != nil {
		a.store.cfg.Hooks.Stage(stage)
	}
}
func (a *MoveAdmin) receive(ctx context.Context, m *moveRecord, i int, body io.Reader) error {
	p := m.Parts[i]
	dir := filepath.Join(a.root, m.ID)
	path := filepath.Join(dir, p.Name+".part")
	stage := path + ".partial"
	if p.State != "received" {
		f, e := os.OpenFile(stage, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0600)
		if e != nil {
			return e
		}
		h := sha256.New()
		n, e := io.Copy(io.MultiWriter(f, h), io.LimitReader(body, p.Size+1))
		e = errors.Join(e, f.Sync(), f.Close())
		m.BytesTransferred += n
		if e != nil {
			return e
		}
		if n != p.Size || hex.EncodeToString(h.Sum(nil)) != p.Checksum {
			return errors.New("move part checksum or size mismatch")
		}
		if e = os.Rename(stage, path); e != nil {
			return e
		}
		if e = syncDir(dir); e != nil {
			return e
		}
		m.Parts[i].State = "received"
		for j := range m.Parts[i].Owners {
			m.Parts[i].Owners[j] = "received"
		}
		if m.Phase == "pending" {
			m.Phase = "received"
			for j := range m.Owners {
				m.Owners[j].State = "received"
			}
		}
		if e = a.save(m); e != nil {
			return e
		}
		a.boundary("move-received-part")
	}
	return a.verifyReceived(ctx, m)
}
func (a *MoveAdmin) verifyReceived(ctx context.Context, m *moveRecord) error {
	dir := filepath.Join(a.root, m.ID)
	known := map[string]bool{}
	for _, p := range m.Request.Parts {
		known[fmt.Sprintf("%x", p.Descriptor.SHA256)] = true
	}
	for i, p := range m.Parts {
		if p.State != "received" || known[p.Name] || p.Size > v2part.MaxDescriptorBytes {
			continue
		}
		b, e := os.ReadFile(filepath.Join(dir, p.Name+".part"))
		if e != nil {
			return e
		}
		d, e := v2part.ParseDescriptor(b)
		if e != nil {
			continue
		}
		name := fmt.Sprintf("%x", d.SHA256)
		found := false
		for _, q := range m.Parts {
			if q.Name == name {
				found = true
				break
			}
		}
		if !found {
			return errors.New("move descriptor lacks SST")
		}
		if known[name] {
			for _, prior := range m.Request.Parts {
				if fmt.Sprintf("%x", prior.Descriptor.SHA256) == name {
					old, _ := json.Marshal(prior.Descriptor)
					next, _ := json.Marshal(d)
					if string(old) != string(next) {
						return errors.New("move descriptor changed for immutable part")
					}
				}
			}
		}
		if !known[name] {
			m.Request.Parts = append(m.Request.Parts, v2core.MovePart{Path: filepath.Join(dir, name+".part"), Descriptor: d})
			known[name] = true
		}
		m.VerifyCount++
		if e = a.verified(m, i); e != nil {
			return e
		}
	}
	for _, part := range m.Request.Parts {
		name := fmt.Sprintf("%x", part.Descriptor.SHA256)
		for i, p := range m.Parts {
			if p.Name != name || p.State != "received" {
				continue
			}
			if e := v2core.VerifyMovePart(ctx, part.Path, part.Descriptor); e != nil {
				return e
			}
			m.VerifyCount++
			if e := a.verified(m, i); e != nil {
				return e
			}
		}
	}
	return nil
}
func (a *MoveAdmin) cleanup(m *moveRecord) error {
	dir := filepath.Join(a.root, m.ID)
	entries, e := os.ReadDir(dir)
	if e != nil {
		return e
	}
	for _, entry := range entries {
		if entry.Name() == "record.json" {
			continue
		}
		if e = os.RemoveAll(filepath.Join(dir, entry.Name())); e != nil {
			return e
		}
	}
	return syncDir(dir)
}
func (a *MoveAdmin) adopt(ctx context.Context, p *v2core.PreparedMove, now int64) error {
	if a.store.cfg.Layer != 3 {
		return errors.New("move requires unplaced L3 daemon")
	}
	ids := map[string]uint64{}
	for i, producer := range a.store.Producers {
		if e := a.onOwner(i, func() error {
			if producer.failed != nil {
				return producer.failed
			}
			local, e := producer.prepareMoveIDs(p)
			if e != nil {
				return e
			}
			for name, id := range local {
				ids[name] = id
			}
			return nil
		}); e != nil {
			return e
		}
	}
	if err := a.onOwner(0, func() error { return a.store.Core.AdoptMove(ctx, p, ids, now) }); err != nil {
		return err
	}
	if a.namesVisible != nil {
		names := make([]string, 0, len(ids))
		for name := range ids {
			names = append(names, name)
		}
		a.namesVisible(names)
	}
	return nil
}
func writeMoveFile(dir, name string, b []byte) error {
	f, e := os.CreateTemp(dir, ".move-")
	if e != nil {
		return e
	}
	defer os.Remove(f.Name())
	_, e = f.Write(b)
	e = errors.Join(e, f.Sync(), f.Close())
	if e != nil {
		return e
	}
	if e = os.Rename(f.Name(), filepath.Join(dir, name)); e != nil {
		return e
	}
	if e = syncDir(dir); e != nil {
		return e
	}
	return syncDir(filepath.Dir(dir))
}
