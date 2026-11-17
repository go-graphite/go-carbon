package v2daemon

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/go-graphite/go-carbon/internal/v2core"
)

// MoveRequest persists the verified inventory and original adoption clock.
// Paths are local files already received by the whole-file transport.
type MoveRequest struct {
	Parts []v2core.MovePart
	Now   int64
}
type MoveStatus struct {
	ID   string
	Done bool
}

// MoveAdmin uses the daemon's HTTP listener and existing serial producer
// callers. Its mutex serializes administrative calls only, never intake.
type MoveAdmin struct {
	mu      sync.Mutex
	root    string
	store   *Store
	onOwner func(int, func() error) error
}

func NewMoveAdmin(root string, store *Store, onOwner func(int, func() error) error) *MoveAdmin {
	return &MoveAdmin{root: filepath.Join(root, "moves"), store: store, onOwner: onOwner}
}
func (a *MoveAdmin) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	a.mu.Lock()
	defer a.mu.Unlock()
	var result MoveStatus
	err := func() error {
		action := strings.TrimPrefix(r.URL.Path, "/admin/v2/move/")
		if action == "prepare" {
			if r.Method != http.MethodPost {
				return errors.New("prepare requires POST")
			}
			var request MoveRequest
			if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 16<<20)).Decode(&request); err != nil {
				return err
			}
			if request.Now == 0 {
				request.Now = time.Now().Unix()
			}
			if request.Now < 1 || request.Now > 1<<32-1 {
				return errors.New("invalid move clock")
			}
			b, err := json.Marshal(request)
			if err != nil {
				return err
			}
			digest := sha256.Sum256(b)
			result.ID = hex.EncodeToString(digest[:])
			dir := filepath.Join(a.root, result.ID)
			if err = os.MkdirAll(dir, 0700); err != nil {
				return err
			}
			p, err := v2core.PrepareMove(r.Context(), dir, request.Parts)
			if err != nil {
				return err
			}
			if err = p.Close(); err != nil {
				return err
			}
			// The prepared inventory owns retained links. Transport paths may
			// disappear as soon as prepare acknowledges, including after restart.
			for i := range request.Parts {
				request.Parts[i].Path = filepath.Join(dir, fmt.Sprintf("%x.sst", request.Parts[i].Descriptor.SHA256))
			}
			b, err = json.Marshal(request)
			if err != nil {
				return err
			}
			if err = writeMoveFile(dir, "request.json", b); err != nil {
				return err
			}
			return syncDir(filepath.Dir(a.root))
		}
		id := r.URL.Query().Get("id")
		if b, err := hex.DecodeString(id); err != nil || len(b) != 32 || hex.EncodeToString(b) != id {
			return errors.New("invalid move ID")
		}
		result.ID = id
		dir := filepath.Join(a.root, id)
		data, err := os.ReadFile(filepath.Join(dir, "request.json"))
		if err != nil {
			return err
		}
		var request MoveRequest
		if err = json.Unmarshal(data, &request); err != nil {
			return err
		}
		if _, err = os.Stat(filepath.Join(dir, "done")); err == nil {
			result.Done = true
		} else if !errors.Is(err, os.ErrNotExist) {
			return err
		}
		switch action {
		case "status":
			if r.Method != http.MethodGet {
				return errors.New("status requires GET")
			}
			return nil
		case "adopt":
			if r.Method != http.MethodPost {
				return errors.New("adopt requires POST")
			}
			if result.Done {
				return nil
			}
			p, err := v2core.PrepareMove(r.Context(), dir, request.Parts)
			if err != nil {
				return err
			}
			defer p.Close()
			if err = a.adopt(r.Context(), p, request.Now); err != nil {
				return err
			}
			if err = writeMoveFile(dir, "done", []byte("done\n")); err != nil {
				return err
			}
			result.Done = true
			return nil
		default:
			return fmt.Errorf("unknown move action %q", action)
		}
	}()
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(result)
}
func (a *MoveAdmin) adopt(ctx context.Context, p *v2core.PreparedMove, now int64) error {
	if a.store.cfg.Layer != 3 || a.store.cfg.Placement != nil {
		return errors.New("move requires unplaced L3 daemon")
	}
	ids := map[string]uint64{}
	for i, producer := range a.store.Producers {
		err := a.onOwner(i, func() error {
			if producer.failed != nil {
				return producer.failed
			}
			local, err := producer.prepareMoveIDs(p)
			if err != nil {
				return err
			}
			for name, id := range local {
				ids[name] = id
			}
			return nil
		})
		if err != nil {
			return err
		}
	}
	// Queue zero remains single-writer while the core owner operations execute.
	return a.onOwner(0, func() error { return a.store.Core.AdoptMove(ctx, p, ids, now) })
}
func writeMoveFile(dir, name string, b []byte) error {
	f, err := os.CreateTemp(dir, ".move-")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	_, err = f.Write(b)
	err = errors.Join(err, f.Sync(), f.Close())
	if err != nil {
		return err
	}
	if err = os.Rename(f.Name(), filepath.Join(dir, name)); err != nil {
		return err
	}
	if err = syncDir(dir); err != nil {
		return err
	}
	return syncDir(filepath.Dir(dir))
}
