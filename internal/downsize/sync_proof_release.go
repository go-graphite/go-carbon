package downsize

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/go-graphite/go-carbon/internal/chunkstore"
)

const proofMaintenancePath = "/v1/maintenance/mutation-proofs"

type syncProofReleaser interface {
	ReleaseProofs(context.Context, string, []string) error
}

func (b *syncLiveBackend) ReleaseProofs(ctx context.Context, host string, names []string) error {
	request := struct{ Names []chunkstore.ProofRelease }{}
	for _, name := range names {
		request.Names = append(request.Names, chunkstore.ProofRelease{Name: name})
	}
	// Old READY attempts can have holders outside the current job's origin
	// set. Authorize this saved holder without mutating the live transport map.
	client := *b.m.client
	if b.o.auth != nil {
		if signed, ok := client.Transport.(*syncJWTTransport); ok {
			client.Transport = signed.base
		}
		withSyncJWT(&client, b.o.auth, host)
	}
	m := &mover{options: options{engine: "metric", tokenEnv: b.o.tokenEnv}, client: &client}
	var reply []chunkstore.ProofReleaseResult
	_, err := m.requestOnce(ctx, host, proofMaintenancePath+"?action=release", request, &reply)
	if err != nil {
		return err
	}
	if len(reply) != len(names) {
		return errors.New("invalid proof release response length")
	}
	for i, r := range reply {
		if r.Name != names[i] || r.Status != "released" && r.Status != "absent" {
			return fmt.Errorf("release %s: %s %s", names[i], r.Status, r.Error)
		}
	}
	return nil
}

type proofCleanupTask struct {
	Catalog string
	Host    string
	Offset  int64
	Size    int64
	Done    bool
}
type proofCloseout struct {
	Version     int
	Identity    string
	Finished    time.Time
	Completed   bool
	Quarantined bool
	Tasks       []proofCleanupTask
}

// A closeout is an immutable historical verdict, not a new proof baseline.
// Persist it before retiring anything. A cleanup-only resume never republishes
// READY or updates saved tokens; a new pass still goes through normal frontier
// revalidation, including changes caused by release itself.
func beginProofCloseout(o syncOptions, quarantined bool) error {
	identity, err := syncIdentity(o)
	if err != nil {
		return err
	}
	c := proofCloseout{Version: 1, Identity: identity, Finished: time.Now().UTC(), Quarantined: quarantined}
	err = filepath.WalkDir(o.state, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		// A separately selected scope has its own command/closeout lifecycle.
		if entry.IsDir() && path != o.state && strings.HasPrefix(entry.Name(), "scope-") {
			return filepath.SkipDir
		}
		if entry.IsDir() || entry.Name() != "union.tsv" {
			return nil
		}
		// Each child journal owns its donor set, including independent READY jobs.
		var header syncRow
		if err := readReadyObservationJSON(filepath.Join(filepath.Dir(path), "sync.jsonl"), &header); err != nil {
			return err
		}
		var scope struct {
			To   string
			From []string
		}
		if header.Kind != "header" {
			return fmt.Errorf("%s: missing sync header", path)
		}
		if err := json.Unmarshal([]byte(header.Identity), &scope); err != nil {
			return err
		}
		if scope.To == "" {
			return fmt.Errorf("%s: missing closeout target", path)
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		relative, err := filepath.Rel(o.state, path)
		if err != nil {
			return err
		}
		for _, host := range append([]string{scope.To}, scope.From...) {
			c.Tasks = append(c.Tasks, proofCleanupTask{Catalog: relative, Host: host, Size: info.Size()})
		}
		return nil
	})
	if err != nil {
		return err
	}
	return atomicStateJSON(filepath.Join(o.state, "proof-closeout.json"), c)
}

func resumeProofCloseout(ctx context.Context, o syncOptions, b syncProofReleaser) (pending bool, result error) {
	path := filepath.Join(o.state, "proof-closeout.json")
	var c proofCloseout
	// Unlike one READY observation, the receipt can describe many child states.
	// It is trusted local state written by this controller, not a network body.
	f, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	err = json.NewDecoder(f).Decode(&c)
	_ = f.Close()
	if err != nil {
		return false, err
	}
	identity, err := syncIdentity(o)
	if err != nil {
		return false, err
	}
	if c.Version != 1 || c.Identity != identity {
		return false, errors.New("proof closeout identity changed")
	}
	if c.Completed {
		return false, nil
	}
	event("sync_proof_cleanup", map[string]any{"record": path, "closeout": c.Finished, "cleanup_only": true})
	defer func() {
		if result != nil {
			event("sync_proof_release_failed", map[string]any{"level": "ERROR", "error": result.Error(), "state_retained": true})
		}
	}()
	for i := range c.Tasks {
		task := &c.Tasks[i]
		if task.Done {
			continue
		}
		if !filepath.IsLocal(task.Catalog) {
			return true, errors.New("invalid proof cleanup catalog path")
		}
		f, err := os.Open(filepath.Join(o.state, task.Catalog))
		if err != nil {
			return true, err
		}
		err = releaseProofCatalog(ctx, f, task, b, func() error { return atomicStateJSON(path, c) })
		_ = f.Close()
		if err != nil {
			return true, err
		}
	}
	c.Completed = true
	if err = atomicStateJSON(path, c); err != nil {
		return true, err
	}
	event("sync_proof_cleanup_complete", map[string]any{"record": path})
	if c.Quarantined {
		return true, ErrSyncQuarantined
	}
	return true, nil
}

func releaseProofCatalog(ctx context.Context, f *os.File, task *proofCleanupTask, b syncProofReleaser, save func() error) error {
	info, err := f.Stat()
	if err != nil {
		return err
	}
	if info.Size() != task.Size || task.Offset < 0 || task.Offset > task.Size {
		return errors.New("proof cleanup catalog changed; state retained")
	}
	if _, err = f.Seek(task.Offset, io.SeekStart); err != nil {
		return err
	}
	reader := bufio.NewReader(f)
	for {
		names := make([]string, 0, 1000)
		offset := task.Offset
		for len(names) < cap(names) {
			line, err := reader.ReadString('\n')
			if err != nil && !errors.Is(err, io.EOF) {
				return err
			}
			if len(line) == 0 {
				break
			}
			name, _, _, parseErr := parseUnionLine(line[:len(line)-1])
			if parseErr != nil || line[len(line)-1] != '\n' {
				return fmt.Errorf("invalid cleanup catalog at %d: %v", offset, parseErr)
			}
			names = append(names, name)
			offset += int64(len(line))
		}
		if len(names) == 0 {
			task.Done = true
			return save()
		}
		if err := b.ReleaseProofs(ctx, task.Host, names); err != nil {
			return fmt.Errorf("release proof scopes on %s: %w", task.Host, err)
		}
		task.Offset = offset
		if err := save(); err != nil {
			return err
		}
	}
}
