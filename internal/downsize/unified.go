package downsize

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	hashing "github.com/go-graphite/go-carbon/internal/downsize/buckyhash"
	"github.com/go-graphite/go-carbon/internal/downsize/ringplan"
	"github.com/go-graphite/go-carbon/internal/storecopy"
)

var errInputFence = errors.New("replacement baseline copied; resume with a fresh input_fenced cutover barrier")
var errReplayReady = errors.New("replacement prepared; start the target with captured log replay, verify reads/index, and resume with targets_ready")

type replacementBarrier struct {
	PlanID       string          `json:"plan_id"`
	ExpiresAt    int64           `json:"expires_at"`
	InputFenced  bool            `json:"input_fenced"`
	Sources      []string        `json:"sources"`
	TargetsReady map[string]bool `json:"targets_ready"`
}

func (m *mover) replacementBarrier() (replacementBarrier, error) {
	var b replacementBarrier
	if m.barrier == "" {
		return b, errInputFence
	}
	data, err := os.ReadFile(m.barrier)
	if err != nil {
		return b, err
	}
	if err := json.Unmarshal(data, &b); err != nil {
		return b, err
	}
	if b.PlanID != m.planData.PlanID || b.ExpiresAt <= time.Now().Unix() || b.ExpiresAt > time.Now().Add(24*time.Hour).Unix() || !b.InputFenced {
		return b, errInputFence
	}
	return b, nil
}
func wholeTask(job ringplan.WholeCopy) task {
	blob, _ := json.Marshal(job)
	hash := sha256.Sum256(blob)
	return task{Kind: "whole", Stream: "whole-" + hex.EncodeToString(hash[:]), Start: 0, End: 1, Source: job.SourceURL, Target: job.TargetURL}
}
func (m *mover) wholeState(job ringplan.WholeCopy) task {
	t := wholeTask(job)
	m.stateMu.Lock()
	defer m.stateMu.Unlock()
	if saved, ok := m.journal.tasks[taskKey(t)]; ok {
		return saved
	}
	return t
}
func (m *mover) saveWhole(t task) error {
	m.stateMu.Lock()
	defer m.stateMu.Unlock()
	return m.journal.save(t)
}
func (m *mover) wholeOffline(host string) bool {
	for _, job := range m.planData.Whole {
		if job.TargetURL == host {
			return m.wholeState(job).Phase != "ready"
		}
	}
	return false
}
func (m *mover) workerRequest(ctx context.Context, job ringplan.WholeCopy, path string, body, out any) error {
	return m.helper(ctx, job.TargetURL, path, body, out)
}
func (m *mover) workerLimits(ctx context.Context, job ringplan.WholeCopy) error {
	m.tuner.mu.Lock()
	a, b := m.tuner.hosts[job.SourceURL], m.tuner.hosts[job.TargetURL]
	workers := min(a.Streams, b.Streams)
	chunk := min(a.BatchBytes, b.BatchBytes, int64(16<<20))
	rate := int64(m.tuner.mbps * 1e6)
	m.tuner.mu.Unlock()
	if chunk < 1<<20 {
		chunk = 1 << 20
	}
	return m.workerRequest(ctx, job, "/limits", workerLease{Limits: storecopy.Limits{Workers: workers, Chunk: chunk, Rate: rate, Expires: time.Now().Add(3 * m.tick), Reason: "shared resize governor"}, PlanID: m.planData.PlanID, Checkpoint: m.wholeState(job).Checkpoint}, nil)
}
func (m *mover) copyWorker(ctx context.Context, job ringplan.WholeCopy, t task, action string) error {
	var nodes []any
	var node string
	for _, member := range m.planData.Spec.New.Nodes {
		nodes = append(nodes, member.Node)
		if member.ID == job.Destination {
			parsed, _ := hashing.NewNodeParser(member.Node)
			node = parsed.Server
		}
	}
	request := workerRequest{PlanID: m.planData.PlanID, Target: job.Destination, Source: job.SourceURL, Checkpoint: t.Checkpoint, Identity: map[string]any{"node": node, "nodes": nodes, "hash": m.planData.Spec.New.Hash, "replicas": max(1, m.planData.Spec.New.Replicas)}}
	if err := m.workerLimits(ctx, job); err != nil {
		return err
	}
	var status workerState
	if err := m.workerRequest(ctx, job, "/"+action, request, &status); err != nil {
		return err
	}
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = m.workerRequest(cleanup, job, "/stop", request, nil)
	}()
	lastBytes := int64(-1)
	for {
		if status.VerifiedBytes != lastBytes {
			lastBytes = status.VerifiedBytes
			event("copy_progress", map[string]any{"kind": "whole", "source": job.SourceURL, "target": job.TargetURL, "checkpoint": t.Checkpoint, "bytes_verified": status.VerifiedBytes, "total_bytes": status.TotalBytes})
		}
		if status.Request.PlanID != m.planData.PlanID || status.Request.Checkpoint != t.Checkpoint {
			return errors.New("target worker checkpoint identity differs")
		}
		if status.Phase == "failed" || status.Phase == "interrupted" {
			return errors.New(status.Error)
		}
		if action == "copy" && (status.Phase == "copied" || status.Prepared) || action == "prepare" && status.Prepared {
			return nil
		}
		if err := sleepContext(ctx, min(m.tick, time.Second)); err != nil {
			return err
		}
		if err := m.workerLimits(ctx, job); err != nil {
			return err
		}
		if err := m.workerRequest(ctx, job, "/status", nil, &status); err != nil {
			return err
		}
	}
}
func (m *mover) executeWhole(ctx context.Context, job ringplan.WholeCopy) error {
	t := m.wholeState(job)
	client := &storecopy.Client{URL: job.SourceURL, Token: os.Getenv(m.tokenEnv), HTTP: m.client}
	if t.Phase == "ready" {
		return m.checkWholeReady(ctx, job, t)
	}
	if t.Checkpoint == "" {
		hash := sha256.Sum256([]byte(m.journal.operation + taskKey(t)))
		manifest, err := client.Begin(ctx, hex.EncodeToString(hash[:]))
		if err != nil {
			return err
		}
		t.Checkpoint = manifest.ID
		t.Phase = "checkpoint"
		if err := m.saveWhole(t); err != nil {
			return err
		}
		event("checkpoint", map[string]any{"source": job.SourceURL, "target": job.TargetURL, "checkpoint": t.Checkpoint})
	}
	if t.Phase == "checkpoint" {
		if err := m.copyWorker(ctx, job, t, "copy"); err != nil {
			return err
		}
		t.Phase = "copied"
		if err := m.saveWhole(t); err != nil {
			return err
		}
	}
	if t.Phase == "copied" {
		b, err := m.replacementBarrier()
		found := false
		for _, source := range b.Sources {
			if source == job.Source {
				found = true
			}
		}
		if err == nil && !found {
			err = errInputFence
		}
		if err != nil {
			event("needs_input_fence", map[string]any{"target": job.Destination, "checkpoint": t.Checkpoint})
			return err
		}
		manifest, err := client.Manifest(ctx, "seal", t.Checkpoint)
		if err != nil {
			return err
		}
		if !manifest.Sealed {
			return errors.New("source did not seal input checkpoint")
		}
		t.Phase = "sealed"
		if err := m.saveWhole(t); err != nil {
			return err
		}
	}
	if t.Phase == "sealed" {
		if err := m.copyWorker(ctx, job, t, "copy"); err != nil {
			return err
		}
		if err := m.copyWorker(ctx, job, t, "prepare"); err != nil {
			return err
		}
		t.Phase = "prepared"
		if err := m.saveWhole(t); err != nil {
			return err
		}
	}
	if t.Phase == "prepared" {
		barrier, err := m.replacementBarrier()
		if err != nil {
			return err
		}
		if !barrier.TargetsReady[job.Destination] {
			event("needs_target_replay", map[string]any{"target": job.Destination, "directory": job.Placement.Directory, "checkpoint": t.Checkpoint})
			return errReplayReady
		}
		if err := m.checkWholeReady(ctx, job, t); err != nil {
			return err
		}
		if _, err := client.Manifest(ctx, "release", t.Checkpoint); err != nil {
			var api *storecopy.APIError
			if !errors.As(err, &api) || api.Status != 410 && !(api.Status == 409 && strings.Contains(api.Message, "copy ID does not match")) {
				return err
			}
			event("checkpoint_already_released", map[string]any{"checkpoint": t.Checkpoint, "source": job.SourceURL})
		}
		t.Phase = "ready"
		if err := m.saveWhole(t); err != nil {
			return err
		}
		event("replacement_ready", map[string]any{"target": job.Destination, "checkpoint": t.Checkpoint, "source_retained": true})
	}
	return nil
}
func (m *mover) checkWholeReady(ctx context.Context, job ringplan.WholeCopy, t task) error {
	var status workerState
	if err := m.workerRequest(ctx, job, "/status", nil, &status); err != nil {
		return err
	}
	if status.Request.PlanID != m.planData.PlanID || status.Request.Checkpoint != t.Checkpoint || !status.Prepared || !status.ReplayComplete {
		return errReplayReady
	}
	if !m.localLab {
		var sample hostSample
		if err := m.helper(ctx, job.TargetURL, "/sample", nil, &sample); err != nil {
			return err
		}
		if sample.Offline {
			return errors.New("replacement requires live application telemetry after replay")
		}
	}
	var storage storageSample
	if _, err := m.request(ctx, job.TargetURL, "/v1/transfer/status", nil, &storage); err != nil {
		return err
	}
	if storage.Protocol != 1 {
		return errors.New("replacement target transfer API unavailable")
	}
	return nil
}
func (m *mover) replacements(ctx context.Context) error {
	var wg sync.WaitGroup
	errs := make(chan error, len(m.planData.Whole))
	for _, job := range m.planData.Whole {
		wg.Go(func() {
			if err := m.executeWhole(ctx, job); err != nil {
				errs <- err
			}
		})
	}
	wg.Wait()
	close(errs)
	var result error
	for err := range errs {
		result = errors.Join(result, err)
	}
	return result
}

func Status(plan, state string, out io.Writer) error {
	data, err := os.ReadFile(filepath.Join(plan, "manifest.json"))
	if err != nil {
		return err
	}
	var manifest struct {
		PlanID string `json:"plan_id"`
	}
	if err := json.Unmarshal(data, &manifest); err != nil {
		return err
	}
	file, err := os.Open(state)
	if err != nil {
		return err
	}
	defer file.Close()
	decoder := json.NewDecoder(file)
	latest := map[string]task{}
	for {
		var row struct {
			Plan string
			Task task
		}
		err := decoder.Decode(&row)
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			break
		}
		if err != nil {
			return err
		}
		if !strings.HasPrefix(row.Plan, manifest.PlanID+":") {
			return errors.New("state plan differs")
		}
		if row.Task.End > row.Task.Start {
			latest[taskKey(row.Task)] = row.Task
		}
	}
	return json.NewEncoder(out).Encode(map[string]any{"plan_id": manifest.PlanID, "tasks": latest})
}
