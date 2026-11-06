package downsize

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strconv"

	"github.com/go-graphite/go-carbon/internal/chunkstore"
	hashing "github.com/go-graphite/go-carbon/internal/downsize/buckyhash"
	"github.com/go-graphite/go-carbon/internal/storecopy"
	"github.com/go-graphite/go-carbon/internal/whisperio"
)

type transferBody []byte

var errBatchLimit = errors.New("batch limit reached")

func (m *mover) closeLocalSources() {
	for _, source := range m.localSources {
		_ = source.Close()
	}
}

func (m *mover) openLocalSources(ctx context.Context) (err error) {
	if m.localSources == nil {
		m.localSources = make(map[string]*chunkstore.TransferSource)
	}
	if m.options.localSources == "" {
		if m.localTarget != "" {
			return errors.New("--local-target requires --local-sources")
		}
		return nil
	}
	if m.delete || m.reconcile || len(m.planData.Whole) != 0 {
		return errors.New("local checkpoints support copy-only range plans; reconcile from live donors separately")
	}
	if _, err := origin(m.localTarget); err != nil || !loopbackOrigin(m.localTarget) {
		return errors.New("local checkpoints require a loopback --local-target")
	}
	data, err := os.ReadFile(m.options.localSources)
	if err != nil {
		return err
	}
	var paths map[string]string
	if err := json.Unmarshal(data, &paths); err != nil {
		return err
	}
	if len(paths) == 0 {
		return errors.New("empty local source map")
	}
	defer func() {
		if err != nil {
			m.closeLocalSources()
		}
	}()
	nodes := make(map[string]member)
	for _, node := range m.planData.Spec.Old.Nodes {
		nodes[node.Endpoint] = node
	}
	for _, node := range m.planData.Spec.New.Nodes {
		nodes[node.Endpoint] = node
	}
	m.localTargets = make(map[string]string)
	var target string
	for source, directory := range paths {
		node, ok := nodes[source]
		if !ok || !filepath.IsAbs(directory) {
			return fmt.Errorf("unknown source or nonabsolute checkpoint: %s", source)
		}
		used := false
		for _, route := range m.planData.Streams {
			if route.sourceURL != source {
				continue
			}
			used = true
			if target != "" && target != route.targetURL {
				return errors.New("local copy process must have exactly one physical target")
			}
			target = route.targetURL
		}
		if !used {
			return fmt.Errorf("unused local source %s", source)
		}
		manifest, err := storecopy.CheckpointSourceManifest(directory)
		if err != nil {
			return err
		}
		parsed, parseErr := hashing.NewNodeParser(node.Node)
		if parseErr != nil || len(manifest.ID) != 32 || manifest.Metadata["node"] != parsed.Server {
			return fmt.Errorf("checkpoint source identity mismatch: %s", source)
		}
		// Pull's completion receipt is written only after synced chunk checksums.
		// Preserve source files and their immutable copy-state proofs for resume.
		var codec chunkstore.ContinuationCodec
		if manifest.StoreFormat >= 6 {
			codec = &whisperio.ContinuationCodec{}
		}
		opened, err := chunkstore.OpenTransferSource(filepath.Join(directory, "store"), chunkstore.TransferSourceOptions{Continuation: codec, CacheBytes: 128 << 20})
		if err != nil {
			return err
		}
		m.localSources[source] = opened
		event("local_source", map[string]any{"source": source, "directory": directory, "checkpoint": manifest.ID})
	}
	var identity struct{ Name string }
	if _, err := m.request(ctx, m.localTarget, "/hashring", nil, &identity); err != nil {
		return err
	}
	parsed, parseErr := hashing.NewNodeParser(nodes[target].Node)
	if parseErr != nil || identity.Name != parsed.Server {
		return errors.New("loopback Buckyd is not the planned physical target")
	}
	m.localTargets[target] = m.localTarget
	return nil
}

func (m *mover) pull(ctx context.Context, task task, names []string, budget int64, proofs *[]chunkstore.TransferProof) (http.Header, error) {
	if source := m.localSources[task.Source]; source != nil {
		records, err := source.ExportTransferLimit(ctx, names, budget)
		if err == nil {
			var wire bytes.Buffer
			if err := chunkstore.WriteTransfer2(&wire, records); err != nil {
				return nil, err
			}
			if int64(wire.Len()) > budget {
				return nil, &apiError{413, "local checkpoint transfer exceeds byte budget"}
			}
			// Older controllers also stop on verified+empty-proofs instead of
			// replaying a potentially partially published import.
			intent := task
			intent.Kind, intent.Phase, intent.Proofs = "bulk-intent", "verified", nil
			m.stateMu.Lock()
			err = m.journal.save(intent)
			m.stateMu.Unlock()
			if err != nil {
				return nil, err
			}
			headers, err := m.request(ctx, task.Target, "/v1/transfer/import", transferBody(wire.Bytes()), proofs)
			if err != nil {
				// Even an error response may follow partial catalog publication.
				// Do not let the ordinary pull's 404/413 retry/split logic replay it.
				return nil, errors.New("uncertain local import response: " + err.Error())
			}
			headers.Set("X-Transfer-Bytes", strconv.Itoa(wire.Len()))
			return headers, nil
		}
		if errors.Is(err, chunkstore.ErrTransferLimit) {
			return nil, &apiError{413, err.Error()}
		}
		return nil, fmt.Errorf("local checkpoint export (recapture if a planned name is missing): %w", err)
	}
	return m.request(ctx, task.Target, "/v1/transfer/pull", struct {
		Source   string
		Names    []string
		MaxBytes int64
	}{task.Source, names, budget}, proofs)
}
