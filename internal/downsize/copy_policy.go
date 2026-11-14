package downsize

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"sort"
	"strconv"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/internal/whisperio"
)

// copyPolicyConflict resolves only an XFilesFactor mismatch. That threshold
// governs future rollups, not the meaning of archived points. Re-encode the
// donor's exact archives under the target policy; the normal target-wins import
// and proof verification still apply. Never change either live policy.
func (m *mover) copyPolicyConflict(ctx context.Context, t task, name string, budget int64, proofs *[]store.TransferProof, original error) (http.Header, error) {
	var source, target []store.TransferMetric
	for _, side := range []struct {
		endpoint string
		out      *[]store.TransferMetric
	}{{t.Source, &source}, {t.Target, &target}} {
		_, err := m.request(ctx, side.endpoint, "/v1/transfer/export?wire=2&max_bytes="+strconv.FormatInt(budget, 10), []string{name}, side.out)
		if err != nil {
			return nil, err
		}
	}
	if len(source) != 1 || len(target) != 1 {
		return nil, original
	}
	record, err := copyWithTargetPolicy(ctx, source[0], target[0].Config)
	if err != nil {
		return nil, original
	}
	var wire bytes.Buffer
	if err := store.WriteTransfer2(&wire, []store.TransferMetric{record}); err != nil {
		return nil, err
	}
	if int64(wire.Len()) > budget {
		return nil, original
	}
	headers, err := m.request(ctx, t.Target, "/v1/transfer/import", transferBody(wire.Bytes()), proofs)
	if err == nil {
		headers.Set("X-Transfer-Bytes", strconv.Itoa(wire.Len()))
		event("copy_policy_resolved", map[string]any{"name": name, "source_xff": source[0].Config.XFilesFactor, "target_xff": target[0].Config.XFilesFactor})
	}
	return headers, err
}

func copyWithTargetPolicy(ctx context.Context, source store.TransferMetric, target store.MetricConfig) (store.TransferMetric, error) {
	cfg := source.Config
	if !source.Continuation || cfg.Name != target.Name || cfg.AggregationMethod != target.AggregationMethod || !slices.Equal(cfg.Retentions, target.Retentions) || cfg.XFilesFactor == target.XFilesFactor {
		return store.TransferMetric{}, errors.New("not an XFilesFactor-only continuation conflict")
	}
	input := policyMemory{}
	for _, kv := range source.Keys {
		input[string(kv.Key)] = bytes.Clone(kv.Value)
	}
	codec := &whisperio.ContinuationCodec{}
	snap, err := codec.Snapshot(ctx, input, store.Metadata{MetricConfig: cfg, Continuation: true}, time.Now)
	if err != nil {
		return store.TransferMetric{}, err
	}
	snap.Metadata.MetricConfig = target
	output := policyMemory{}
	if err := codec.RestoreArchives(ctx, output, snap, time.Now); err != nil {
		return store.TransferMetric{}, err
	}
	got, err := codec.Snapshot(ctx, output, snap.Metadata, time.Now)
	if err != nil {
		return store.TransferMetric{}, err
	}
	if err := store.EqualSnapshots(snap, got); err != nil {
		return store.TransferMetric{}, fmt.Errorf("policy archive verification: %w", err)
	}
	source.Config = target
	source.Keys = nil
	if err := output.Visit(nil, func(k, v []byte) error {
		source.Keys = append(source.Keys, store.RelayoutKV{Key: bytes.Clone(k), Value: bytes.Clone(v)})
		return nil
	}); err != nil {
		return store.TransferMetric{}, err
	}
	return source, nil
}

type policyMemory map[string][]byte

func (m policyMemory) Get(k []byte) ([]byte, error) {
	v, ok := m[string(k)]
	if !ok {
		return nil, store.ErrNotFound
	}
	return bytes.Clone(v), nil
}
func (m policyMemory) Set(k, v []byte) error { m[string(k)] = bytes.Clone(v); return nil }
func (m policyMemory) Delete(k []byte) error { delete(m, string(k)); return nil }
func (m policyMemory) DeletePrefix(p []byte) error {
	for k := range m {
		if bytes.HasPrefix([]byte(k), p) {
			delete(m, k)
		}
	}
	return nil
}
func (m policyMemory) Visit(p []byte, f func([]byte, []byte) error) error {
	var keys []string
	for k := range m {
		if bytes.HasPrefix([]byte(k), p) {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	for _, k := range keys {
		if err := f([]byte(k), bytes.Clone(m[k])); err != nil {
			return err
		}
	}
	return nil
}
