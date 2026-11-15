package downsize

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"os"
	"time"

	"github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/internal/whisperio"
)

// A native boundary receipt can outlive its source view. Keep that receipt
// immutable, and resolve only its exact archive/timestamp against fresh state.
// A finite target point already satisfies target-wins coverage. Otherwise a
// later persisted coarse bucket establishes that this is historical rollup
// data, rather than the source's still-forming trailing bucket. Sparse finer
// contributors do not invalidate an independently authoritative closed rollup.
func (b *syncLiveBackend) repairWindowBoundary(ctx context.Context, c *nativeController, source, target, name string, skip chunkstore.NativeWindowSkip) (bool, error) {
	if skip.Reason != "incomplete source boundary bucket" || skip.Archive <= 0 {
		return false, nil
	}
	names := []string{name}
	meta, err := b.SumDigest(ctx, source, names, 0, skip.Timestamp, skip.Timestamp+1)
	if err != nil {
		return false, err
	}
	archives := meta[name]
	if skip.Archive >= len(archives) {
		return false, nil
	}
	step := archives[skip.Archive].Step
	if step <= 0 || skip.Timestamp%step != 0 || skip.Timestamp+2*step > time.Now().Unix() {
		return false, nil
	}
	covered := func() (bool, error) {
		got, e := b.SumDigest(ctx, target, names, step, skip.Timestamp, skip.Timestamp+1)
		if e != nil {
			return false, e
		}
		for _, a := range got[name] {
			if a.Step == step && a.Count == 1 {
				return true, nil
			}
		}
		return false, nil
	}
	if ok, e := covered(); ok || e != nil {
		return ok, e
	}
	id, err := c.session(ctx, source)
	if err != nil {
		return false, err
	}
	window := chunkstore.NativeWindow{From: skip.Timestamp, Until: skip.Timestamp + 2*step}
	body, err := json.Marshal(struct {
		Session string
		Names   []string
		Window  *chunkstore.NativeWindow
	}{id, names, &window})
	if err != nil {
		return false, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, source+"/v1/native/export", bytes.NewReader(body))
	if err != nil {
		return false, err
	}
	req.Header.Set("Content-Type", "application/json")
	if token := os.Getenv(b.o.tokenEnv); token != "" {
		req.Header.Set("X-Buckyd-Authorization", token)
	}
	response, err := b.client.Do(req)
	if err != nil {
		return false, err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return false, fmt.Errorf("boundary export: HTTP %d", response.StatusCode)
	}
	stream, err := chunkstore.NewTransferStream(response.Body)
	if err != nil {
		return false, err
	}
	if stream.Count != 1 || stream.Window == nil || *stream.Window != window {
		return false, fmt.Errorf("boundary export scope mismatch")
	}
	record, err := stream.Next()
	if err != nil {
		return false, err
	}
	// Finish validates the stream checksum before any target mutation.
	if _, _, err := stream.Finish(); err != nil {
		return false, err
	}
	if record.Config.Name != name || record.NativeSkip != nil || !record.Continuation {
		return false, nil
	}
	snap, err := chunkstore.TransferSnapshot(ctx, record, &whisperio.ContinuationCodec{}, time.Now)
	if err != nil {
		return false, err
	}
	if skip.Archive >= len(snap.Archives) || int64(snap.Archives[skip.Archive].Retention.Step) != step {
		return false, nil
	}
	var point *chunkstore.Point
	later := false
	for _, p := range snap.Archives[skip.Archive].Points {
		if math.IsNaN(p.Value) || math.IsInf(p.Value, 0) {
			continue
		}
		if p.Timestamp == skip.Timestamp {
			v := p
			point = &v
		}
		if p.Timestamp == skip.Timestamp+step {
			later = true
		}
	}
	if point == nil || !later {
		return false, nil
	}
	for ai := range snap.Archives {
		snap.Archives[ai].Points = nil
	}
	snap.Archives[skip.Archive].Points = []chunkstore.Point{*point}
	output := policyMemory{}
	codec := &whisperio.ContinuationCodec{}
	if err := codec.RestoreArchives(ctx, output, snap, time.Now); err != nil {
		return false, err
	}
	got, err := codec.Snapshot(ctx, output, snap.Metadata, time.Now)
	if err != nil {
		return false, err
	}
	if err := chunkstore.EqualSnapshots(snap, got); err != nil {
		return false, err
	}
	record.Keys = nil
	record.WindowSkipped = nil
	record.PointDigest = ""
	record.Counts = nil
	if err := output.Visit(nil, func(k, v []byte) error {
		record.Keys = append(record.Keys, chunkstore.RelayoutKV{Key: bytes.Clone(k), Value: bytes.Clone(v)})
		return nil
	}); err != nil {
		return false, err
	}
	var wire bytes.Buffer
	if err := chunkstore.WriteTransfer2(&wire, []chunkstore.TransferMetric{record}); err != nil {
		return false, err
	}
	var proofs []chunkstore.TransferProof
	if _, err := b.m.requestOnce(ctx, target, "/v1/transfer/import", transferBody(wire.Bytes()), &proofs); err != nil {
		return false, err
	}
	var verified struct{ Verified int }
	if _, err := b.m.requestOnce(ctx, target, "/v1/transfer/verify", proofs, &verified); err != nil {
		return false, err
	}
	if verified.Verified != 1 {
		return false, fmt.Errorf("boundary proof count mismatch")
	}
	ok, err := covered()
	if ok && err == nil {
		event("sync_window_boundary_repaired", map[string]any{"metric": name, "timestamp": skip.Timestamp, "step": step, "target_wins": true})
	}
	return ok, err
}
