package chunkstore

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"

	"github.com/cockroachdb/pebble"
)

const mutationHistoryLimit = 16
const mutationScopeLimit = 16
const mutationRangeLimit = 256

type MutationRange struct {
	Archive     int
	From, Until int64
}
type mutationEntry struct {
	Revision uint64
	Ranges   []MutationRange
}
type mutationScope struct {
	From, Until int64
	Revision    uint64
	Reason      string
}

type mutationHistory struct {
	Scopes      []mutationScope
	Version     int
	Floor, Head uint64
	Entries     []mutationEntry
}

// ContinuationMutationRecorder is optional. A codec must report every changed
// logical slot, including removed owners, and call CompleteMutation only after
// inspecting the entire mutation. Otherwise the revision fence remains unknown.
type ContinuationMutationRecorder interface {
	MutationBaseline() (ContinuationIO, func())
	RecordMutation(int, int64, int64)
	CompleteMutation()
	MutationWindows() []MutationRange
}

func mutationKey(m Metadata) []byte {
	k := []byte("proof/ranges/")
	k = binary.BigEndian.AppendUint64(k, m.ID)
	return binary.BigEndian.AppendUint64(k, m.Generation)
}

func readMutationHistory(reader pebble.Reader, m Metadata) (mutationHistory, error) {
	var h mutationHistory
	data, closer, err := reader.Get(mutationKey(m))
	if errors.Is(err, pebble.ErrNotFound) {
		return h, nil
	}
	if err != nil {
		return h, err
	}
	defer closer.Close()
	if len(data) > 64<<10 {
		return h, errors.New("mutation history exceeds limit")
	}
	if err := json.Unmarshal(data, &h); err != nil {
		return h, err
	}
	if h.Version != 1 || h.Floor > h.Head || len(h.Entries) > mutationHistoryLimit || len(h.Scopes) > mutationScopeLimit {
		return h, errors.New("invalid mutation history")
	}
	for _, scope := range h.Scopes {
		if scope.From <= 0 || scope.Until <= scope.From || scope.Revision > h.Head {
			return h, errors.New("invalid mutation scope")
		}
	}
	previous := h.Floor
	for _, e := range h.Entries {
		if e.Revision <= previous || e.Revision > h.Head || len(e.Ranges) > mutationRangeLimit {
			return h, errors.New("invalid mutation entry")
		}
		for _, r := range e.Ranges {
			if r.Archive < 0 || r.From < 0 || r.Until <= r.From {
				return h, errors.New("invalid mutation range")
			}
		}
		previous = e.Revision
	}
	return h, nil
}

// stageMutationHistory is in the same atomic batch as the points and revision.
// A missing revision in this chain is an UNKNOWN fence, never an empty delta.
func (s *Store) stageMutationHistory(b *pebble.Batch, m Metadata, ranges []MutationRange, complete bool) error {
	h, err := readMutationHistory(bReader(b, s.db), m)
	if err != nil {
		return err
	}
	return stageMutationHistoryFrom(b, m, h, ranges, complete)
}

// The caller may reuse history read under the same metric mutation reservation.
// No codec writes this key; points, proof history and revision commit atomically.
func stageMutationHistoryFrom(b *pebble.Batch, m Metadata, h mutationHistory, ranges []MutationRange, complete bool) error {
	if h.Version == 0 {
		return nil
	}
	if h.Head != m.Revision {
		h.Floor = m.Revision
		h.Head = m.Revision
		h.Entries = nil
		for i := range h.Scopes {
			h.Scopes[i].Revision = m.Revision
			h.Scopes[i].Reason = "unknown mutation path"
		}
	}
	next := m.Revision + 1
	if !complete || len(ranges) > mutationRangeLimit {
		h.Floor = next
		h.Head = next
		h.Entries = nil
		for i := range h.Scopes {
			h.Scopes[i].Revision = next
			h.Scopes[i].Reason = "unknown mutation path"
		}
	} else {
		h.Head = next
		for i, scope := range h.Scopes {
			for _, r := range ranges {
				if r.From < scope.Until && r.Until > scope.From {
					h.Scopes[i].Revision = next
					h.Scopes[i].Reason = "in-window mutation"
					break
				}
			}
		}
		if len(ranges) > 0 {
			h.Entries = append(h.Entries, mutationEntry{next, ranges})
		}
		if len(h.Entries) > mutationHistoryLimit {
			h.Floor = h.Entries[0].Revision
			h.Entries = h.Entries[1:]
		}
	}
	data, err := json.Marshal(h)
	if err != nil {
		return err
	}
	if len(data) > 32<<10 {
		h.Floor = next
		h.Entries = nil
		data, _ = json.Marshal(h)
	}
	return b.Set(mutationKey(m), data, nil)
}

// An indexed batch includes earlier updates to the same name in UpdateBatch.
func bReader(b *pebble.Batch, fallback pebble.Reader) pebble.Reader {
	if b.Indexed() {
		return b
	}
	return fallback
}

// MutationProof binds a bounded requested window to durable storage lineage.
// Only revisions whose complete range record intersects this window enter the
// token. Unsupported paths advance Floor and consequently reopen this name.
func (s *Store) MutationProof(ctx context.Context, name string, from, until int64) (string, string, error) {
	ctx, finish, err := s.beginRead(ctx)
	if err != nil {
		return "", "", err
	}
	defer finish()
	if from <= 0 || until <= from {
		return "", "", errors.New("invalid proof window")
	}
	m, err := s.Metadata(ctx, name)
	found := err == nil
	if err != nil && !errors.Is(err, ErrNotFound) {
		return "", "", err
	}
	if s.proofLineage.Era == "" || s.IncrementalSource() != "" && (!found || m.FileMigration != FileReclaimed) {
		return "", "unknown ownership", nil
	}
	var reader pebble.Reader = s.db
	if b := s.digestReadBatch(ctx); b != nil {
		reader = b.view
	}
	// Register only names for which a caller actually requests proofs.
	if found && !s.readOnly {
		unlock := s.lockMetric(name)
		current, e := metadataFrom(s.db, name)
		if e == nil && current.ID == m.ID && current.Generation == m.Generation && current.Revision == m.Revision {
			h, e := readMutationHistory(s.db, m)
			if e != nil {
				unlock()
				return "", "", e
			}
			dirty := h.Version == 0 || h.Head != m.Revision
			if h.Version == 0 {
				h = mutationHistory{Version: 1, Floor: m.Revision, Head: m.Revision}
			}
			if h.Head != m.Revision {
				h.Floor, h.Head, h.Entries = m.Revision, m.Revision, nil
				for i := range h.Scopes {
					h.Scopes[i].Revision, h.Scopes[i].Reason = m.Revision, "unknown mutation path"
				}
			}
			exists := false
			for _, scope := range h.Scopes {
				if scope.From == from && scope.Until == until {
					exists = true
				}
			}
			if !exists {
				if len(h.Scopes) == mutationScopeLimit {
					h.Scopes = h.Scopes[1:]
				}
				h.Scopes = append(h.Scopes, mutationScope{from, until, m.Revision, "scope registered"})
				dirty = true
			}
			if dirty {
				data, _ := json.Marshal(h)
				// A clean process seal syncs these registrations before publishing.
				// An unclean restart always rotates the proof era.
				if e = s.db.Set(mutationKey(m), data, pebble.NoSync); e != nil {
					unlock()
					return "", "", e
				}
			}
		}
		unlock()
	}
	revision := m.Revision
	reason := "unknown mutation path"
	if found {
		h, e := readMutationHistory(reader, m)
		if e != nil {
			return "", "", e
		}
		if h.Version == 1 && h.Head == m.Revision {
			for _, scope := range h.Scopes {
				if scope.From == from && scope.Until == until {
					revision = scope.Revision
					reason = scope.Reason
					break
				}
			}
		}
	}
	data, err := json.Marshal(struct {
		Incarnation, Era, Name   string
		Found                    bool
		ID, Generation, Revision uint64
		Config                   MetricConfig
		From, Until              int64
	}{s.proofLineage.Incarnation, s.proofLineage.Era, name, found, m.ID, m.Generation, revision, m.MetricConfig, from, until})
	if err != nil {
		return "", "", err
	}
	digest := sha256.Sum256(data)
	return hex.EncodeToString(digest[:]), reason, nil
}

func (io *continuationIO) MutationBaseline() (ContinuationIO, func()) {
	if !io.mutationTracked || io.store == nil {
		return nil, func() {}
	}
	base := io.store.newContinuationIO(io.ctx, io.store.db, nil, io.metric)
	// Reuse only immutable committed state for this reserved metric revision.
	// A separate view keeps baseline reads independent of writer changes.
	// A nonempty batch may contain earlier updates to this name, while the
	// baseline reader still sees the committed DB. Keep that path uncached.
	// The codec still opens and validates the cached header bytes.
	if io.state != nil && io.batch != nil && io.batch.Count() == 0 {
		st := io.state.base
		if st != nil && st.meta.ID == io.metric.ID && st.meta.Generation == io.metric.Generation && st.meta.Revision == io.metric.Revision {
			base.state = io.store.newStateView(st, io.metric)
		}
	}
	return base, func() { _ = base.Close() }
}
func (io *continuationIO) RecordMutation(archive int, from, until int64) {
	if len(io.mutationRanges) <= mutationRangeLimit {
		io.mutationRanges = append(io.mutationRanges, MutationRange{archive, from, until})
	}
}
func (io *continuationIO) CompleteMutation() { io.mutationComplete = true }

func (io *continuationIO) MutationWindows() []MutationRange { return io.mutationWindows }
