package buckyd

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"slices"
	"time"

	"github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/golang-jwt/jwt/v5"
)

// SyncRequest asks this store to compare Names with Source and pull only the
// names whose points differ: rsync for metrics. From/Until bound the
// comparison (both zero means full history); a differing name is still pulled
// whole and merged, existing local points winning.
type SyncRequest struct {
	Source   string
	Names    []string
	From     int64 `json:",omitempty"`
	Until    int64 `json:",omitempty"`
	MaxBytes int64 `json:",omitempty"`
	// Recover re-exports equal names and reports copied, with a deletion
	// version, for those this store provably holds as a verbatim copy of
	// that export. Callers that delete sources set it so an interrupted run
	// can finish; it costs the export bytes of every equal name.
	Recover bool `json:",omitempty"`
}

// syncOriginHeader carries the syncing target's per-process identity to the
// source, which answers 421 when that identity is its own.
const syncOriginHeader = "X-Buckyd-Sync-Origin"

var errSelfTransfer = errors.New("sync source and target are the same daemon")

// Sync row statuses.
const (
	SyncEqual          = "equal"
	SyncCopied         = "copied"
	SyncMerged         = "merged"
	SyncAbsentOnSource = "absent_on_source"
)

// storeFenceHeader marks a refusal that applies to the whole store (the
// whole-store copy fence), which no smaller batch can pass. Statuses stay as
// existing clients expect them; bucky2 reads the header to stop splitting.
const storeFenceHeader = "X-Buckyd-Store-Fence"

// SyncRow is one requested name, in request order. Source is present only on
// copied rows: a source version whose every stored point this store durably
// holds. Pass it to the source's /v1/transfer/delete.
type SyncRow struct {
	Name   string
	Status string
	Source *chunkstore.TransferVersion `json:",omitempty"`
}

type SyncReply struct {
	Rows  []SyncRow
	Bytes int64
}

func (s *Service) syncTransfer(ctx context.Context, r SyncRequest, digest DigestRequest) (SyncReply, error) {
	remote, err := s.sourceDigest(ctx, r.Source, digest)
	if err != nil {
		return SyncReply{}, err
	}
	local, err := s.digest(ctx, digest)
	if err != nil {
		return SyncReply{}, err
	}
	reply := SyncReply{Rows: make([]SyncRow, len(r.Names))}
	// names is exported from the source in one request: first the rows to
	// import, then (with Recover) the equal rows whose copy receipt is checked.
	var names []string
	var rows []int
	var recoverRows []int
	for i, name := range r.Names {
		if remote[i].Name != name {
			return SyncReply{}, fmt.Errorf("source digest order: got %q, want %q", remote[i].Name, name)
		}
		reply.Rows[i] = SyncRow{Name: name}
		switch {
		case !remote[i].Found:
			reply.Rows[i].Status = SyncAbsentOnSource
		case local[i].Found && sameDigest(local[i].Archives, remote[i].Archives):
			reply.Rows[i].Status = SyncEqual
			if r.Recover {
				recoverRows = append(recoverRows, i)
			}
		default:
			names = append(names, name)
			rows = append(rows, i)
		}
	}
	imports := len(names)
	for _, i := range recoverRows {
		names = append(names, r.Names[i])
	}
	if len(names) == 0 {
		return reply, nil
	}
	buffer := s.transferBuffer()
	defer s.transferPool.Put(buffer)
	records, n, err := s.pullTransfer(ctx, r.Source, names, r.MaxBytes, buffer)
	if err != nil {
		return SyncReply{}, err
	}
	if len(records) != len(names) {
		return SyncReply{}, fmt.Errorf("source exported %d metrics for %d names", len(records), len(names))
	}
	for j, record := range records {
		if record.Version.Name != names[j] {
			return SyncReply{}, fmt.Errorf("source export order: got %q, want %q", record.Version.Name, names[j])
		}
	}
	reply.Bytes = n
	// A Source version authorizes deleting the whole source metric. It is
	// issued only for a name this store holds as a verbatim copy of that
	// exact source export: every stored source point is then here, and the
	// import committed it with a synced WAL.
	//
	// The guarantee is about one instant: this store durably held every
	// stored point of that source version. Writes that reach this copy
	// afterwards, before or after the source is deleted, are the metric's
	// ordinary history: applied to the same keys they change it exactly as
	// they would have on the source, including overwriting a slot. This is
	// the contract VerifyTransfer already gives storectl ("foreground updates
	// inherit its history and win overlaps"), so no lock is held across the
	// source delete. A digest ends at the daemon's
	// clock and a merge can drop a future-dated point into an occupied slot,
	// so equal and merged rows never authorize a delete.
	fence := func(row *SyncRow, version chunkstore.TransferVersion) {
		row.Status = SyncCopied
		row.Source = &version
	}
	if imports > 0 {
		proofs, err := s.store.ImportTransfer(ctx, records[:imports])
		if err != nil {
			return SyncReply{}, err
		}
		s.transferImported(records[:imports])
		if len(proofs) != imports {
			return SyncReply{}, fmt.Errorf("import returned %d proofs for %d names", len(proofs), imports)
		}
		for j, proof := range proofs {
			row := &reply.Rows[rows[j]]
			if proof.Source.Name != row.Name {
				return SyncReply{}, fmt.Errorf("import proof order: got %q, want %q", proof.Source.Name, row.Name)
			}
			if proof.Merged || local[rows[j]].Found {
				row.Status = SyncMerged
				continue
			}
			// The import published the copy before its WAL sync. Only a copy
			// still at its published revision now, after that sync, was
			// durable and complete at one instant. A write that slipped in
			// between leaves the copy in place without a deletion version.
			row.Status = SyncCopied
			m, err := s.store.Metadata(ctx, row.Name)
			if err == nil && (chunkstore.TransferVersion{Name: m.Name, ID: m.ID, Generation: m.Generation, Revision: m.Revision}) == proof.Target {
				fence(row, proof.Source)
			}
		}
	}
	// A copy whose reply was lost, or that committed before a later name
	// failed the batch, now compares equal. Its durable import receipt still
	// proves the verbatim copy when the source export is unchanged.
	// ponytail: recovery re-exports the full metric; a proof-only export
	// would save the bytes if delete reruns over large duplicates get slow.
	if len(recoverRows) > 0 {
		copied, err := s.store.VerifyTransferCopy(ctx, records[imports:])
		if err != nil {
			return SyncReply{}, err
		}
		for j, ok := range copied {
			if ok {
				fence(&reply.Rows[recoverRows[j]], records[imports+j].Version)
			}
		}
	}
	return reply, nil
}

// sameDigest ignores From/Until: each store clips archives at its own clock,
// so those can differ by a step between hosts holding equal points. Points
// (archive capacity) must match: an equal digest over a shorter retention is
// not the same metric. A mismatch falls through to the import, whose policy
// check rejects incompatible retentions.
func sameDigest(a, b []DigestArchive) bool {
	return slices.EqualFunc(a, b, func(x, y DigestArchive) bool {
		return x.Step == y.Step && x.Points == y.Points && x.Count == y.Count && x.Hash == y.Hash
	})
}

func (s *Service) sourceDigest(ctx context.Context, source string, digest DigestRequest) ([]DigestResult, error) {
	u, err := s.transferSourceOrigin(source)
	if err != nil {
		return nil, err
	}
	body, err := json.Marshal(digest)
	if err != nil {
		return nil, err
	}
	u.Path = "/v1/transfer/digest"
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, u.String(), bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set(syncOriginHeader, s.proofEpoch)
	if err := s.signRead(request); err != nil {
		return nil, err
	}
	response, err := bulkTransferClient.Do(request)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	if response.StatusCode == http.StatusMisdirectedRequest {
		return nil, &transferUpstreamError{status: http.StatusMisdirectedRequest, message: errSelfTransfer.Error()}
	}
	if response.StatusCode != 200 {
		return nil, &transferUpstreamError{status: response.StatusCode, message: fmt.Sprintf("source digest: %s", response.Status)}
	}
	// An overflowing reply is the caller's batch being too large: 413 lets it
	// split, where a truncated decode would look like a server error.
	data, err := io.ReadAll(io.LimitReader(response.Body, s.config.MaxBodyBytes+1))
	if err != nil {
		return nil, fmt.Errorf("source digest: %w", err)
	}
	if int64(len(data)) > s.config.MaxBodyBytes {
		return nil, &transferUpstreamError{status: http.StatusRequestEntityTooLarge, message: "source digest reply exceeds max_body_bytes; use a smaller batch"}
	}
	var results []DigestResult
	if err := json.Unmarshal(data, &results); err != nil {
		return nil, fmt.Errorf("source digest: %w", err)
	}
	if len(results) != len(digest.Names) {
		return nil, errors.New("source digest: wrong result count")
	}
	return results, nil
}

// signRead attaches the short-lived root read token peers accept from each other.
func (s *Service) signRead(request *http.Request) error {
	if len(s.secret) == 0 {
		return nil
	}
	signed, err := jwt.NewWithClaims(jwt.SigningMethodHS256, ACL{RegisteredClaims: jwt.RegisteredClaims{ExpiresAt: jwt.NewNumericDate(time.Now().Add(5 * time.Minute))}, Namespaces: []string{"*"}, Ops: []string{"read"}}).SignedString(s.secret)
	if err != nil {
		return err
	}
	request.Header.Set(authHeader, signed)
	return nil
}
