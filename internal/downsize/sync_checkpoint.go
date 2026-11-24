package downsize

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"syscall"
	"time"
)

// The append journal remains the audit log. Once discovery and missing-name
// copying finish, their large selection payloads are no longer needed to resume
// verification. This cache seals that phase boundary, retaining verification
// receipts and replaying only the journal tail. It never manufactures a proof or
// changes the mutation frontier's decision to revalidate a metric.
const syncCheckpointLimit = 256 << 20

type syncCheckpointStamp struct {
	Size     int64
	Inode    uint64
	Modified int64
}

type syncPhaseCheckpoint struct {
	Schema         int
	Identity       string
	Options        string
	Offset         int64
	JournalInode   uint64
	Boundary       string
	Union, Missing syncCheckpointStamp
	Legacy         bool
	Copied         []string
	Summary        syncCheckpointSummary
	ReceiptFile    string
	ReceiptSHA256  string
	Rows           []syncRow
	Progress       progressSnapshot
}

type syncCheckpointSummary struct {
	Discovered, OnTarget, Missing, NotOwned, Short, Both int
	Copied, CopyFailed                                   int
	Residual                                             []syncResidual
}

type syncCheckpointEnvelope struct {
	SHA256  string
	Payload json.RawMessage
}

func checkpointStamp(path string) (syncCheckpointStamp, error) {
	st, err := os.Stat(path)
	if err != nil {
		return syncCheckpointStamp{}, err
	}
	return syncCheckpointStamp{st.Size(), st.Sys().(*syscall.Stat_t).Ino, st.ModTime().UnixNano()}, nil
}

func checkpointBoundary(f *os.File, offset int64) (string, error) {
	if offset <= 0 {
		return "", errors.New("empty checkpoint journal")
	}
	data := make([]byte, min(offset, 4096))
	if _, err := f.ReadAt(data, offset-int64(len(data))); err != nil {
		return "", err
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:]), nil
}

func readSyncCheckpoint(f *os.File, identity string, options ...string) (*syncPhaseCheckpoint, error) {
	// Always authenticate the actual journal header, even when the checkpoint
	// skips every byte of its old body.
	header, err := bufio.NewReader(io.NewSectionReader(f, 0, 1<<20)).ReadBytes('\n')
	if err != nil {
		return nil, nil
	}
	var row syncRow
	if err := json.Unmarshal(header, &row); err != nil {
		return nil, err
	}
	if row.Kind != "header" || row.Identity != identity {
		return nil, errors.New("checkpoint journal header changed")
	}
	p := f.Name() + ".checkpoint.json"
	st, err := os.Stat(p)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if st.Size() > syncCheckpointLimit {
		return nil, errors.New("checkpoint exceeds size limit")
	}
	data, err := os.ReadFile(p)
	if err != nil {
		return nil, err
	}
	var envelope syncCheckpointEnvelope
	if err := json.Unmarshal(data, &envelope); err != nil {
		return nil, err
	}
	sum := sha256.Sum256(envelope.Payload)
	if hex.EncodeToString(sum[:]) != envelope.SHA256 {
		return nil, errors.New("checkpoint checksum mismatch")
	}
	var cp syncPhaseCheckpoint
	if err := json.Unmarshal(envelope.Payload, &cp); err != nil {
		return nil, err
	}
	js, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if len(options) > 0 && cp.Options != options[0] {
		return nil, errors.New("checkpoint execution options changed")
	}
	if (cp.Schema != 1 && cp.Schema != 2) || cp.Identity != identity || cp.Offset <= 0 || cp.Offset > js.Size() || cp.JournalInode != js.Sys().(*syscall.Stat_t).Ino || len(cp.Rows) == 0 || cp.Rows[0].Kind != "header" || cp.Rows[0].Identity != identity {
		return nil, errors.New("checkpoint journal identity or extent changed")
	}
	boundary, err := checkpointBoundary(f, cp.Offset)
	if err != nil {
		return nil, err
	}
	if boundary != cp.Boundary {
		return nil, errors.New("checkpoint journal boundary changed")
	}
	for name, want := range map[string]syncCheckpointStamp{"union.tsv": cp.Union, "missing.tsv": cp.Missing} {
		got, err := checkpointStamp(filepath.Join(filepath.Dir(f.Name()), name))
		if err != nil {
			return nil, err
		}
		if got != want {
			return nil, fmt.Errorf("checkpoint catalog %s changed", name)
		}
	}
	if cp.Schema == 2 {
		if cp.ReceiptFile != filepath.Base(cp.ReceiptFile) || cp.ReceiptFile != filepath.Base(f.Name())+".receipts-"+cp.ReceiptSHA256+".jsonl" {
			return nil, errors.New("invalid checkpoint receipt path")
		}
		receipts, err := os.Open(filepath.Join(filepath.Dir(f.Name()), cp.ReceiptFile))
		if err != nil {
			return nil, err
		}
		defer receipts.Close()
		hash := sha256.New()
		decoder := json.NewDecoder(io.TeeReader(receipts, hash))
		cp.Rows = nil
		for {
			var row syncRow
			err := decoder.Decode(&row)
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				return nil, err
			}
			cp.Rows = append(cp.Rows, row)
		}
		if hex.EncodeToString(hash.Sum(nil)) != cp.ReceiptSHA256 || len(cp.Rows) == 0 || cp.Rows[0].Kind != "header" || cp.Rows[0].Identity != identity {
			return nil, errors.New("checkpoint receipt checksum or header mismatch")
		}
	}
	return &cp, nil
}

// Keep large receipt frontiers on disk. The size limit bounds the metadata
// envelope, never the amount of verification evidence a sync may retain.
func keepVerificationRow(row syncRow) bool {
	if row.Kind == "ownership-scope" {
		return true
	}
	if len(row.Resolved) > 0 {
		return true
	}
	switch row.Kind {
	case "legacy-round-repair", "legacy-round-adoption", "verification-round-scope", "verification-invalidated", "ambiguous-receipt", "receipt-reverified", "receipt-contradiction", "redo", "phase-regression", "phase-scope", "phase-progress", "phase-position", "header", "verified", "verification-retry", "verification-incomplete", "verification-pass", "window-resolved", "window-retry-visited", "window-retry-floor", "window-retry-terminal", "window-policy":
		return true
	case "window-selected":
		return len(row.Skipped) > 0
	case "copied":
		return row.Phase == "fill"
	}
	return false
}

func verificationCheckpointRows(rows []syncRow) ([]syncRow, error) {
	var kept []syncRow
	for _, row := range rows {
		if keepVerificationRow(row) {
			kept = append(kept, row)
		}
	}
	return kept, nil
}

func (j *syncJournal) spillCheckpointRows(cp *syncPhaseCheckpoint, budget int) error {
	metadata := *cp
	metadata.Rows = nil
	b, err := json.Marshal(metadata)
	if err != nil {
		return err
	}
	// Leave room for the metadata, array separators and a sidecar reference;
	// receipts near the inline limit must spill too, rather than fail later.
	budget -= len(b) + 512
	size := 0
	for _, row := range cp.Rows {
		b, err := json.Marshal(row)
		if err != nil {
			return err
		}
		size += len(b) + 1
		if size > budget {
			break
		}
	}
	if size <= budget {
		cp.Schema = 1
		cp.ReceiptFile = ""
		cp.ReceiptSHA256 = ""
		return nil
	}
	f, err := os.CreateTemp(filepath.Dir(j.file.Name()), ".receipts-*")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	hash := sha256.New()
	writer := bufio.NewWriterSize(io.MultiWriter(f, hash), 1<<20)
	encoder := json.NewEncoder(writer)
	for _, row := range cp.Rows {
		if err = encoder.Encode(row); err != nil {
			f.Close()
			return err
		}
	}
	err = errors.Join(writer.Flush(), f.Sync(), f.Close())
	if err != nil {
		return err
	}
	cp.ReceiptSHA256 = hex.EncodeToString(hash.Sum(nil))
	cp.ReceiptFile = filepath.Base(j.file.Name()) + ".receipts-" + cp.ReceiptSHA256 + ".jsonl"
	if err = os.Rename(f.Name(), filepath.Join(filepath.Dir(j.file.Name()), cp.ReceiptFile)); err != nil {
		return err
	}
	if err = syncDirectory(filepath.Dir(j.file.Name())); err != nil {
		return err
	}
	cp.Schema = 2
	cp.Rows = cp.Rows[:1] // authenticate the header before opening the external rows
	return nil
}

func (j *syncJournal) checkpointVerification() error {
	return j.checkpointVerificationBudget(syncCheckpointLimit / 2)
}

func (j *syncJournal) checkpointVerificationBudget(budget int) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.checkpoint == nil {
		return nil
	}
	cp := *j.checkpoint
	var err error
	cp.Rows, err = verificationCheckpointRows(j.rows)
	if err != nil {
		return err
	}
	cp.Progress = j.progress
	cp.Offset, err = j.file.Seek(0, io.SeekCurrent)
	if err != nil {
		return err
	}
	st, err := j.file.Stat()
	if err != nil {
		return err
	}
	cp.JournalInode = st.Sys().(*syscall.Stat_t).Ino
	cp.Boundary, err = checkpointBoundary(j.file, cp.Offset)
	if err != nil {
		return err
	}
	if err := j.file.Sync(); err != nil {
		return err
	}
	if err := j.spillCheckpointRows(&cp, budget); err != nil {
		return err
	}
	payload, err := json.Marshal(cp)
	if err != nil {
		return err
	}
	if len(payload) > syncCheckpointLimit/2 {
		return errors.New("phase checkpoint exceeds size budget")
	}
	sum := sha256.Sum256(payload)
	if err := atomicStateJSON(j.file.Name()+".checkpoint.json", syncCheckpointEnvelope{hex.EncodeToString(sum[:]), payload}); err != nil {
		return err
	}
	old := j.checkpoint.ReceiptFile
	// The live checkpoint retains its original rows; only the persisted envelope
	// carries the compact header plus external receipt reference.
	j.checkpoint.ReceiptFile, j.checkpoint.ReceiptSHA256 = cp.ReceiptFile, cp.ReceiptSHA256
	if old != "" && old != cp.ReceiptFile {
		_ = os.Remove(filepath.Join(filepath.Dir(j.file.Name()), old))
	}
	return nil
}

func (s *syncer) restoreVerificationCheckpoint() bool {
	cp := s.j.checkpoint
	if cp == nil {
		return false
	}
	c := cp.Summary
	s.sum.Discovered, s.sum.OnTarget, s.sum.Missing, s.sum.NotOwned, s.sum.Short, s.both = c.Discovered, c.OnTarget, c.Missing, c.NotOwned, c.Short, c.Both
	s.sum.Copied, s.sum.CopyFailed = c.Copied, c.CopyFailed
	s.sum.residual = append([]syncResidual(nil), c.Residual...)
	s.legacyCopyReceipts = cp.Legacy
	for _, n := range cp.Copied {
		s.copied[n] = true
	}
	event("sync_verification_checkpoint_resumed", map[string]any{"state": s.o.state, "journal_offset": cp.Offset, "catalog_names": c.Discovered})
	return true
}

func (s *syncer) startVerificationCheckpoint() {
	// A verify-only run can later be resumed with copying enabled through an API
	// caller; keep its copy discovery live. The CLI otherwise freezes this option.
	if s.o.noCopy || s.o.noVerify {
		return
	}
	// Avoid encoding a large named failure set merely to discover that it exceeds
	// the cache bound. Those runs retain the original, fully compatible replay.
	if len(s.sum.residual) > 0 || len(s.forbid) > 0 || len(s.failedCp) > 0 || s.legacyCopyReceipts && len(s.copied) > 100000 {
		return
	}
	if q, p := s.o.issues.counts(); q > 0 || p > 0 {
		return
	}
	identity, err := syncIdentity(s.o)
	if err != nil {
		return
	}
	cp := &syncPhaseCheckpoint{Schema: 1, Identity: identity, Options: syncCheckpointOptions(s.o), Legacy: s.legacyCopyReceipts,
		Summary: syncCheckpointSummary{s.sum.Discovered, s.sum.OnTarget, s.sum.Missing, s.sum.NotOwned, s.sum.Short, s.both, s.sum.Copied, s.sum.CopyFailed, append([]syncResidual(nil), s.sum.residual...)}}
	cp.Union, err = checkpointStamp(s.unionPath())
	if err == nil {
		cp.Missing, err = checkpointStamp(s.missingPath())
	}
	if err != nil {
		return
	}
	if cp.Legacy {
		cp.Copied = sortedKeys(s.copied)
	}
	s.j.checkpoint = cp
	if err := s.j.checkpointVerification(); err != nil {
		event("sync_checkpoint_unavailable", map[string]any{"error": err.Error(), "journal_retained": true})
	}
}

// These are cache eligibility, not journal identity: changing them falls back
// to normal replay, preserving that protocol's option checks and copy retries.
func syncCheckpointOptions(o syncOptions) string {
	if o.now == nil {
		o.now = time.Now
	}
	data, _ := json.Marshal(struct {
		NoCopy, NoVerify, FullHistory bool
		Settle                        time.Duration
		Batch                         int
		Windows                       syncRanges
	}{o.noCopy, o.noVerify, o.fullHistory, o.settle, o.batch, (&syncer{o: o}).copyWindows()})
	return string(data)
}
