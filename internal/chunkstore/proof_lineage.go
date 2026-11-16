package chunkstore

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/cockroachdb/pebble"
)

const proofLineageFile = "PROOF-LINEAGE"

var proofIdentityKey = []byte("proof/identity")

type proofLineage struct {
	Version                                           int
	Incarnation, Era, Process, Previous, PreviousHash string
	Sequence                                          uint64
	Clean                                             bool
	Reason                                            string
}

func lineageHash(data []byte) string { h := sha256.Sum256(data); return hex.EncodeToString(h[:]) }
func (s *Store) readProofFile(name string) ([]byte, error) {
	f, err := s.fs.Open(s.fs.PathJoin(s.dir, name))
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return io.ReadAll(io.LimitReader(f, 64<<10))
}
func (s *Store) publishProofFile(name string, data []byte) error {
	path := s.fs.PathJoin(s.dir, name)
	tmp := path + ".tmp"
	f, err := s.fs.Create(tmp)
	if err != nil {
		return err
	}
	n, err := f.Write(data)
	if err == nil && n != len(data) {
		err = io.ErrShortWrite
	}
	if err = errors.Join(err, f.Sync(), f.Close()); err != nil {
		return err
	}
	if err = s.fs.Rename(tmp, path); err != nil {
		return err
	}
	return syncStoreDirectory(s.fs, s.dir)
}
func validProcessID(id string) bool {
	_, err := hex.DecodeString(id)
	return len(id) == 32 && err == nil
}
func processID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err)
	}
	return hex.EncodeToString(b[:])
}
func (s *Store) validateProofChain(head proofLineage, data []byte) error {
	seen := map[string]bool{}
	for links := 0; ; links++ {
		if head.Version != 1 || !validProcessID(head.Process) || head.Incarnation == "" || head.Era == "" || seen[head.Process] || links >= 1024 {
			return errors.New("unknown or broken process chain")
		}
		seen[head.Process] = true
		receipt, err := s.readProofFile("PROOF-" + head.Process)
		if err != nil || lineageHash(receipt) != lineageHash(data) {
			return errors.New("missing process chain link")
		}
		if head.Previous == "" {
			return nil
		}
		if !validProcessID(head.Previous) {
			return errors.New("invalid predecessor")
		}
		data, err = s.readProofFile("PROOF-" + head.Previous)
		if err != nil || lineageHash(data) != head.PreviousHash {
			return errors.New("missing predecessor chain link")
		}
		var prev proofLineage
		if err = json.Unmarshal(data, &prev); err != nil {
			return err
		}
		if prev.Incarnation != head.Incarnation || prev.Era != head.Era || !prev.Clean {
			return errors.New("unclean predecessor chain")
		}
		head = prev
	}
}
func (s *Store) openProofLineage() error {
	next := proofLineage{Version: 1, Incarnation: processID(), Era: processID(), Process: processID(), Reason: "storage initialized"}
	var old proofLineage
	var identity proofLineage
	identityData, closer, identityErr := s.db.Get(proofIdentityKey)
	if identityErr == nil {
		identityErr = json.Unmarshal(identityData, &identity)
		closer.Close()
	}
	if identityErr != nil && !errors.Is(identityErr, pebble.ErrNotFound) {
		next.Reason = "unknown storage identity"
	}
	data, readErr := s.readProofFile(proofLineageFile)
	if readErr == nil && json.Unmarshal(data, &old) == nil && identityErr == nil && identity.Version == 1 && identity.Incarnation != "" {
		next.Incarnation = identity.Incarnation
		switch {
		case identity.Process != old.Process || identity.Incarnation != old.Incarnation:
			next.Reason = "storage rollback or interrupted publish"
		case s.validateProofChain(old, data) != nil:
			next.Reason = "missing or unknown process chain"
		case !old.Clean:
			next.Reason = "unclean predecessor"
		case uint64(s.db.VisibleSequence()) != old.Sequence:
			next.Reason = "storage sequence changed outside handoff"
		default:
			next.Era = old.Era
			next.Previous = old.Process
			next.PreviousHash = lineageHash(data)
			next.Reason = "clean predecessor"
		}
	} else if !errors.Is(readErr, os.ErrNotExist) || identityErr == nil {
		next.Reason = "missing or unknown process chain"
	}
	// The DB marker and independently synced receipt cross-check partial publish
	// and restoring database files while retaining newer lineage receipts.
	encoded, _ := json.Marshal(next)
	if err := s.db.Set(proofIdentityKey, encoded, pebble.Sync); err != nil {
		return err
	}
	next.Sequence = uint64(s.db.VisibleSequence())
	if err := s.publishProofLineage(next); err != nil {
		return err
	}
	s.proofLineage = next
	return nil
}
func (s *Store) publishProofLineage(r proofLineage) error {
	data, err := json.Marshal(r)
	if err != nil {
		return err
	}
	if err = s.publishProofFile("PROOF-"+r.Process, data); err != nil {
		return fmt.Errorf("publish process receipt: %w", err)
	}
	return s.publishProofFile(proofLineageFile, data)
}
func (s *Store) sealProofLineage() error {
	if s.proofLineage.Process == "" {
		return nil
	}
	// The background WAL syncer has already joined at this close boundary.
	if err := s.logSync(); err != nil {
		return err
	}
	r := s.proofLineage
	r.Clean = true
	r.Sequence = uint64(s.db.VisibleSequence())
	return s.publishProofLineage(r)
}

// ProofLineage describes the already validated process chain. A clean storage
// lineage proves committed data continuity only, not lossless upstream intake.
func (s *Store) ProofLineage() (incarnation, process, predecessor, reason string) {
	r := s.proofLineage
	return r.Incarnation, r.Process, r.Previous, r.Reason
}
