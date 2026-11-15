package downsize

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"
)

var errSyncNoProof = errors.New("store has no mutation proof endpoint")

type syncProofBackend interface {
	Proofs(context.Context, string, []string) ([]string, error)
}

// Catalog line numbers are stable IDs. One 32-byte record per ID holds a
// 128-bit all-holder mutation fence and a 128-bit scope/name integrity seal.
// Zero, missing and torn records are pending. No metric names live in RAM or
// in a second catalog. Changing verification batch boundaries keeps the IDs.
type syncProof [16]byte
type syncFrontier struct {
	file  *os.File
	scope struct {
		Identity string
		Settle   time.Duration
		Now      int64
	}
	key string
}

func (s *syncer) openFrontier(ctx context.Context) error {
	b, ok := s.b.(syncProofBackend)
	if !ok {
		return nil
	}
	source := s.unionSource()
	name, _, err := source.next()
	source.close()
	if errors.Is(err, io.EOF) {
		return nil
	}
	if err != nil {
		return err
	}
	// Negotiate every holder before adopting a fixed verification generation.
	// A legacy holder keeps the complete current verification path.
	for _, store := range append([]string{s.o.to}, s.o.from...) {
		got, err := b.Proofs(ctx, store, []string{name})
		if errors.Is(err, errSyncNoProof) {
			if _, e := os.Stat(filepath.Join(s.o.state, "verification-scope.json")); errors.Is(e, os.ErrNotExist) {
				return nil
			} else if e != nil {
				return e
			}
			continue
		}
		if err != nil {
			return err
		}
		if len(got) != 1 {
			return errors.New("invalid mutation proof response")
		}
	}
	for _, w := range s.o.windows {
		if w.Until > s.o.now().Unix()-int64(s.o.settle.Seconds()) {
			return nil
		}
	}
	f := &syncFrontier{}
	f.scope.Identity, err = syncIdentity(s.o)
	if err != nil {
		return err
	}
	f.scope.Settle = s.o.settle
	f.scope.Now = s.o.now().Unix()
	path := filepath.Join(s.o.state, "verification-scope.json")
	data, err := os.ReadFile(path)
	if err == nil {
		expected := f.scope
		if err = json.Unmarshal(data, &f.scope); err != nil {
			return err
		}
		if f.scope.Identity != expected.Identity || f.scope.Settle != expected.Settle || f.scope.Now <= 0 || f.scope.Now > expected.Now {
			return errors.New("verification scope changed; use a fresh state directory")
		}
	} else if errors.Is(err, os.ErrNotExist) {
		if err = atomicStateJSON(path, f.scope); err != nil {
			return err
		}
	} else {
		return err
	}
	data, _ = json.Marshal(f.scope)
	f.key = string(data)
	f.file, err = os.OpenFile(filepath.Join(s.o.state, "verification-frontier.bin"), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return err
	}
	s.frontier = f
	return nil
}

func (s *syncer) verificationHorizon() int64 {
	now := s.o.now().Unix()
	if s.frontier != nil {
		now = min(now, s.frontier.scope.Now)
	}
	return now - int64(s.o.settle.Seconds())
}

// proofStamps reads only metadata. The serving-index schema participates too:
// a native metric without its serving-index entry is not a clean read proof.
func (s *syncer) proofStamps(ctx context.Context, names []string) ([]syncProof, error) {
	stores := append([]string{s.o.to}, s.o.from...)
	tokens := make([][]string, len(stores))
	errs := make([]error, len(stores))
	infos := make([]map[string][]syncArchive, len(stores))
	infoErrs := make([]error, len(stores))
	var wg sync.WaitGroup
	for i, store := range stores {
		wg.Add(2)
		go func() { defer wg.Done(); infos[i], infoErrs[i] = s.b.Info(ctx, store, names) }()
		go func() { defer wg.Done(); tokens[i], errs[i] = s.b.(syncProofBackend).Proofs(ctx, store, names) }()
	}
	wg.Wait()
	for _, err := range infoErrs {
		if err != nil {
			return nil, err
		}
	}
	for i, err := range errs {
		if err != nil && !errors.Is(err, errSyncNoProof) {
			return nil, err
		}
		if err == nil && len(tokens[i]) != len(names) {
			return nil, errors.New("invalid mutation proof response")
		}
	}
	out := make([]syncProof, len(names))
	for i, n := range names {
		hash := sha256.New()
		fmt.Fprintln(hash, s.frontier.key, n)
		known := true
		for store := range stores {
			if errs[store] != nil || tokens[store][i] == "" {
				known = false
				break
			}
			fmt.Fprintln(hash, stores[store], tokens[store][i])
			schema, _ := json.Marshal(infos[store][n])
			hash.Write(schema)
			// A fixed window can age into another serving archive without a write.
			// Its selected archive therefore participates in the mutation fence.
			for _, w := range s.o.windows {
				if w.From >= min(w.Until, s.verificationHorizon()) {
					continue
				}
				step := int64(-1)
				for _, a := range infos[store][n] {
					if a.Step*a.Points >= s.o.now().Unix()-w.From+1 {
						step = a.Step
						break
					}
				}
				fmt.Fprintln(hash, w.Name, step)
			}
		}
		if known {
			copy(out[i][:], hash.Sum(nil))
		}
	}
	return out, nil
}

func (f *syncFrontier) record(name string, stamp syncProof) [32]byte {
	var record [32]byte
	if stamp == (syncProof{}) {
		return record
	}
	copy(record[:16], stamp[:])
	seal := sha256.Sum256(append([]byte(f.key+"\x00"+name+"\x00"), stamp[:]...))
	copy(record[16:], seal[:16])
	return record
}

func (s *syncer) verifyFrontierBatch(ctx context.Context, round int, names []string, ids []int64) (syncRow, error) {
	before, err := s.proofStamps(ctx, names)
	if err != nil {
		return syncRow{}, err
	}
	// Catalog batches are contiguous. Read/write their compact records together
	// rather than issuing one filesystem operation per metric at catalog scale.
	var block []byte
	if span := ids[len(ids)-1] - ids[0] + 1; span > 0 && span <= int64(2*len(ids)) {
		block = make([]byte, span*32)
		if _, err := s.frontier.file.ReadAt(block, ids[0]*32); err != nil && !errors.Is(err, io.EOF) {
			return syncRow{}, err
		}
	}
	selected := make([]string, 0, len(names))
	positions := make([]int, 0, len(names))
	for i, n := range names {
		var saved [32]byte
		if block != nil {
			copy(saved[:], block[(ids[i]-ids[0])*32:])
		} else if _, err := s.frontier.file.ReadAt(saved[:], ids[i]*32); err != nil && !errors.Is(err, io.EOF) {
			return syncRow{}, err
		}
		if before[i] != (syncProof{}) && saved == s.frontier.record(n, before[i]) {
			continue
		}
		selected = append(selected, n)
		positions = append(positions, i)
	}
	row := syncRow{Kind: "verified", Round: round}
	if len(selected) > 0 {
		row, err = s.verifyBatch(ctx, round, selected)
		if err != nil {
			return row, err
		}
		after, err := s.proofStamps(ctx, selected)
		if err != nil {
			return row, err
		}
		dirty := map[string]bool{}
		for _, h := range row.Holes {
			dirty[h.Name] = true
		}
		for _, n := range row.Unindexed {
			dirty[n] = true
		}
		for _, k := range row.Skipped {
			dirty[k.Name] = true
		}
		// Eligibility/absence exclusions may depend on policy outside the native
		// metric config. Re-resolve them instead of persisting a clean data proof.
		for _, k := range row.Resolved {
			dirty[k.Name] = true
		}
		for _, n := range row.Short {
			dirty[n] = true
		}
		for j, i := range positions {
			stamp := syncProof{}
			if before[i] != (syncProof{}) && before[i] != after[j] {
				row.Skipped = append(row.Skipped, syncSkip{names[i], "store changed during verification"})
				dirty[names[i]] = true
			}
			if !dirty[names[i]] && before[i] == after[j] {
				stamp = after[j]
			}
			record := s.frontier.record(names[i], stamp)
			if block != nil {
				copy(block[(ids[i]-ids[0])*32:], record[:])
			} else if _, err = s.frontier.file.WriteAt(record[:], ids[i]*32); err != nil {
				return row, err
			}
		}
		if block != nil {
			if _, err = s.frontier.file.WriteAt(block, ids[0]*32); err != nil {
				return row, err
			}
		}
	}
	row.First, row.Last, row.Names = names[0], names[len(names)-1], len(names)
	row.Reused = len(names) - len(selected)
	return row, nil
}

// A later requested parity period cannot silently expand a frozen history
// generation. Such a tail is still unproven even when a parity sample is clean.
func frontierReadyBounds(o syncOptions, r syncReadyOptions, report *readyReport) error {
	data, err := os.ReadFile(filepath.Join(o.state, "verification-scope.json"))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	var f syncFrontier
	if err = json.Unmarshal(data, &f.scope); err != nil {
		return err
	}
	horizon := f.scope.Now - int64(f.scope.Settle.Seconds())
	report.HistoryVerifiedUntil = horizon
	if r.scope != "windows" {
		for _, w := range r.period {
			if w.Until > horizon {
				report.Reasons = append(report.Reasons, fmt.Sprintf("parity period %s extends past history verification horizon %d; a new history scope is required", w.Name, horizon))
			}
		}
	}
	return nil
}
