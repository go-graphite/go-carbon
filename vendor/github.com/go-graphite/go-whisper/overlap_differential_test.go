package whisper

import (
	"bytes"
	"crypto/sha256"
	"testing"
)

// TestOverlapReplayDifferential runs realistic emitters through the overlap
// replay twice in lockstep: once merging the sidecar into a published file
// first, and once merging it in memory (selected by a filesystem that commits
// atomically). Main file bytes, sidecar bytes and every archive's fetch must
// match after every update. The digest tests extend the comparison to the
// code before the change.
func TestOverlapReplayDifferential(t *testing.T) {
	var replays int
	for i, sc := range digestScenarios() {
		if testing.Short() && i%3 != 0 {
			continue
		}
		var published [][32]byte
		runDigestScenario(t, t.TempDir(), sc, int64(i), nil, func(_ int, state []byte) {
			published = append(published, sha256.Sum256(state))
		})
		var mismatch int = -1
		before := overlapCount()
		runDigestScenario(t, t.TempDir(), sc, int64(i), atomicOSFileSystem{}, func(step int, state []byte) {
			if sum := sha256.Sum256(state); mismatch < 0 && (step >= len(published) || !bytes.Equal(sum[:], published[step][:])) {
				mismatch = step
			}
		})
		replays += int(overlapCount() - before)
		if mismatch >= 0 {
			t.Errorf("%s %v method=%d: in-memory merge diverges at update %d", sc.schema, sc.e, sc.method, mismatch)
		}
	}
	if replays == 0 {
		t.Fatal("no update took the replay path")
	}
}

func overlapCount() (n uint64) {
	for k := range overlapUpdates {
		n += overlapUpdates[k].Load()
	}
	return n
}
