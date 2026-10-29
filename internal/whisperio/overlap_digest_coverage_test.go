package whisperio

import (
	"os"
	"testing"
	"time"

	whisper "github.com/go-graphite/go-whisper"
)

// TestContinuationOverlapDigestCoverage checks that the digest workload takes
// the replay path, with and without a sidecar to merge first.
func TestContinuationOverlapDigestCoverage(t *testing.T) {
	if os.Getenv("OVERLAP_DIGEST") == "" {
		t.Skip("set OVERLAP_DIGEST=1")
	}
	total := func() (n uint64) {
		for _, k := range whisper.OverlapKinds() {
			n += whisper.OverlapUpdates(k)
		}
		return n
	}
	var withSidecar, without int
	var sidecar bool
	var count uint64
	digestBatchHook = func(before bool, kv mapContinuationIO, now int) {
		if before {
			fs := newContinuationFiles(kv, true)
			w, err := whisper.OpenWithOptions(fs.main(), continuationOptions(fs, func() time.Time { return time.Unix(int64(now), 0) }))
			if err != nil {
				t.Fatal(err)
			}
			sidecar = w.OutOfOrderPath() != ""
			w.Close()
			count = total()
			return
		}
		if total() != count {
			if sidecar {
				withSidecar++
			} else {
				without++
			}
		}
	}
	defer func() { digestBatchHook = nil }()
	before := map[whisper.OverlapKind]uint64{}
	for _, k := range whisper.OverlapKinds() {
		before[k] = whisper.OverlapUpdates(k)
	}
	TestContinuationOverlapDigest(t)
	for _, k := range whisper.OverlapKinds() {
		t.Logf("%s: %d", k, whisper.OverlapUpdates(k)-before[k])
	}
	t.Logf("replays with a sidecar: %d, without: %d", withSidecar, without)
	if withSidecar == 0 || without == 0 {
		t.Fatal("digest workload does not cover both replay variants")
	}
}
