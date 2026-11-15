package whisperio

import (
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	whisper "github.com/go-graphite/go-whisper"
)

// continuationHead is an exclusively owned transaction capsule. It is moved out
// of the retained table before use, never shallow-cloned while still reachable
// there. This is a linear resume boundary: failure drops it; successful database
// commit alone returns it. No reader or concurrent transaction can observe it.
// Keeping the private Whisper encoder safe this way needs no dependency fork.
type continuationHead struct {
	writer *whisper.Whisper
	fs     *continuationFiles
	file   *continuationFile
	values []whisper.TimeSeriesPoint
	points []*whisper.TimeSeriesPoint
}

func (h *continuationHead) rebind(kv store.ContinuationIO, clock func() time.Time) *continuationFiles {
	fs := h.fs
	fs.kv, fs.prober = kv, nil
	fs.prober, _ = kv.(store.ContinuationProber)
	fs.withClock(clock)
	fs.classicConflict = nil
	// Close has released only our virtual handle. All header/encoder state is
	// still private in this capsule and belongs to the checked revision.
	h.file.closed = false
	h.file.data.refs++
	return fs
}

func (h *continuationHead) pointScratch(n int) ([]whisper.TimeSeriesPoint, []*whisper.TimeSeriesPoint) {
	if cap(h.values) < n {
		h.values = make([]whisper.TimeSeriesPoint, n)
	}
	if cap(h.points) < n {
		h.points = make([]*whisper.TimeSeriesPoint, 0, n)
	}
	return h.values[:n], h.points[:0]
}

func (s *ownerHeadSession) resumeCharge() int64 {
	if s.resume == nil {
		return 0
	}
	h := s.resume
	// Include Go writer/archive state, virtual maps, duplicate header buffers,
	// block ranges and reusable input storage. This is a conservative retained
	// allowance, not a bound on existing transaction scratch or total RSS.
	return 16384 + 4*int64(len(h.file.data.header)) + int64(cap(h.values))*16 + int64(cap(h.points))*8
}

func (s *ownerHeadSession) detach() {
	if s.resume == nil {
		return
	}
	fs := s.resume.fs
	// Do not pin an indexed batch/snapshot, context timer or transaction wrapper
	// for the lifetime of a retained metric.
	fs.kv, fs.prober, fs.head, fs.timer = nil, nil, nil, nil
	// Reference payloads and verification buffers are operation scratch, not
	// part of the charged hot-head cache. Never retain arbitrary historical
	// payloads merely because an update happened to materialize them.
	fs.referencedPoints, fs.verifiedBlocks = nil, nil
	fs.snapshotBlock, fs.snapshotBlockOK = nil, false
}
