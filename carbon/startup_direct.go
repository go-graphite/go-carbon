package carbon

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/go-graphite/go-carbon/internal/handoff"
	"github.com/lomik/zapwriter"
	"go.uber.org/zap"
)

const startupDirectDelay = 5 * time.Second

// An empty terminal segment records the journal's place in the existing epoch
// ordering. It uses the ordinary capture filename/encoding and retires last.
const startupDirectSuffix = ".999999.999999999999.tail.bin"

func isStartupDirectFence(path string) bool { return strings.HasSuffix(path, startupDirectSuffix) }

func (s *startupInput) refuseDirect(now time.Time, reason string) bool {
	if s.refusedAt.IsZero() || now.Sub(s.refusedAt) >= time.Minute {
		s.refusedAt = now
		zapwriter.Logger("app").Warn("startup capture direct switch deferred", zap.String("reason", reason), zap.Duration("since_start", now.Sub(s.started)))
		return true
	}
	return false
}

// The monitor owns attempts; shutdown takes the same mutex before choosing its
// capture or ordinary dump path. The receiver fence joins all submitted writes.
func (s *startupInput) maybeDirect(now time.Time) {
	s.transition.Lock()
	defer s.transition.Unlock()
	if s.direct.Load() || s.switchStopped {
		return
	}
	sink := s.sink.Load()
	if sink == nil || !s.historyReady.Load() {
		s.refuseDirect(now, "history replay has not completed")
		return
	}
	if sink.startLog == nil || sink.directAdd == nil {
		// A disabled journal cannot protect ordinary input after the switch.
		if !s.disabledWarned {
			s.disabledWarned = s.refuseDirect(now, "input log is disabled; retaining durable capture")
		}
		return
	}
	var promotions uint64
	for _, sh := range s.shards {
		if failure := sh.err.Load(); failure != nil {
			s.refuseDirect(now, failure.err.Error())
			return
		}
		if !sh.live.Load() {
			s.liveSince = time.Time{}
			s.refuseDirect(now, "not every partition is live")
			return
		}
		promotions += sh.promoted.Load()
	}
	if s.liveSince.IsZero() || promotions != s.livePromotions {
		s.liveSince = now
		s.livePromotions = promotions
		return
	}
	if now.Sub(s.liveSince) < startupDirectDelay {
		return
	}
	if s.releaseLog != nil && s.directFence == nil && !s.preparingDirect {
		// Recovery owns this pin for a previous direct epoch. Reusing it as
		// the new switch's pin would move that earlier journal after newer
		// captured corrections. Its history must retire first.
		if sink.drain != nil {
			sink.drain(true)
		}
		s.refuseDirect(now, "earlier direct journal remains pinned by history")
		return
	}
	if s.receiver == nil {
		return
	}
	// Journal pin/reset and the terminal ordering marker may synchronize disk.
	// Capture stays open during all preparation, including a failed attempt.
	// Do not require an unforced globally idle instant before closing the
	// submission gate: continuous input can keep at least one append pending.
	if err := s.prepareDirect(); err != nil {
		s.refuseDirect(now, err.Error())
		return
	}
	if err := s.receiver.SwitchCapture(sink.directAdd, s.checkDirectFrontiers); err != nil {
		s.liveSince = time.Time{}
		s.refuseDirect(now, err.Error())
		return
	}
	s.direct.Store(true)
	// No receiver can append to the old files now. Sync/seal them without the
	// receiver fence; direct input is protected by the pinned journal. Until
	// sync succeeds these files remain unsealed and cannot be retired.
	if err := s.sealForDirect(); err != nil {
		s.legacy.fail(err)
		zapwriter.Logger("app").Error("direct input active; retaining capture sources after seal failure", zap.Error(err))
	}
	if sink.drain != nil {
		sink.drain(false)
	}
	frontiers := make([]string, 0, len(s.shards))
	for i, sh := range s.shards {
		p := sh.applied.Load()
		frontiers = append(frontiers, fmt.Sprintf("%d:%s:%d", i, filepath.Base(p.segment.path), p.offset))
	}
	zapwriter.Logger("app").Info("startup capture switched to direct journalled input", zap.Strings("frontiers", frontiers), zap.Duration("since_start", time.Since(s.started)), zap.Int64("captured_points", s.points.Load()))
}

func (s *startupShard) fence(seal bool) error {
	reply := make(chan error, 1)
	s.requests <- startupAppend{fenced: reply, seal: seal}
	return <-reply
}

// Called with submissions joined. Append acknowledgements already published
// the file frontiers. A busy replay simply refuses this attempt; there are no
// channel waits, syncs or sleeps inside the receiver's exclusive fence.
func (s *startupInput) checkDirectFrontiers() error {
	for _, sh := range s.shards {
		if failure := sh.err.Load(); failure != nil {
			return failure.err
		}
		cur, applied := sh.current.Load(), sh.applied.Load()
		if sh.pending.Load() != 0 || !sh.live.Load() || applied.segment != cur || applied.offset != cur.size.Load() {
			return errors.New("partition is behind its append frontier")
		}
	}
	var promotions uint64
	for _, sh := range s.shards {
		promotions += sh.promoted.Load()
	}
	if promotions != s.livePromotions {
		return errors.New("a partition was promoted during the direct fence")
	}
	return nil
}

// Publish the journal's ordering marker before the direct sink is visible.
// While preparation runs, further capture records still precede that marker.
// On retry, retain our own pin without confusing it with an older direct epoch.
func (s *startupInput) prepareDirect() error {
	if s.releaseLog == nil {
		release, err := s.sink.Load().startLog()
		if err != nil {
			return err
		}
		s.releaseLog = release
		s.preparingDirect = true
	}
	if seg := s.directFence; seg != nil {
		return errors.Join(seg.file.Sync(), syncDir(filepath.Dir(seg.path)))
	}
	path := fmt.Sprintf("%s.capture-epoch-%020d%s", s.path, s.epoch, startupDirectSuffix)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0600)
	if err != nil {
		return err
	}
	unlock, err := handoff.LockExclusive(path)
	if err != nil {
		_ = f.Close()
		return err
	}
	s.directFence = &startupSegment{file: f, path: path, unlock: unlock}
	return errors.Join(f.Sync(), syncDir(filepath.Dir(path)))
}

func (s *startupInput) sealForDirect() error {
	if err := s.prepareDirect(); err != nil {
		return err
	}
	var wg sync.WaitGroup
	errs := make([]error, len(s.shards))
	for i, sh := range s.shards {
		wg.Go(func() { errs[i] = sh.fence(true) })
	}
	wg.Wait()
	return errors.Join(errs...)
}

func (s *startupInput) stopDirectSwitch() bool {
	s.transition.Lock()
	defer s.transition.Unlock()
	s.switchStopped = true
	return s.direct.Load()
}

// A newer durable dump is also authority for unconfirmed capture sources. Keep
// the ordinary stop's dump-before-removal boundary, then remove each old prefix
// before the terminal fence. No receiver can submit to these files after direct.
func (s *startupInput) retireAfterDump() error {
	if err := s.close(); err != nil {
		return err
	}
	for _, sh := range append([]*startupShard{s.legacy}, s.shards...) {
		for seg := sh.first.Load(); seg != nil; seg = seg.next.Load() {
			if seg.file == nil || seg.retired.Load() {
				continue
			}
			if err := os.Remove(seg.path); err != nil && !errors.Is(err, os.ErrNotExist) {
				return err
			}
			if err := syncDir(filepath.Dir(seg.path)); err != nil {
				return err
			}
			seg.retired.Store(true)
		}
	}
	if seg := s.directFence; seg != nil && !seg.retired.Load() {
		if err := os.Remove(seg.path); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
		if err := syncDir(filepath.Dir(seg.path)); err != nil {
			return err
		}
		_ = seg.file.Close()
		_ = seg.unlock()
		seg.retired.Store(true)
	}
	if s.releaseLog != nil {
		return s.releaseLog()
	}
	return nil
}
