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
	if s.releaseLog != nil && s.directFence == nil {
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
	if err := s.receiver.SwitchCapture(sink.directAdd, s.sealForDirect); err != nil {
		s.liveSince = time.Time{}
		s.refuseDirect(now, err.Error())
		return
	}
	s.direct.Store(true)
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

// Called only with the receiver's exclusive submission fence held. No append
// can arrive between the all-partition check and the seals. Replay can only
// advance the applied positions; a frontier refusal cannot partially seal them.
func (s *startupInput) sealForDirect() error {
	// With submissions fenced, a small final reader suffix can drain without
	// competing with more input. Bound the pause well below a relay queue.
	deadline := time.Now().Add(250 * time.Millisecond)
	for _, sh := range s.shards {
		for {
			err := sh.fence(false)
			if err == nil {
				break
			}
			if sh.err.Load() != nil || time.Now().After(deadline) {
				return err
			}
			time.Sleep(time.Millisecond)
		}
	}
	var promotions uint64
	for _, sh := range s.shards {
		promotions += sh.promoted.Load()
	}
	if promotions != s.livePromotions {
		return errors.New("a partition was promoted during the direct fence")
	}
	if s.releaseLog == nil {
		release, err := s.sink.Load().startLog()
		if err != nil {
			return err
		}
		s.releaseLog = release
	}
	var wg sync.WaitGroup
	errs := make([]error, len(s.shards))
	for i, sh := range s.shards {
		wg.Go(func() { errs[i] = sh.fence(true) })
	}
	wg.Wait()
	if err := errors.Join(errs...); err != nil {
		return err
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
	if err := removeStartupResume(filepath.Dir(s.path)); err != nil {
		return err
	}
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
