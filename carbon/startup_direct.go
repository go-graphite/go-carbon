package carbon

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
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

func (s *startupInput) refuseDirect(now time.Time, reason string) {
	if s.refusedAt.IsZero() || now.Sub(s.refusedAt) >= time.Minute {
		s.refusedAt = now
		zapwriter.Logger("app").Warn("startup capture direct switch deferred", zap.String("reason", reason), zap.Duration("since_start", now.Sub(s.started)))
	}
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
		if s.liveSince.IsZero() {
			s.refuseDirect(now, "input log is disabled; retaining durable capture")
			s.liveSince = now
		}
		return
	}
	for _, sh := range s.shards {
		if !sh.live.Load() {
			s.liveSince = time.Time{}
			s.refuseDirect(now, "not every partition is live")
			return
		}
	}
	if s.liveSince.IsZero() {
		s.liveSince = now
		return
	}
	if now.Sub(s.liveSince) < startupDirectDelay {
		return
	}
	// A previous journal boundary must disappear before creating another one.
	// Its final retirement sync makes old corrections ineligible for replay.
	if s.legacy.first.Load() != nil {
		s.refuseDirect(now, "previous history sources have not retired")
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
// advance the applied positions, so refusal cannot leave a partially sealed set.
func (s *startupInput) sealForDirect() error {
	for _, sh := range s.shards {
		if err := sh.fence(false); err != nil {
			return err
		}
	}
	if s.releaseLog == nil {
		release, err := s.sink.Load().startLog()
		if err != nil {
			return err
		}
		s.releaseLog = release
	}
	for _, sh := range s.shards {
		if err := sh.fence(true); err != nil {
			return err
		}
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
