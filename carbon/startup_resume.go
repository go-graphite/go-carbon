package carbon

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/go-graphite/go-carbon/cache"
	"github.com/go-graphite/go-carbon/internal/inputlog"
	"github.com/go-graphite/go-carbon/internal/recovery"
)

const startupResumeName = "startup-replay-cursor.json"

// The cursor and cache snapshot form one recovery transaction. Publishing the
// cursor is the commit: before it, the original sources remain authoritative;
// after it, the snapshot precedes every unread source suffix. Missing snapshots
// have already crossed reclaim's store Sync and durable unlink boundary.
type startupResume struct {
	Version        int                              `json:"version"`
	Root           string                           `json:"root"`
	Snapshots      []string                         `json:"snapshots"`
	Offsets        map[string]int64                 `json:"offsets"`
	JournalOffsets map[uint16]inputlog.ReplayCursor `json:"journal_offsets,omitempty"`
}

func readStartupResume(dir string) (*startupResume, error) {
	f, err := os.Open(filepath.Join(dir, startupResumeName))
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var state startupResume
	dec := json.NewDecoder(io.LimitReader(f, 64<<20))
	if err = dec.Decode(&state); err != nil {
		return nil, fmt.Errorf("read startup replay cursor: %w", err)
	}
	if state.Version != 1 || filepath.Base(state.Root) != state.Root || !recovery.IsTail(state.Root) || len(state.Snapshots) == 0 {
		return nil, errors.New("invalid startup replay cursor")
	}
	for name, offset := range state.Offsets {
		if filepath.Base(name) != name || offset < 0 {
			return nil, errors.New("invalid startup replay source cursor")
		}
	}
	for _, name := range state.Snapshots {
		if filepath.Base(name) != name || !strings.HasPrefix(name, "startup-replay-snapshot.") {
			return nil, errors.New("invalid startup replay snapshot")
		}
	}
	return &state, nil
}

func (app *App) hasStartupResume() bool {
	return app.startupInput != nil && app.startupInput.resume != nil
}

// saveStartupResume requires joined replay/monitor/persistence and stopped
// pending recovery. Input may keep appending because it cannot enter the cache.
func (app *App) saveStartupResume(s *startupInput) error {
	return app.saveStartupResumeAt(s, func(string) {})
}

func (app *App) saveStartupResumeAt(s *startupInput, stage func(string)) error {
	if app.Cache == nil {
		return errors.New("cannot checkpoint startup replay without cache")
	}
	if app.MetricStore != nil {
		if err := app.MetricStore.Sync(); err != nil {
			return err
		}
	}
	dir := filepath.Dir(s.path)
	state := &startupResume{Version: 1, Root: filepath.Base(s.path), Offsets: make(map[string]int64)}
	if s.resume != nil {
		for name, offset := range s.resume.Offsets {
			state.Offsets[name] = offset
		}
		state.Snapshots = append(state.Snapshots, s.resume.Snapshots...)
		state.JournalOffsets = s.resume.JournalOffsets
	}
	if s.journalOffsets != nil {
		state.JournalOffsets = s.journalOffsets
	}
	for seg := s.legacy.first.Load(); seg != nil; seg = seg.next.Load() {
		if seg.path != "" {
			state.Offsets[filepath.Base(seg.path)] = seg.offset.Load()
		}
	}
	for _, sh := range s.shards {
		pos := sh.applied.Load()
		for seg := sh.first.Load(); seg != nil; seg = seg.next.Load() {
			offset := int64(0)
			if seg.replayed.Load() {
				offset = seg.size.Load()
			}
			if pos.segment == seg {
				offset = pos.offset
			}
			state.Offsets[filepath.Base(seg.path)] = offset
		}
	}
	name := fmt.Sprintf("startup-replay-snapshot.%d.bin", time.Now().UnixNano())
	writer, err := recovery.NewWriterFormat(filepath.Join(dir, name), 0, 1<<20, nil, recovery.FormatLegacy)
	if err != nil {
		return err
	}
	defer writer.Close()
	const shards = cache.ShardCount
	err = writer.WriteSegments(2*shards, dumpWorkers(), func(part int, out *recovery.Segment) error {
		if part < shards {
			return app.Cache.DumpPendingRange(part, shards, out)
		}
		return app.Cache.DumpShards(part-shards, part-shards+1, out.WritePoints)
	})
	if err != nil {
		return err
	}
	if _, err = writer.Close(); err != nil {
		return err
	}
	if err = syncDir(dir); err != nil {
		return err
	}
	stage("snapshot_synced")
	state.Snapshots = append([]string{name}, state.Snapshots...)
	raw, err := json.Marshal(state)
	if err != nil {
		return err
	}
	tmp, err := os.CreateTemp(dir, ".startup-replay-cursor-*")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name())
	_, err = tmp.Write(raw)
	if err == nil {
		err = tmp.Sync()
	}
	closeErr := tmp.Close()
	if err = errors.Join(err, closeErr); err != nil {
		return err
	}
	stage("cursor_synced")
	if err = os.Rename(tmp.Name(), filepath.Join(dir, startupResumeName)); err != nil {
		return err
	}
	stage("cursor_renamed")
	if err := syncDir(dir); err != nil {
		return err
	}
	stage("cursor_committed")
	return nil
}

func removeStartupResume(dir string) error {
	if err := os.Remove(filepath.Join(dir, startupResumeName)); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return syncDir(dir)
}
