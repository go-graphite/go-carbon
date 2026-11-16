package carbon

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/go-graphite/go-carbon/internal/handoff"
	"github.com/go-graphite/go-carbon/internal/inputlog"
	"github.com/go-graphite/go-carbon/internal/recovery"
	"github.com/go-graphite/go-carbon/points"
	"github.com/lomik/zapwriter"
	"go.uber.org/zap"
)

type discardedStartup struct {
	Version      int       `json:"version"`
	RecordedAt   time.Time `json:"recorded_at"`
	CaptureStart time.Time `json:"capture_start"`
	MinTimestamp int64     `json:"min_point_timestamp"`
	MaxTimestamp int64     `json:"max_point_timestamp"`
	Points       int64     `json:"points"`
	Files        []string  `json:"files"`
	ScanErrors   []string  `json:"scan_errors,omitempty"`
	// For a torn record, the safe backfill window extends through RecordedAt.
	BackfillFrom int64 `json:"backfill_from"`
	BackfillTo   int64 `json:"backfill_to"`
}

// The manifest is durable before the first rename. A restart completes its
// remaining moves before opening intake, including after only some files moved.
// No spool or snapshot is deleted. The operator owns replica backfill/cleanup.
func (app *App) discardLegacyStartup() (bool, error) {
	dir := app.Config.Dump.Path
	entries, err := os.ReadDir(dir)
	if err != nil {
		return false, err
	}
	discarded := false
	for _, entry := range entries {
		if !entry.IsDir() || !strings.HasPrefix(entry.Name(), "startup-discard.") {
			continue
		}
		path := filepath.Join(dir, entry.Name())
		data, err := os.ReadFile(filepath.Join(path, "window.json"))
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return false, err
		}
		var record discardedStartup
		if err = json.Unmarshal(data, &record); err != nil {
			return false, err
		}
		if record.Version != 1 {
			return false, errors.New("invalid legacy startup window")
		}
		moved, err := completeStartupDiscard(dir, path, &record)
		if err != nil {
			return false, err
		}
		if moved {
			app.recordStartupDiscard(path, &record)
		}
		discarded = discarded || moved
	}
	entries, err = os.ReadDir(dir)
	if err != nil {
		return false, err
	}
	var names []string
	var snapshots []string
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		if recovery.IsCaptureSegment(entry.Name()) {
			names = append(names, entry.Name())
		}
		if entry.Name() == startupResumeName || strings.HasPrefix(entry.Name(), "startup-replay-snapshot.") {
			snapshots = append(snapshots, entry.Name())
		}
	}
	if len(names) == 0 {
		return discarded, nil
	}
	record := discardedStartup{Version: 1, RecordedAt: time.Now().UTC(), Files: append(names, snapshots...)}
	for _, name := range names {
		if _, epoch, ok := strings.Cut(name, ".capture-epoch-"); ok {
			token, _, _ := strings.Cut(epoch, ".")
			if ns, err := strconv.ParseInt(token, 10, 64); err == nil {
				started := time.Unix(0, ns)
				if record.CaptureStart.IsZero() || started.Before(record.CaptureStart) {
					record.CaptureStart = started
				}
			}
		}
		unlock, err := handoff.LockExclusive(filepath.Join(dir, name))
		if err != nil {
			return false, err
		}
		err = scanDiscardedStartup(filepath.Join(dir, name), &record)
		_ = unlock()
		if err != nil {
			record.ScanErrors = append(record.ScanErrors, name+": "+err.Error())
		}
	}
	record.BackfillFrom = record.MinTimestamp
	if !record.CaptureStart.IsZero() && (record.BackfillFrom == 0 || record.CaptureStart.Unix() < record.BackfillFrom) {
		record.BackfillFrom = record.CaptureStart.Unix()
	}
	record.BackfillTo = max(record.MaxTimestamp, record.RecordedAt.Unix())
	path := filepath.Join(dir, fmt.Sprintf("startup-discard.%020d", time.Now().UnixNano()))
	if err = os.Mkdir(path, 0700); err != nil {
		return false, err
	}
	data, err := json.MarshalIndent(record, "", "  ")
	if err != nil {
		return false, err
	}
	f, err := os.OpenFile(filepath.Join(path, "window.json"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return false, err
	}
	_, writeErr := f.Write(data)
	err = errors.Join(writeErr, f.Sync(), f.Close(), syncDir(path), syncDir(dir))
	if err != nil {
		return false, err
	}
	if _, err = completeStartupDiscard(dir, path, &record); err != nil {
		return false, err
	}
	app.recordStartupDiscard(path, &record)
	return true, nil
}
func (app *App) recordStartupDiscard(path string, record *discardedStartup) {
	app.startupDiscarded.Add(1)
	zapwriter.Logger("app").Info("legacy startup spool moved aside for replica backfill", zap.String("path", path), zap.Int64("backfill_from", record.BackfillFrom), zap.Int64("backfill_to", record.BackfillTo), zap.Int64("points", record.Points), zap.Strings("scan_errors", record.ScanErrors))
}
func completeStartupDiscard(dir, destination string, record *discardedStartup) (bool, error) {
	moved := false
	for _, name := range record.Files {
		if filepath.Base(name) != name || name == "." {
			return false, errors.New("invalid legacy startup spool path")
		}
		from, to := filepath.Join(dir, name), filepath.Join(destination, name)
		if _, err := os.Stat(to); err == nil {
			continue
		} else if !errors.Is(err, os.ErrNotExist) {
			return false, err
		}
		if err := os.Rename(from, to); err != nil {
			return false, err
		}
		moved = true
	}
	return moved, errors.Join(syncDir(destination), syncDir(dir))
}
func scanDiscardedStartup(path string, record *discardedStartup) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	visit := func(p *points.Points) {
		for _, point := range p.Data {
			if record.Points == 0 || point.Timestamp < record.MinTimestamp {
				record.MinTimestamp = point.Timestamp
			}
			if record.Points == 0 || point.Timestamp > record.MaxTimestamp {
				record.MaxTimestamp = point.Timestamp
			}
			record.Points++
		}
	}
	if inputlog.IsSegment(f) {
		info, err := f.Stat()
		if err != nil {
			return err
		}
		replay, err := inputlog.NewSegmentReplay(f)
		if err != nil {
			return err
		}
		defer replay.Close()
		for {
			records, _, _, err := replay.Next(info.Size())
			if errors.Is(err, io.EOF) {
				return nil
			}
			if err != nil {
				return err
			}
			for _, p := range records {
				visit(p)
			}
		}
	}
	_, err = recovery.ReadCaptureFramesPipeline(f, 1<<20, func([]byte, int, <-chan struct{}) (int64, error) { return 0, nil }, func(raw []byte, _ int, _, _ int64) error { return points.ReadBinary(bytes.NewReader(raw), visit) })
	if errors.Is(err, io.EOF) {
		return nil
	}
	return err
}

func legacyStartupPresent(dir string) bool {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return false
	}
	for _, e := range entries {
		if !e.IsDir() && recovery.IsCaptureSegment(e.Name()) {
			return true
		}
	}
	return false
}
