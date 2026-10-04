// Package whisperio adapts chunkstore snapshots to classic Whisper files.
package whisperio

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"

	"github.com/go-graphite/go-carbon/internal/chunkstore"
	whisper "github.com/go-graphite/go-whisper"
)

type Adapter struct{ Store *chunkstore.Store }

func New(store *chunkstore.Store) *Adapter { return &Adapter{Store: store} }

func (a *Adapter) ExportWSP(ctx context.Context, name, path string) error {
	snapshot, err := a.Store.Snapshot(ctx, name)
	if err != nil {
		return err
	}
	return a.ExportSnapshot(ctx, snapshot, path)
}

func (a *Adapter) ExportSnapshot(ctx context.Context, snapshot chunkstore.Snapshot, path string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(snapshot.Archives) != len(snapshot.Metadata.Retentions) {
		return errors.New("snapshot archive count does not match retentions")
	}
	retentions := make([]whisper.Retention, len(snapshot.Metadata.Retentions))
	for i, r := range snapshot.Metadata.Retentions {
		retentions[i] = whisper.NewRetention(r.Step, r.Count)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return fmt.Errorf("create export directory: %w", err)
	}
	w, err := whisper.Create(path, whisper.NewRetentionsNoPointer(retentions), whisper.AggregationMethod(snapshot.Metadata.AggregationMethod), snapshot.Metadata.XFilesFactor)
	if err != nil {
		return fmt.Errorf("create export: %w", err)
	}
	for i, archive := range snapshot.Archives {
		points := make([]whisper.TimeSeriesPoint, 0, len(archive.Points))
		for _, p := range archive.Points {
			// A point the Whisper field cannot hold is skipped, not fatal: one
			// stray timestamp must not block the transfer of the whole metric.
			if timestamp, ok := exportTimestamp(p.Timestamp); ok {
				points = append(points, whisper.TimeSeriesPoint{Time: timestamp, Value: p.Value})
			}
		}
		if err := w.ReplaceArchivePoints(i, points); err != nil {
			_ = w.Close()
			return fmt.Errorf("write archive %d: %w", i, err)
		}
	}
	return w.Close()
}

// exportTimestamp reports whether a timestamp fits Whisper's uint32 field.
// Zero is Whisper's empty-slot sentinel, not a representable point.
func exportTimestamp(timestamp int64) (int, bool) {
	if timestamp <= 0 || timestamp > math.MaxUint32 || timestamp > int64(^uint(0)>>1) {
		return 0, false
	}
	return int(timestamp), true
}

func (a *Adapter) ImportWSP(ctx context.Context, name, path string, replace bool) (chunkstore.Metadata, error) {
	snapshot, err := readSnapshot(ctx, name, path)
	if err != nil {
		return chunkstore.Metadata{}, err
	}
	if replace {
		return a.Store.Replace(ctx, snapshot)
	}
	return a.Store.CreateFromSnapshot(ctx, snapshot)
}

func (a *Adapter) FillWSP(ctx context.Context, name, path string) (chunkstore.Metadata, error) {
	snapshot, err := readSnapshot(ctx, name, path)
	if err != nil {
		return chunkstore.Metadata{}, err
	}
	return a.Store.Fill(ctx, snapshot)
}

func readSnapshot(ctx context.Context, name, path string) (chunkstore.Snapshot, error) {
	if err := ctx.Err(); err != nil {
		return chunkstore.Snapshot{}, err
	}
	snapshotPath, cleanup, err := whisper.OfflineMergeOutOfOrderSnapshot(path)
	if err != nil {
		return chunkstore.Snapshot{}, err
	}
	defer cleanup()
	readOnly := os.O_RDONLY
	w, err := whisper.OpenWithOptions(snapshotPath, &whisper.Options{OpenFileFlag: &readOnly})
	if err != nil {
		return chunkstore.Snapshot{}, fmt.Errorf("open import: %w", err)
	}
	defer w.Close()
	retentions := w.Retentions()
	config := chunkstore.MetricConfig{Name: name, Retentions: make([]chunkstore.Retention, len(retentions)), AggregationMethod: chunkstore.AggregationMethod(w.AggregationMethod()), XFilesFactor: w.XFilesFactor()}
	for i, r := range retentions {
		config.Retentions[i] = chunkstore.Retention{Step: r.SecondsPerPoint(), Count: r.NumberOfPoints()}
	}
	if !w.IsCompressed() {
		info, err := os.Stat(snapshotPath)
		if err != nil {
			return chunkstore.Snapshot{}, fmt.Errorf("stat import: %w", err)
		}
		if int64(w.Size()) > info.Size() {
			return chunkstore.Snapshot{}, errors.New("import archives exceed file size")
		}
	}
	snapshot := chunkstore.Snapshot{Metadata: chunkstore.Metadata{MetricConfig: config}, Archives: make([]chunkstore.Archive, len(retentions))}
	for i, r := range config.Retentions {
		points, err := w.ArchivePoints(i)
		if err != nil {
			return chunkstore.Snapshot{}, fmt.Errorf("snapshot archive %d: %w", i, err)
		}
		archive := chunkstore.Archive{Retention: r, Points: make([]chunkstore.Point, len(points))}
		for j, p := range points {
			archive.Points[j] = chunkstore.Point{Timestamp: int64(p.Time), Value: p.Value}
		}
		snapshot.Archives[i] = archive
	}
	return snapshot, nil
}
