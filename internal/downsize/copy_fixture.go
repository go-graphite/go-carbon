package downsize

import (
	"context"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"time"

	"github.com/go-graphite/go-carbon/internal/chunkstore"
)

func fixture(ctx context.Context, directory string, metrics, count int) error {
	if directory == "" || metrics < 1 || count < 1 || count > 86400 {
		return fmt.Errorf("invalid fixture dimensions")
	}
	if err := os.Mkdir(directory, 0700); err != nil {
		return err
	}
	store, err := chunkstore.Open(filepath.Join(directory, "live"), chunkstore.Options{
		ExactBlocks: true, CacheSize: 16 << 20, MemTableSize: 16 << 20, MaxUnflushedMemTableBytes: 64 << 20,
		CompactionWorkers: 2, SyncInterval: time.Second,
	})
	if err != nil {
		return err
	}
	defer store.Close()
	now := time.Now().Unix()
	started := time.Now()
	retention := chunkstore.Retention{Step: 1, Count: 86400}
	snapshot := chunkstore.Snapshot{
		Metadata: chunkstore.Metadata{MetricConfig: chunkstore.MetricConfig{AggregationMethod: chunkstore.Average, Retentions: []chunkstore.Retention{retention}}},
		Archives: []chunkstore.Archive{{Retention: retention, Points: make([]chunkstore.Point, count)}},
	}
	// Deterministic high-entropy values keep compressed sizes representative of
	// sparse exact stores rather than benchmarking an all-zero compression case.
	seed := uint64(42)
	for i := range metrics {
		if err := ctx.Err(); err != nil {
			return err
		}
		snapshot.Metadata.Name = fmt.Sprintf("synthetic.production.cluster.service.%08d.cpu.usage", i)
		for j := range count {
			seed ^= seed << 13
			seed ^= seed >> 7
			seed ^= seed << 17
			snapshot.Archives[0].Points[j] = chunkstore.Point{Timestamp: now - int64(count-j), Value: math.Float64frombits((seed >> 12) | (1023 << 52))}
		}
		if _, err := store.CreateFromSnapshot(ctx, snapshot); err != nil {
			return err
		}
		if (i+1)%100000 == 0 {
			fmt.Printf("fixture metrics=%d elapsed=%.3f\n", i+1, time.Since(started).Seconds())
		}
	}
	if err := store.Flush(); err != nil {
		return err
	}
	if err := store.Checkpoint(filepath.Join(directory, "checkpoint")); err != nil {
		return err
	}
	var bytes int64
	entries, err := os.ReadDir(filepath.Join(directory, "checkpoint"))
	if err != nil {
		return err
	}
	for _, entry := range entries {
		info, err := entry.Info()
		if err != nil {
			return err
		}
		bytes += info.Size()
	}
	fmt.Printf("fixture metrics=%d bytes=%d bytes_per_metric=%.2f elapsed=%.3f\n", metrics, bytes, float64(bytes)/float64(metrics), time.Since(started).Seconds())
	return nil
}
