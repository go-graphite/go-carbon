// cold-pack runs bounded maintenance on an exclusively opened, offline store.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/internal/whisperio"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "ERROR cold-pack:", err)
		os.Exit(1)
	}
}
func run() (err error) {
	dir := flag.String("store", "", "offline store directory")
	source := flag.String("source", "", "original canonical source binding, if incremental")
	mode := flag.String("mode", "unpack", "exact, decimal or unpack")
	downgrade := flag.Bool("downgrade", false, "after clean unpack, sync and replace v9 fence with v7")
	workers := flag.Int("workers", 1, "parallel maintenance workers (1-64)")
	rate := flag.Int64("bytes-per-second", 1<<20, "maintenance read plus batch byte budget")
	cpu := flag.Float64("cpu-fraction", .02, "worker duty fraction (up to workers * fraction cores)")
	window := flag.Duration("write-window", 24*time.Hour, "keep more recent ranges ordinary")
	reserve := flag.Uint64("min-free-bytes", 1<<30, "minimum free disk reserve")
	debt := flag.Uint64("max-compaction-debt", 64<<20, "pause while estimated compaction debt exceeds this")
	flag.Parse()
	if *dir == "" || (*mode != "exact" && *mode != "decimal" && *mode != "unpack") || (*downgrade && *mode != "unpack") {
		return errors.New("set -store and a valid -mode; -downgrade requires unpack")
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	db, err := chunkstore.Open(*dir, chunkstore.Options{ExactBlocks: true, TimeBuckets: true, Continuation: &whisperio.ContinuationCodec{}, IncrementalSource: *source, CacheSize: 32 << 20, MemTableSize: 8 << 20})
	if err != nil {
		return err
	}
	defer func() {
		err = errors.Join(err, db.Close())
		if err != nil {
			err = errors.Join(err, json.NewEncoder(os.Stderr).Encode(db.ColdPackStats()))
		}
	}()
	db.StopBackground()
	if err = db.ConfigureColdPack(chunkstore.ColdPackOptions{Mode: *mode, Workers: *workers, BytesPerSecond: *rate, CPUFraction: *cpu, WriteWindow: *window, MinFreeBytes: *reserve, MaxCompactionDebt: *debt}); err != nil {
		return err
	}
	if err = runColdPack(ctx, db, *downgrade); err != nil {
		return err
	}
	return json.NewEncoder(os.Stdout).Encode(db.ColdPackStats())
}

// A pass may finish without authorizing downgrade: conflicts, bounded skips,
// or a resumed failed pass require another sweep. Keep the store's safety check
// authoritative and cap the offline retry work.
func runColdPack(ctx context.Context, db *chunkstore.Store, downgrade bool) error {
	passes := 1
	if downgrade {
		passes = 3
	}
	for pass := 1; ; pass++ {
		before := db.ColdPackStats()
		err := db.RunColdPack(ctx)
		if err == nil {
			if !downgrade {
				return nil
			}
			err = db.DowngradeColdPack(ctx)
			if err == nil {
				return nil
			}
			if !errors.Is(err, chunkstore.ErrColdPackIncomplete) {
				return coldPackFailure(err, pass, db.ColdPackStats())
			}
		}
		stats := db.ColdPackStats()
		// Retry completed sweeps only. Interrupted sweeps retain their
		// cursor; cancellation never starts another pass.
		if !downgrade || stats.Passes == before.Passes || pass == passes || ctx.Err() != nil ||
			errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return coldPackFailure(err, pass, stats)
		}
	}
}

func coldPackFailure(err error, passes int, stats chunkstore.ColdPackStats) error {
	return fmt.Errorf("after %d cold-pack pass(es): %w (errors=%d conflicts=%d skipped=%d last_error=%q last_incomplete=%q cursor=%q block_cursor=%d)",
		passes, err, stats.Errors, stats.Conflicts, stats.Skipped, stats.Error, stats.LastIncomplete, stats.Cursor, stats.BlockCursor)
}
