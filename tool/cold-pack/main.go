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
	rate := flag.Int64("bytes-per-second", 1<<20, "maintenance read plus batch byte budget")
	cpu := flag.Float64("cpu-fraction", .02, "conservative fraction of one worker")
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
	defer func() { err = errors.Join(err, db.Close()) }()
	db.StopBackground()
	if err = db.ConfigureColdPack(chunkstore.ColdPackOptions{Mode: *mode, BytesPerSecond: *rate, CPUFraction: *cpu, WriteWindow: *window, MinFreeBytes: *reserve, MaxCompactionDebt: *debt}); err != nil {
		return err
	}
	if err = db.RunColdPack(ctx); err != nil {
		return err
	}
	if *downgrade {
		if err = db.DowngradeColdPack(ctx); err != nil {
			return err
		}
	}
	return json.NewEncoder(os.Stdout).Encode(db.ColdPackStats())
}
