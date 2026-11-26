// store-reclaim-orphans marks staged imports whose original Whisper file is
// gone as reclaimed, so an incremental store whose conversion removed the
// originals without publishing ownership serves those metrics again. With
// -unreferenced-ids it instead deletes the keys of metric IDs below a fence
// that no catalog record, reservation, receipt or staged operation
// references. Run it with go-carbon stopped; without -apply it only counts
// and opens the store read-only.
package reclaim

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/internal/whisperio"
)

func Main(args []string) error {
	flags := flag.NewFlagSet("storectl maintain reclaim-orphans", flag.ContinueOnError)
	dir := flags.String("store", "", "exact-block store directory")
	source := flags.String("source", "", "original Whisper tree (the daemon's data-dir)")
	apply := flags.Bool("apply", false, "write the reclaimed state; without it only count")
	cache := flags.Int64("cache-bytes", 256<<20, "Pebble block cache budget")
	unreferenced := flags.Bool("unreferenced-ids", false, "delete keys of metric IDs nothing references instead of reclaiming staged imports")
	var opt store.UnreferencedOptions
	flags.Uint64Var(&opt.Below, "below-id", 0, "with -unreferenced-ids: sweep IDs below this fence (required)")
	flags.Uint64Var(&opt.From, "from-id", 1, "with -unreferenced-ids: first ID swept")
	flags.Uint64Var(&opt.Margin, "margin-ids", store.DefaultUnreferencedMargin, "with -unreferenced-ids: refuse a fence above the allocator watermark minus this many IDs")
	flags.Uint64Var(&opt.MinRunIDs, "min-run-ids", 1, "with -unreferenced-ids: leave runs of fewer unreferenced IDs alone")
	flags.BoolVar(&opt.CountKeys, "count-keys", false, "with -unreferenced-ids: count keys in every range (scans them)")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return errors.New("unexpected positional arguments")
	}
	if *unreferenced {
		if *dir == "" || opt.Below == 0 {
			return errors.New("set -store DIR and -below-id ID; -apply deletes the unreferenced ranges")
		}
		opt.Apply = *apply
		return runUnreferenced(*dir, *source, opt, *cache)
	}
	if *dir == "" || *source == "" {
		return errors.New("set -store DIR and -source DATA_DIR; -apply publishes reclaimed state")
	}
	return run(*dir, *source, *apply, *cache)
}

func run(dir, source string, apply bool, cache int64) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	start := time.Now()
	db, err := store.Open(dir, store.Options{
		Continuation: &whisperio.ContinuationCodec{}, ExactBlocks: true, IncrementalSource: source,
		CacheSize: cache, SyncInterval: time.Second,
	})
	if err != nil {
		return fmt.Errorf("open store: %w", err)
	}
	opened := time.Since(start)
	result, reclaimErr := db.ReclaimOrphanedFiles(ctx, func(name string) (bool, error) {
		return originalExists(source, name)
	}, apply)
	closeErr := db.Close()
	report, _ := json.Marshal(struct {
		store.OrphanReclaim
		Apply   bool
		Open    string
		Elapsed string
	}{result, apply, opened.Round(time.Millisecond).String(), time.Since(start).Round(time.Millisecond).String()})
	fmt.Println(string(report))
	return errors.Join(reclaimErr, closeErr)
}

func runUnreferenced(dir, source string, opt store.UnreferencedOptions, cache int64) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	start := time.Now()
	db, err := store.Open(dir, store.Options{
		Continuation: &whisperio.ContinuationCodec{}, ExactBlocks: true, IncrementalSource: source,
		CacheSize: cache, SyncInterval: time.Second, ReadOnly: !opt.Apply,
	})
	if err != nil {
		return fmt.Errorf("open store: %w", err)
	}
	opened := time.Since(start)
	result, sweepErr := db.ReclaimUnreferencedIDs(ctx, opt)
	closeErr := db.Close()
	report, _ := json.Marshal(struct {
		store.UnreferencedReclaim
		Apply   bool
		Open    string
		Elapsed string
	}{result, opt.Apply, opened.Round(time.Millisecond).String(), time.Since(start).Round(time.Millisecond).String()})
	fmt.Println(string(report))
	return errors.Join(sweepErr, closeErr)
}

func originalExists(source, name string) (bool, error) {
	path := filepath.Join(source, filepath.FromSlash(strings.ReplaceAll(name, ".", "/")+".wsp"))
	if _, err := os.Lstat(path); err == nil {
		return true, nil
	} else if errors.Is(err, os.ErrNotExist) {
		return false, nil
	} else {
		return false, err
	}
}
