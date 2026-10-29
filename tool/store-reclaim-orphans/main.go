// store-reclaim-orphans marks staged imports whose original Whisper file is
// gone as reclaimed, so an incremental store whose conversion removed the
// originals without publishing ownership serves those metrics again. Run it
// with go-carbon stopped; without -apply it only counts.
package main

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

func main() {
	dir := flag.String("store", "", "exact-block store directory")
	source := flag.String("source", "", "original Whisper tree (the daemon's data-dir)")
	apply := flag.Bool("apply", false, "write the reclaimed state; without it only count")
	cache := flag.Int64("cache-bytes", 256<<20, "Pebble block cache budget")
	flag.Parse()
	if *dir == "" || *source == "" {
		fmt.Fprintln(os.Stderr, "usage: store-reclaim-orphans -store DIR -source DATA_DIR [-apply]")
		os.Exit(2)
	}
	if err := run(*dir, *source, *apply, *cache); err != nil {
		fmt.Fprintln(os.Stderr, "store-reclaim-orphans:", err)
		os.Exit(1)
	}
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
