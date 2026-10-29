// store-convert imports complete Whisper archives into a shared exact-block
// store. The catalog commit is the per-metric checkpoint, so interrupted runs
// resume without replaying every historical point through UpdateMany.
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/internal/whisperio"
)

type entry struct{ Metric, Path string }
type identity = chunkstore.CaptureIdentity
type counters struct{ Imported, Resumed, Points, Errors, Vanished atomic.Uint64 }
type converter struct {
	db                        *chunkstore.Store
	source, copyRoot, scratch string
	reflink, verify           bool
	live                      bool
	reconcile                 bool
	proof                     *captureProof
	checkpoint                *baselineCheckpoint
	skipped                   atomic.Uint64
	visited                   *visitedMetrics
	deletionChecksSkipped     atomic.Uint64
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	source := flag.String("source", "", "source Whisper tree")
	destination := flag.String("destination", "", "dedicated exact-block store directory")
	sourceID := flag.String("source-id", "", "immutable dataset identity; must match on resume")
	copyRoot := flag.String("copy", "", "dedicated tree retaining coherent copies of the original files")
	live := flag.Bool("source-live", false, "capture a live baseline; leaves NEEDS-CATCHUP and cannot be selected for serving")
	quiesced := flag.Bool("source-quiesced", false, "assert the entire source tree is immutable for this conversion and any resume")
	reconcile := flag.Bool("reconcile", false, "replace and verify every metric from a quiesced canonical source tree, reconcile deletions and retire serving guards")
	refresh := flag.Bool("refresh-live", false, "refresh complete live baseline while retaining serving guards; never makes the store ready")
	fast := flag.Bool("fast-catchup", false, "skip unchanged verified XFS reflink captures using durable destination generation proofs")
	adopt := flag.Bool("trust-verified-baseline", false, "explicitly assert an older completed live baseline was fully verified and its captures remain immutable; audit original command/log first")
	manifest := flag.String("manifest", "", "optional JSONL {Metric,Path} inventory; Path is relative to source")
	workers := flag.Int("workers", 2, "bounded number of metrics being decoded and encoded concurrently")
	cpu := flag.Int("cpu", 2, "GOMAXPROCS for conversion")
	controlFile := flag.String("control-file", "", "expiring JSON resource lease; budgets change without restarting")
	maxWorkers := flag.Int("max-workers", 128, "maximum live worker budget and worker goroutines")
	maxCPU := flag.Int("max-cpu", 128, "maximum live GOMAXPROCS budget")
	memoryLimit := flag.Int64("memory-limit-bytes", 8<<30, "Go memory limit when live resource control is enabled")
	checkpoints := flag.Bool("verified-checkpoints", true, "retain per-metric exact-verification receipts for full baseline resume")
	verify := flag.Bool("verify", true, "compare every imported/resumed archive timestamp and Float64 bit pattern")
	reflink := flag.Bool("require-reflink", true, "fail rather than read-copy files while holding source locks")
	compact := flag.Bool("compact", true, "compact the completed store before closing")
	cache := flag.Int64("cache-bytes", 64<<20, "Pebble block cache budget")
	memtable := flag.Uint64("memtable-bytes", 32<<20, "Pebble memtable budget")
	flag.Parse()
	if *source == "" || *destination == "" || *sourceID == "" || *workers < 1 || *workers > *maxWorkers || *maxWorkers < 1 || *maxWorkers > 256 || *cpu < 1 || *cpu > *maxCPU || *maxCPU < 1 || *live && *copyRoot == "" || *live == *quiesced {
		return errors.New("source, destination, source-id and positive worker/CPU budgets are required; select source-live with -copy, or source-quiesced")
	}
	if *reconcile && (!*quiesced || !*verify || *manifest != "") {
		return errors.New("reconcile requires source-quiesced, verification, and a complete canonical tree walk (no manifest)")
	}
	if *refresh && (!*live || !*verify || *copyRoot == "" || *manifest != "" || *reconcile) {
		return errors.New("refresh-live requires source-live, retained copies, verification, full canonical inventory, and no reconcile")
	}
	if *fast && (!(*reconcile || *refresh) || !*verify || *copyRoot == "") || *adopt && !*fast {
		return errors.New("fast-catchup requires reconcile or refresh-live, verification and retained copies; adoption requires fast-catchup")
	}
	var err error
	*source, err = physicalPath(*source)
	if err != nil {
		return err
	}
	*destination, err = physicalPath(*destination)
	if err != nil {
		return err
	}
	if *copyRoot != "" {
		*copyRoot, err = physicalPath(*copyRoot)
		if err != nil {
			return err
		}
	}
	for _, pair := range [][2]string{{*source, *destination}, {*source, *copyRoot}, {*destination, *copyRoot}} {
		if pair[1] != "" && (within(pair[0], pair[1]) || within(pair[1], pair[0])) {
			return errors.New("source, destination and copy trees must be disjoint")
		}
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	heapLimit := int64(0)
	poolSize := *workers
	if *controlFile != "" {
		if *memoryLimit < 64<<20 || *memoryLimit > 128<<30 {
			return errors.New("live conversion memory limit must be between 64 MiB and 128 GiB")
		}
		heapLimit = *memoryLimit
		poolSize = *maxWorkers
	}
	control := newResourceControl(*sourceID, *cpu, *workers, heapLimit, *maxCPU, *maxWorkers)
	if *controlFile != "" {
		// Missing or stale leases must not start with an unobserved high budget.
		control.apply(control.fallback)
		go control.watch(ctx, *controlFile)
	}
	db, err := chunkstore.Open(*destination, chunkstore.Options{ExactBlocks: true, CacheSize: *cache, MemTableSize: *memtable, SyncInterval: time.Second})
	if err != nil {
		return err
	}
	defer func() {
		if db != nil {
			_ = db.Close()
		}
	}()
	id := identity{Source: *source, Copy: *copyRoot, SourceID: *sourceID, Destination: *destination, Live: *live}
	if *reconcile || *refresh {
		id, err = reconciliationIdentity(*destination, id)
		if err != nil {
			return err
		}
	}
	if err := checkIdentity(*destination, id, db); err != nil {
		return err
	}
	var proof *captureProof
	if *adopt {
		if err := adoptVerifiedBaseline(*destination, id); err != nil {
			return err
		}
	}
	if *fast {
		proof, err = openCaptureProof(*destination, id)
		if err != nil {
			return err
		}
		defer proof.db.Close()
	} else if *reconcile || *refresh {
		// A non-proof-aware replacement must not leave an old baseline receipt
		// usable with capture files it may have refreshed before a crash.
		if err := os.Remove(filepath.Join(*destination, receiptName)); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
		if err := syncDirectory(*destination); err != nil {
			return err
		}
	}
	if err := writeCheckpointFile(*destination, "CONVERSION-IN-PROGRESS", []byte(*sourceID+"\n"), os.O_TRUNC); err != nil {
		return err
	}
	if *live {
		if err := writeCheckpointFile(*destination, "NEEDS-CATCHUP", []byte("Live baseline only; concurrent writes must be reconciled and validated before serving.\n"), os.O_TRUNC); err != nil {
			return err
		}
	}
	if *copyRoot != "" {
		if err := checkCopyIdentity(*copyRoot, id); err != nil {
			return err
		}
	}
	scratch := filepath.Join(*destination, ".conversion-tmp")
	if err := os.MkdirAll(scratch, 0700); err != nil {
		return err
	}
	// Pebble's exclusive database lock and the matching conversion identity
	// ensure these interrupted private snapshots belong to this converter.
	orphans, err := os.ReadDir(scratch)
	if err != nil {
		return err
	}
	for _, orphan := range orphans {
		if orphan.IsDir() && strings.HasPrefix(orphan.Name(), "go-whisper-ooo-snapshot-") {
			if err := os.RemoveAll(filepath.Join(scratch, orphan.Name())); err != nil {
				return err
			}
		}
	}
	var checkpoint *baselineCheckpoint
	if *checkpoints && *verify && !*reconcile && !*refresh && *copyRoot != "" && *manifest == "" {
		checkpoint, err = openBaselineCheckpoint(*destination, id)
		if err != nil {
			return err
		}
		defer func() {
			if checkpoint != nil {
				_ = checkpoint.close()
			}
		}()
	}
	conv := converter{db: db, source: *source, copyRoot: *copyRoot, scratch: scratch, reflink: *reflink, verify: *verify, live: *live, reconcile: *reconcile || *refresh, proof: proof, checkpoint: checkpoint}
	if *reconcile {
		// Only a fenced source makes a successful visit authoritative for the
		// later deletion pass. A live refresh still checks current existence.
		conv.visited = &visitedMetrics{}
	}
	var stats counters
	started := time.Now()
	report := func(phase string) {
		budget := control.snapshot()
		var skipped uint64
		if conv.checkpoint != nil {
			skipped = conv.checkpoint.skipped.Load()
		}
		data, _ := json.Marshal(map[string]any{"phase": phase, "seconds": time.Since(started).Seconds(), "imported": stats.Imported.Load(), "resumed": stats.Resumed.Load(), "points": stats.Points.Load(), "errors": stats.Errors.Load(), "vanished": stats.Vanished.Load(), "workers": budget.Workers, "cpu": budget.CPU, "active_workers": control.gate.activeCount(), "memory_limit_bytes": budget.MemoryLimitBytes, "control_generation": budget.Generation, "control_expires_at": budget.ExpiresAt, "completed_metrics": stats.Imported.Load() + stats.Resumed.Load(), "checkpoint_skips": skipped, "source_live": *live, "reconcile": *reconcile, "refresh_live": *refresh, "skipped_unchanged": conv.skipped.Load(), "deletion_checks_skipped": conv.deletionChecksSkipped.Load(), "ready_for_cutover": *reconcile && phase == "reconciled"})
		fmt.Fprintln(os.Stdout, string(data))
	}
	jobs := make(chan entry, poolSize)
	var group sync.WaitGroup
	for range poolSize {
		group.Go(func() {
			for job := range jobs {
				if ctx.Err() != nil {
					return
				}
				if err := control.gate.acquire(ctx); err != nil {
					return
				}
				func() {
					defer control.gate.release()
					points, resumed, err := conv.convert(ctx, job)
					if err != nil {
						vanished, dropErr := conv.discardVanished(ctx, job, err)
						if vanished {
							stats.Vanished.Add(1)
							data, _ := json.Marshal(map[string]any{"metric": job.Metric, "path": job.Path, "status": "source-vanished"})
							fmt.Fprintln(os.Stdout, string(data))
							return
						}
						err = errors.Join(err, dropErr)
						stats.Errors.Add(1)
						data, _ := json.Marshal(map[string]any{"metric": job.Metric, "path": job.Path, "error": err.Error()})
						fmt.Fprintln(os.Stderr, string(data))
						return
					}
					if resumed {
						stats.Resumed.Add(1)
					} else {
						stats.Imported.Add(1)
					}
					stats.Points.Add(uint64(points))
				}()
			}
		})
	}
	done := make(chan struct{})
	reporterDone := make(chan struct{})
	go func() {
		defer close(reporterDone)
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				if checkpoint != nil {
					if err := checkpoint.sync(); err != nil {
						stats.Errors.Add(1)
						fmt.Fprintf(os.Stderr, "baseline-checkpoint sync failed: %v\n", err)
					}
				}
				report("converting")
			case <-done:
				return
			}
		}
	}()
	enqueue := func(e entry) error {
		select {
		case jobs <- e:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if *manifest != "" {
		err = readManifest(*manifest, enqueue)
	} else {
		err = walk(*source, enqueue)
	}
	close(jobs)
	group.Wait()
	close(done)
	<-reporterDone
	if err != nil {
		report("incomplete")
		return err
	}
	if err := ctx.Err(); err != nil {
		report("interrupted")
		return err
	}
	if stats.Errors.Load() != 0 {
		report("incomplete")
		return errors.New("conversion has failed metrics; source and successful checkpoints are retained")
	}
	if *reconcile || *refresh {
		if stats.Imported.Load()+stats.Resumed.Load() == 0 {
			return errors.New("refusing to finalize an empty source tree; retain serving guards and check the source mount and inventory")
		}
		deleted, err := conv.reconcileDeletions(ctx)
		if err != nil {
			return err
		}
		stats.Vanished.Add(deleted)
	}
	if *compact {
		report("compacting")
		if err := db.Compact(); err != nil {
			return err
		}
	}
	baselineLimit, err := db.CatalogHighWatermark(ctx)
	if err != nil {
		return err
	}
	if err := db.Close(); err != nil {
		db = nil
		return err
	}
	db = nil
	if checkpoint != nil {
		err := checkpoint.close()
		checkpoint = nil
		if err != nil {
			return err
		}
	}
	if !*reconcile && !*refresh && *verify && *copyRoot != "" {
		if err := publishCaptureReceipt(*destination, id, "converter: all captured archive timestamps and Float64 bits verified", baselineLimit); err != nil {
			return err
		}
	}
	if *reconcile {
		for _, guard := range []string{"NEEDS-CATCHUP", "MIRROR-RECONCILE-REQUIRED"} {
			if err := os.Remove(filepath.Join(*destination, guard)); err != nil && !errors.Is(err, os.ErrNotExist) {
				return err
			}
		}
	}
	if err := os.Remove(filepath.Join(*destination, "CONVERSION-IN-PROGRESS")); err != nil {
		return err
	}
	if err := syncDirectory(*destination); err != nil {
		return err
	}
	switch {
	case *reconcile:
		report("reconciled")
	case *refresh:
		report("live-refresh-complete")
	default:
		report("baseline-complete")
	}
	return nil
}

func within(root, path string) bool {
	rel, err := filepath.Rel(root, path)
	return err == nil && (rel == "." || filepath.IsLocal(rel))
}

// Resolve existing ancestors too: a new destination beneath a symlink must not
// evade the disjoint-tree check and overwrite source files.
func physicalPath(path string) (string, error) {
	path, err := filepath.Abs(path)
	if err != nil {
		return "", err
	}
	ancestor := path
	for {
		_, err := os.Lstat(ancestor)
		if err == nil {
			break
		}
		if !errors.Is(err, os.ErrNotExist) {
			return "", err
		}
		parent := filepath.Dir(ancestor)
		if parent == ancestor {
			return "", err
		}
		ancestor = parent
	}
	resolved, err := filepath.EvalSymlinks(ancestor)
	if err != nil {
		return "", err
	}
	rel, err := filepath.Rel(ancestor, path)
	if err != nil {
		return "", err
	}
	return filepath.Join(resolved, rel), nil
}

func checkIdentity(dir string, id identity, db *chunkstore.Store) error {
	path := filepath.Join(dir, "CONVERSION.json")
	data, err := os.ReadFile(path)
	if err == nil {
		var previous identity
		if err := json.Unmarshal(data, &previous); err != nil {
			return err
		}
		if previous != id {
			return errors.New("conversion identity differs from the existing checkpoint")
		}
		return nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	page, err := db.ListPage(context.Background(), "", "", 1)
	if err != nil {
		return err
	}
	if len(page) != 0 {
		return errors.New("refusing a destination with data but no conversion identity")
	}
	data, err = json.Marshal(id)
	if err != nil {
		return err
	}
	return writeCheckpointFile(dir, "CONVERSION.json", data, os.O_EXCL)
}

func checkCopyIdentity(dir string, id identity) error {
	if err := os.MkdirAll(dir, 0750); err != nil {
		return err
	}
	path := filepath.Join(dir, "CAPTURE.json")
	data, err := os.ReadFile(path)
	if err == nil {
		var previous identity
		if err := json.Unmarshal(data, &previous); err != nil {
			return err
		}
		if previous != id {
			return errors.New("copy tree belongs to a different conversion")
		}
		return nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	if len(entries) != 0 {
		return errors.New("refusing a nonempty copy tree without a matching capture identity")
	}
	data, err = json.Marshal(id)
	if err != nil {
		return err
	}
	return writeCheckpointFile(dir, "CAPTURE.json", data, os.O_EXCL)
}

// Persist serving guards before importing any batches. Syncing only Pebble's
// WAL cannot make a newly created marker or identity survive a power failure.
func writeCheckpointFile(dir, name string, data []byte, flags int) error {
	f, err := os.OpenFile(filepath.Join(dir, name), os.O_WRONLY|os.O_CREATE|flags, 0600)
	if err != nil {
		return err
	}
	_, err = f.Write(data)
	if err := errors.Join(err, f.Sync(), f.Close()); err != nil {
		return err
	}
	return syncDirectory(dir)
}

func syncDirectory(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	return errors.Join(d.Sync(), d.Close())
}

func (c *converter) convert(ctx context.Context, e entry) (int, bool, error) {
	db, source, copyRoot, reflink, verify := c.db, c.source, c.copyRoot, c.reflink, c.verify
	if c.checkpoint != nil && (!verify || copyRoot == "" || c.reconcile) {
		return 0, false, errors.New("baseline verification checkpoints require verified retained baseline captures")
	}
	if !filepath.IsLocal(e.Path) || e.Metric == "" {
		return 0, false, errors.New("invalid metric or relative source path")
	}
	if c.reconcile && !canonicalEntry(e) {
		return 0, false, errors.New("final reconciliation requires canonical untagged metric paths")
	}
	metadata, err := db.Metadata(ctx, e.Metric)
	resumed := err == nil
	if err != nil && !errors.Is(err, chunkstore.ErrNotFound) {
		return 0, false, err
	}
	if resumed && !verify && !c.reconcile {
		return 0, true, nil
	}
	path := filepath.Join(source, e.Path)
	if err := validateSourcePath(source, e.Path); err != nil {
		return 0, resumed, err
	}
	if c.proof != nil && resumed {
		matches, err := c.proof.matches(e.Metric, metadata)
		if err != nil {
			return 0, true, err
		}
		if matches {
			same, err := whisperio.SameCapture(ctx, path, filepath.Join(copyRoot, e.Path))
			if err != nil {
				return 0, true, err
			}
			if same {
				c.skipped.Add(1)
				c.visited.Mark(metadata.ID)
				return 0, true, nil
			}
		}
	}
	if c.proof != nil {
		if err := c.proof.pending(ctx, e.Metric); err != nil {
			return 0, resumed, err
		}
	}
	if copyRoot != "" {
		path = filepath.Join(copyRoot, e.Path)
		if !resumed || c.reconcile {
			if err := whisperio.Capture(ctx, filepath.Join(source, e.Path), path, reflink); err != nil {
				return 0, false, err
			}
		}
	}
	var stamp [32]byte
	if c.checkpoint != nil {
		stamp, err = baselineCaptureStamp(path)
		if err != nil {
			return 0, resumed, err
		}
		if resumed {
			count, matched, err := c.checkpoint.match(e.Metric, metadata, stamp)
			if err != nil || matched {
				return count, true, err
			}
		}
	}
	snapshot, err := whisperio.ReadSnapshotIn(ctx, e.Metric, path, c.scratch)
	if err != nil {
		return 0, resumed, err
	}
	if c.reconcile {
		metadata, err = db.Replace(ctx, snapshot)
		if err != nil {
			return 0, resumed, err
		}
	} else if !resumed {
		metadata, err = db.CreateFromSnapshot(ctx, snapshot)
		if err != nil {
			return 0, false, err
		}
	}
	count := 0
	for _, a := range snapshot.Archives {
		count += len(a.Points)
	}
	if verify {
		actual, err := db.Snapshot(ctx, e.Metric)
		if err != nil {
			return 0, resumed, err
		}
		if err := equalSnapshots(snapshot, actual); err != nil {
			return 0, resumed, err
		}
	}
	if c.proof != nil {
		if err := c.proof.verified(e.Metric, metadata); err != nil {
			return 0, resumed, err
		}
	}
	if c.checkpoint != nil {
		after, err := baselineCaptureStamp(path)
		if err != nil {
			return 0, resumed, err
		}
		if after != stamp {
			return 0, resumed, errors.New("retained baseline capture changed during verification")
		}
		if err := c.checkpoint.verified(e.Metric, metadata, stamp, count); err != nil {
			return 0, resumed, err
		}
	}
	c.visited.Mark(metadata.ID)
	return count, resumed, nil
}

// Live inventories can race source expiration. Only a confirmed missing source
// is discarded; a missing private copy or a permission/format error still fails.
// The live destination remains unservable until deletion/write catch-up.
func (c *converter) discardVanished(ctx context.Context, e entry, cause error) (bool, error) {
	if !c.live || !errors.Is(cause, os.ErrNotExist) || !filepath.IsLocal(e.Path) {
		return false, nil
	}
	if _, err := os.Stat(filepath.Join(c.source, e.Path)); !errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err := c.db.Delete(ctx, e.Metric); err != nil && !errors.Is(err, chunkstore.ErrNotFound) {
		return false, err
	}
	return true, nil
}

func equalSnapshots(want, got chunkstore.Snapshot) error {
	return chunkstore.EqualSnapshots(want, got)
}

func readManifest(path string, fn func(entry) error) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 4096), 1<<20)
	for scanner.Scan() {
		var e entry
		if err := json.Unmarshal(scanner.Bytes(), &e); err != nil {
			return err
		}
		if err := fn(e); err != nil {
			return err
		}
	}
	return scanner.Err()
}

func walk(root string, fn func(entry) error) error {
	return whisperio.WalkMetrics(context.Background(), root, false, func(e whisperio.MetricPath) error {
		return fn(entry{Metric: e.Metric, Path: e.Path})
	})
}
