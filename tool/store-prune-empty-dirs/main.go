// store-prune-empty-dirs removes empty original-tree directories and stale locks
// after conversion. The daemon remains online and owns the store lock.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"

	"golang.org/x/time/rate"
)

type options struct {
	root, admin, metrics, stopFile, devices, exclude    string
	dry, auto                                           bool
	dirsPerSecond, maxRate, maxUtil, maxAwait, maxWrite float64
	workers                                             int
	interval                                            time.Duration
}

func main() {
	var o options
	flag.StringVar(&o.root, "root", "/var/lib/carbon/whisper", "Whisper data root (retained)")
	flag.StringVar(&o.admin, "admin-url", "http://127.0.0.1:8080/admin/info?scopes=storage", "local storage status URL")
	flag.StringVar(&o.metrics, "metrics-url", "http://127.0.0.1:7007/metrics", "local Prometheus endpoint for auto mode")
	flag.StringVar(&o.stopFile, "stop-file", "/var/lib/carbon/stop-prune-empty-dirs", "stop when this file exists; keep it outside the tree")
	flag.StringVar(&o.devices, "devices", "", "comma-separated /proc/diskstats device names; required in auto mode")
	flag.StringVar(&o.exclude, "exclude", "", "comma-separated paths relative to root to retain without walking")
	flag.BoolVar(&o.dry, "dry-run", false, "only count directories and file suffixes")
	flag.BoolVar(&o.auto, "auto", false, "raise rate after healthy disk and write-latency samples")
	flag.IntVar(&o.workers, "workers", 32, "parallel depth-3 subtree workers")
	flag.Float64Var(&o.dirsPerSecond, "rate", 0, "optional shared directory operations/s budget; zero is unlimited")
	flag.Float64Var(&o.dirsPerSecond, "dirs-per-second", 0, "directory operation budget/s (reads and rmdirs share it)")
	flag.Float64Var(&o.maxRate, "max-dirs-per-second", 2000, "auto mode maximum operation budget/s")
	flag.Float64Var(&o.maxUtil, "max-util", 60, "auto mode disk utilization ceiling, percent")
	flag.Float64Var(&o.maxAwait, "max-await-ms", 5, "auto mode disk await ceiling, milliseconds")
	flag.Float64Var(&o.maxWrite, "max-write-ms", 20, "auto mode mean successful Pebble write latency ceiling, milliseconds")
	flag.DurationVar(&o.interval, "progress-interval", 5*time.Second, "progress and health sample interval")
	flag.Parse()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, o, os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "store-prune-empty-dirs:", err)
		os.Exit(1)
	}
}

var errStopFile = errors.New("stop file exists")

func run(ctx context.Context, o options, out io.Writer) error {
	if math.IsNaN(o.dirsPerSecond) || o.dirsPerSecond < 0 || o.dirsPerSecond > 1e9 || o.interval < 100*time.Millisecond {
		return errors.New("rate must be finite in [0,1e9] and progress interval >= 100ms")
	}
	if o.workers == 0 {
		o.workers = 32
	}
	if o.workers < 1 || o.workers > 1024 {
		return errors.New("workers must be between 1 and 1024")
	}
	if o.auto && o.dirsPerSecond == 0 {
		return errors.New("auto mode requires an explicit positive rate")
	}
	if o.auto {
		for _, value := range []float64{o.maxRate, o.maxUtil, o.maxAwait, o.maxWrite} {
			if math.IsNaN(value) || math.IsInf(value, 0) {
				return errors.New("auto ceilings must be finite")
			}
		}
	}
	if o.auto && (o.maxRate < o.dirsPerSecond || o.maxRate > 1e9 || o.devices == "" || o.maxUtil <= 0 || o.maxAwait <= 0 || o.maxWrite <= 0) {
		return errors.New("auto mode requires devices, positive health ceilings and max rate >= initial rate")
	}
	var err error
	o.root, err = filepath.Abs(o.root)
	if err != nil {
		return err
	}
	if o.root == "/" {
		return errors.New("root cannot be /")
	}
	for _, endpoint := range []string{o.admin, o.metrics} {
		if err := localURL(endpoint); err != nil {
			return err
		}
	}
	client := &http.Client{Timeout: 3 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return errors.New("redirect refused") }}
	if err := sharedStatus(ctx, client, o.admin); err != nil {
		return err
	}
	exclude := make(map[string]bool)
	for _, path := range strings.Split(o.exclude, ",") {
		if path == "" {
			continue
		}
		path = filepath.Clean(path)
		if filepath.IsAbs(path) || path == "." || path == ".." || strings.HasPrefix(path, "../") {
			return fmt.Errorf("invalid exclusion: %s", path)
		}
		exclude[path] = true
	}
	var limiter *rate.Limiter
	if o.dirsPerSecond > 0 {
		limiter = rate.NewLimiter(rate.Limit(o.dirsPerSecond), max(1, min(256, int(o.dirsPerSecond/10))))
	}
	w := newParallelWalk(o.workers, o.dry, exclude)
	health := controller{options: o, client: client, rate: o.dirsPerSecond}
	if o.auto {
		if err := health.baseline(ctx); err != nil {
			return err
		}
	}
	started, lastReport := time.Now(), time.Time{}
	lastDirs := uint64(0)
	report := func(final bool) {
		now := time.Now()
		elapsed := now.Sub(started).Seconds()
		c := w.snapshot()
		recent := float64(c.Dirs-lastDirs) / now.Sub(lastReport).Seconds()
		if lastReport.IsZero() {
			recent = float64(c.Dirs) / elapsed
		}
		budget := "unlimited"
		if limiter != nil {
			budget = strconv.FormatFloat(health.rate, 'f', 0, 64)
		}
		fmt.Fprintf(out, "workers=%d elapsed=%.1fs passes=%d dirs=%d empty_dirs=%d removable_dirs=%d removed=%d occupied_or_missing=%d files=%d symlinks=%d other=%d excluded=%d locks_eligible=%d locks_removed=%d dirs/s=%.0f budget/s=%s final=%t %s\n", o.workers, elapsed, c.Passes, c.Dirs, c.EmptyDirs, c.RemovableDirs, c.Removed, c.Occupied, c.Files, c.Symlinks, c.Other, c.Excluded, c.LocksEligible, c.LocksRemoved, recent, budget, final, health.status)
		lastReport, lastDirs = now, c.Dirs
	}
	defer func() {
		c := w.final()
		report(true)
		var suffixes []string
		for suffix := range c.Suffixes {
			suffixes = append(suffixes, suffix)
		}
		sort.Strings(suffixes)
		for _, suffix := range suffixes {
			fmt.Fprintf(out, "suffix=%q files=%d\n", suffix, c.Suffixes[suffix])
		}
	}()

	checkStop := func() error {
		if o.stopFile == "" {
			return nil
		}
		if _, err := os.Lstat(o.stopFile); err == nil {
			return errStopFile
		} else if !errors.Is(err, os.ErrNotExist) {
			return err
		}
		return nil
	}
	if err := checkStop(); err != nil {
		if errors.Is(err, errStopFile) {
			return nil
		}
		return err
	}
	workCtx, cancel := context.WithCancelCause(ctx)
	done := make(chan struct{})
	report(false)
	go func() {
		defer close(done)
		progress := time.NewTicker(o.interval)
		defer progress.Stop()
		stop := time.NewTicker(200 * time.Millisecond)
		defer stop.Stop()
		for {
			select {
			case <-workCtx.Done():
				return
			case <-stop.C:
				if err := checkStop(); err != nil {
					cancel(err)
					return
				}
			case <-progress.C:
				if err := sharedStatus(workCtx, client, o.admin); err != nil {
					cancel(err)
					return
				}
				if o.auto {
					if err := health.sample(workCtx); err != nil {
						cancel(err)
						return
					}
					limiter.SetLimit(rate.Limit(health.rate))
				}
				report(false)
			}
		}
	}()
	var before func() error
	if limiter != nil {
		before = func() error { return limiter.Wait(workCtx) }
	}
	err = w.walk(workCtx, o.root, before)
	if workCtx.Err() != nil {
		err = context.Cause(workCtx)
	}
	cancel(nil)
	<-done
	if errors.Is(err, errStopFile) || errors.Is(err, context.Canceled) {
		fmt.Fprintf(out, "stopped=%q; rerun the same command to resume\n", err)
		return nil
	}
	return err
}
