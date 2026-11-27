package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"
	"sort"
	"syscall"
	"time"

	"github.com/go-graphite/go-carbon/internal/storeclone"
)

const cloneUsage = `usage: storectl clone send|recv|verify [flags]
  send    stage a running store's flushed state with hard links and stream it
  recv    accept the streams into an empty directory and check every file
  verify  open a received clone, check every live table and its checksums`

func cloneMain(args []string) error {
	if len(args) == 0 {
		return errors.New(cloneUsage)
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	switch args[0] {
	case "send":
		return cloneSend(ctx, args[1:])
	case "recv":
		return cloneRecv(ctx, args[1:])
	case "verify":
		return cloneVerify(args[1:])
	}
	return errors.New(cloneUsage)
}

func cloneSend(ctx context.Context, args []string) (err error) {
	fs := flag.NewFlagSet("clone send", flag.ContinueOnError)
	source := fs.String("source", "", "Pebble directory of the running store (read only)")
	staging := fs.String("staging", "", "new staging directory on the source filesystem (removed at exit)")
	raw := fs.String("raw", "", "send the regular files of this directory as they are, without staging (network probe)")
	to := fs.String("to", "", "receiver host:port")
	streams := fs.Int("streams", 16, "parallel TCP streams")
	metrics := fs.String("cache-metrics", "", "source daemon Prometheus URL; pause while its cache is high or dropping")
	pauseAt := fs.Float64("pause-at", 0.75, "pause at this cache fill")
	resumeAt := fs.Float64("resume-at", 0.60, "resume below this cache fill")
	startFree := fs.Float64("start-free", 0.15, "minimum free share of the staging filesystem to start")
	abortFree := fs.Float64("abort-free", 0.12, "abort and remove the links below this free share")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *to == "" || (*source == "") == (*raw == "") || *source != "" && *staging == "" || *streams < 1 {
		return errors.New("clone send needs -to and either -source with -staging or -raw")
	}
	started := time.Now()
	var files []storeclone.File
	dir := *raw
	if *source != "" {
		if free := freeShare(*source); free < *startFree {
			return fmt.Errorf("source filesystem free %.1f%% below %.0f%%", free*100, *startFree*100)
		}
		dir = *staging
		defer func() { err = errors.Join(err, os.RemoveAll(*staging)) }()
		if files, err = storeclone.Stage(*source, *staging); err != nil {
			return err
		}
	} else if files, err = rawFiles(*raw); err != nil {
		return err
	}
	var total int64
	for _, f := range files {
		total += f.Size
	}
	fmt.Fprintf(os.Stderr, "%s staged %d files %d bytes in %s\n", time.Now().Format(time.RFC3339), len(files), total, time.Since(started).Round(time.Second))
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var gate storeclone.Gate
	var cacheGate *storeclone.CacheGate
	if *metrics != "" {
		cacheGate = &storeclone.CacheGate{URL: *metrics, PauseAt: *pauseAt, ResumeAt: *resumeAt, Every: 5 * time.Second, StagingDir: dir, MinFree: *abortFree, Log: os.Stderr}
		if *source == "" {
			cacheGate.StagingDir = ""
		}
		go cacheGate.Run(ctx)
		gate = cacheGate
	}
	var progress storeclone.Progress
	go report(ctx, "sent", &progress, total, func() string {
		if cacheGate == nil {
			return ""
		}
		ratio, paused := cacheGate.State()
		return fmt.Sprintf(" cache=%.1f%% paused=%v", ratio*100, paused)
	})
	sendStarted := time.Now()
	if err := storeclone.Send(ctx, dir, files, *to, *streams, gate, &progress); err != nil {
		return err
	}
	secs := time.Since(sendStarted).Seconds()
	fmt.Fprintf(os.Stderr, "%s done files=%d bytes=%d send_seconds=%.0f MBps=%.1f wall=%s\n", time.Now().Format(time.RFC3339),
		len(files), total, secs, float64(total)/secs/1e6, time.Since(started).Round(time.Second))
	return nil
}

func cloneRecv(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("clone recv", flag.ContinueOnError)
	listen := fs.String("listen", "", "listen address, e.g. :18431")
	dir := fs.String("dir", "", "target directory (created; existing files are never overwritten)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *listen == "" || *dir == "" {
		return errors.New("clone recv needs -listen and -dir")
	}
	ln, err := net.Listen("tcp", *listen)
	if err != nil {
		return err
	}
	defer ln.Close()
	started := time.Now()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var progress storeclone.Progress
	go report(ctx, "received", &progress, 0, func() string { return "" })
	list, err := storeclone.Receive(ctx, ln, *dir, &progress)
	if err != nil {
		return err
	}
	fmt.Fprintf(os.Stderr, "%s done files=%d bytes=%d seconds=%.0f\n", time.Now().Format(time.RFC3339), len(list), progress.Bytes.Load(), time.Since(started).Seconds())
	return nil
}

func cloneVerify(args []string) error {
	fs := flag.NewFlagSet("clone verify", flag.ContinueOnError)
	dir := fs.String("dir", "", "received clone directory")
	workers := fs.Int("workers", 32, "parallel checksum readers")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *dir == "" {
		return errors.New("clone verify needs -dir")
	}
	report, err := storeclone.Verify(*dir, *workers)
	_ = storeclone.WriteJSON(os.Stdout, report)
	return err
}

func rawFiles(dir string) ([]storeclone.File, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var files []storeclone.File
	for _, e := range entries {
		if info, err := e.Info(); err == nil && info.Mode().IsRegular() {
			files = append(files, storeclone.File{Name: e.Name(), Size: info.Size()})
		}
	}
	sort.Slice(files, func(i, j int) bool { return files[i].Size > files[j].Size })
	return files, nil
}

func freeShare(dir string) float64 {
	var st syscall.Statfs_t
	if syscall.Statfs(dir, &st) != nil || st.Blocks == 0 {
		return 0
	}
	return float64(st.Bavail) / float64(st.Blocks)
}

func report(ctx context.Context, verb string, p *storeclone.Progress, total int64, extra func() string) {
	t := time.NewTicker(10 * time.Second)
	defer t.Stop()
	last, lastAt := int64(0), time.Now()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-t.C:
			b := p.Bytes.Load()
			fmt.Fprintf(os.Stderr, "%s %s files=%d bytes=%d/%d MBps=%.1f%s\n", now.Format(time.RFC3339), verb, p.Files.Load(), b, total,
				float64(b-last)/now.Sub(lastAt).Seconds()/1e6, extra())
			last, lastAt = b, now
		}
	}
}
