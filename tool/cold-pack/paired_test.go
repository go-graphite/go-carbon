package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/internal/whisperio"
)

type pairedReply struct {
	Nanos int64
	Done  bool
	Error string
	Stats store.ColdPackStats
}

func pairedOptions(t *testing.T, dir string) store.Options {
	t.Helper()
	marker, err := os.ReadFile(filepath.Join(dir, "INCREMENTAL-FILE-MIGRATION"))
	if err != nil {
		t.Fatal(err)
	}
	clock, err := strconv.ParseInt(os.Getenv("COLD_PACK_BENCH_CLOCK"), 10, 64)
	if err != nil {
		t.Fatal(err)
	}
	return store.Options{ExactBlocks: true, TimeBuckets: true, Continuation: &whisperio.ContinuationCodec{}, IncrementalSource: strings.Split(string(marker), "\n")[1], CacheSize: 64 << 20, MemTableSize: 8 << 20, MaxUnflushedMemTableBytes: 32 << 20, SyncInterval: time.Second, Now: func() time.Time { return time.Unix(clock, 0) }}
}

// Each peer owns its process and GC heap. The idle control therefore sees host
// scheduling/IO changes without inheriting the maintenance process's GC pauses.
func TestColdPackPairedChild(t *testing.T) {
	dir := os.Getenv("COLD_PACK_PAIRED_CHILD")
	if dir == "" {
		t.Skip("private benchmark peer")
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	opts := pairedOptions(t, dir)
	db, err := store.Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	db.StopBackground()
	metrics, err := db.List(ctx, "")
	if err != nil {
		t.Fatal(err)
	}
	input, output := bufio.NewScanner(os.Stdin), json.NewEncoder(os.Stdout)
	var worker chan error
	done := false
	defer func() {
		cancel()
		if worker != nil && !done {
			<-worker
		}
		db.Close()
	}()
	for input.Scan() {
		n, err := strconv.Atoi(input.Text())
		if err != nil {
			t.Fatal(err)
		}
		if n < 0 {
			if worker != nil {
				t.Fatal("duplicate maintenance start")
			}
			if err := db.ConfigureColdPack(store.ColdPackOptions{Mode: os.Getenv("COLD_PACK_BENCH_MODE"), WriteWindow: time.Second, CPUFraction: .02, BytesPerSecond: 1 << 20, MinFreeBytes: 30 << 30}); err != nil {
				t.Fatal(err)
			}
			worker = make(chan error, 1)
			go func() { worker <- db.RunColdPack(ctx) }()
		}
		reply := pairedReply{}
		if n >= 0 {
			n %= 500
			start := time.Now()
			for j := 0; j < 20; j++ {
				m := metrics[(n*7919+j*104729)%len(metrics)]
				if _, err := db.Fetch(ctx, m.Name, int(opts.Now().Unix())-3600, int(opts.Now().Unix())); err != nil {
					reply.Error = err.Error()
					break
				}
			}
			reply.Nanos = int64(time.Since(start))
		}
		if worker != nil && !done {
			select {
			case err := <-worker:
				done = true
				if err != nil {
					reply.Error = err.Error()
				}
			default:
			}
		}
		reply.Done = done
		if done {
			reply.Stats = db.ColdPackStats()
		}
		if err := output.Encode(reply); err != nil {
			t.Fatal(err)
		}
	}
	if err := input.Err(); err != nil {
		t.Fatal(err)
	}
}

type pairedPeer struct {
	cmd *exec.Cmd
	in  io.WriteCloser
	out *json.Decoder
}

func pairedStart(t *testing.T, ctx context.Context, dir, log string) *pairedPeer {
	t.Helper()
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestColdPackPairedChild$", "-test.timeout=275s")
	cmd.Env = append(os.Environ(), "COLD_PACK_PAIRED_CHILD="+dir)
	in, err := cmd.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	out, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	stderr, err := os.Create(log)
	if err != nil {
		t.Fatal(err)
	}
	cmd.Stderr = stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	p := &pairedPeer{cmd, in, json.NewDecoder(bufio.NewReader(out))}
	t.Cleanup(func() {
		in.Close()
		err := cmd.Wait()
		stderr.Close()
		if err != nil && ctx.Err() == nil {
			t.Errorf("paired peer: %v", err)
		}
	})
	return p
}
func (p *pairedPeer) query(n int) (pairedReply, error) {
	if _, err := fmt.Fprintln(p.in, n); err != nil {
		return pairedReply{}, err
	}
	var r pairedReply
	if err := p.out.Decode(&r); err != nil {
		return r, err
	}
	if r.Error != "" {
		return r, errors.New(r.Error)
	}
	return r, nil
}

// Opt-in paired control for a full paced sweep, with identical requests sent in
// alternating order to separate idle/maintenance processes on the same host.
func TestColdPackPairedReads(t *testing.T) {
	dir := os.Getenv("COLD_PACK_PAIRED_DIR")
	if dir == "" {
		t.Skip("private paired corpus benchmark")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 270*time.Second)
	defer cancel()
	db, err := store.Open(dir, pairedOptions(t, dir))
	if err != nil {
		t.Fatal(err)
	}
	db.StopBackground()
	ref := filepath.Join(filepath.Dir(dir), "reference")
	if err := db.Checkpoint(ref); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	idle := pairedStart(t, ctx, ref, filepath.Join(filepath.Dir(dir), "idle.log"))
	active := pairedStart(t, ctx, dir, filepath.Join(filepath.Dir(dir), "active.log"))
	// The same fixed mix warms both readers before starting maintenance.
	for n := 0; n < 500; n++ {
		for _, p := range []*pairedPeer{idle, active} {
			if _, err := p.query(n); err != nil {
				t.Fatal(err)
			}
		}
	}
	if _, err := active.query(-1); err != nil {
		t.Fatal(err)
	}
	var a, b, ar, br []float64
	start := time.Now()
	var last pairedReply
	for n := 0; ; n++ {
		peers := []*pairedPeer{idle, active}
		if n%2 != 0 {
			peers[0], peers[1] = peers[1], peers[0]
		}
		for _, p := range peers {
			at := time.Now()
			r, err := p.query(n)
			roundTrip := float64(time.Since(at)) / float64(time.Millisecond)
			if err != nil {
				t.Fatal(err)
			}
			if p == idle {
				a = append(a, float64(r.Nanos)/float64(time.Millisecond))
				ar = append(ar, roundTrip)
			} else {
				b = append(b, float64(r.Nanos)/float64(time.Millisecond))
				br = append(br, roundTrip)
				last = r
			}
		}
		if last.Done {
			break
		}
		timer := time.NewTimer(5 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			t.Fatal(ctx.Err())
		case <-timer.C:
		}
	}
	result := map[string]any{"idle": quantiles(a), "during": quantiles(b), "idle_roundtrip": quantiles(ar), "during_roundtrip": quantiles(br), "wall_s": time.Since(start).Seconds(), "stats": last.Stats, "method": "independent processes, identical 500-request cycle, alternating order, 5ms think time"}
	data, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(os.Getenv("COLD_PACK_BENCH_RESULT"), append(data, '\n'), 0600); err != nil {
		t.Fatal(err)
	}
}
