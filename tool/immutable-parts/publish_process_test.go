package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	pp "github.com/go-graphite/go-carbon/internal/partspublish"
	pr "github.com/go-graphite/go-carbon/internal/partsread"
)

// The parent fixture is append-only and retains every published part until the
// child has joined. This is a benchmark transport, not a production deletion or
// crash-recovery protocol. Envelopes become visible only after core commit.
type isolationProcessResult struct {
	Queries             uint64
	Errors              uint64
	Generations         uint64
	Latest              [16]uint64
	GOMAXPROCS          int
	QuerySeconds        float64
	RefreshSeconds      float64
	MaxRefreshSeconds   float64
	FinalRefreshSeconds float64
}

type isolationProcess struct {
	cmd    *exec.Cmd
	input  io.WriteCloser
	log    *os.File
	root   string
	once   sync.Once
	result isolationProcessResult
	err    error
}

func startIsolationProcess(root string, readers int) (*isolationProcess, error) {
	exe, err := os.Executable()
	if err != nil {
		return nil, err
	}
	args := []string{"-test.run=^TestPublisherReaderProcess$", "-test.v", "-test.timeout=120s"}
	command := exe
	if runtime.GOOS == "linux" {
		command = "taskset"
		args = append([]string{"-c", os.Getenv("PARTS_READ_BENCH_CPUS"), exe}, args...)
		if node := os.Getenv("PARTS_PUBLISH_READER_NODE"); node != "" {
			n, parseErr := strconv.Atoi(node)
			if parseErr != nil || n < 0 {
				return nil, errors.New("invalid reader NUMA node")
			}
			command = "numactl"
			args = append([]string{"--membind=" + node, "taskset"}, args...)
		}
	}
	p := &isolationProcess{root: root}
	p.cmd = exec.Command(command, args...)
	for _, v := range os.Environ() {
		if !strings.HasPrefix(v, "GOMAXPROCS=") {
			p.cmd.Env = append(p.cmd.Env, v)
		}
	}
	procs := os.Getenv("PARTS_PUBLISH_READER_PROCS")
	if procs == "" {
		procs = "64"
	}
	p.cmd.Env = append(p.cmd.Env, "GOMAXPROCS="+procs, "PARTS_PUBLISH_CHILD="+root, "PARTS_PUBLISH_CHILD_READERS="+strconv.Itoa(readers))
	p.log, err = os.Create(filepath.Join(root, "reader-process.log"))
	if err != nil {
		return nil, err
	}
	p.cmd.Stdout, p.cmd.Stderr = p.log, p.log
	p.input, err = p.cmd.StdinPipe()
	if err != nil {
		p.log.Close()
		return nil, err
	}
	if err = p.cmd.Start(); err != nil {
		p.input.Close()
		p.log.Close()
		return nil, err
	}
	for deadline := time.Now().Add(30 * time.Second); time.Now().Before(deadline); {
		if _, err = os.Stat(filepath.Join(root, "reader-ready")); err == nil {
			return p, nil
		}
		time.Sleep(10 * time.Millisecond)
	}
	_ = p.cmd.Process.Kill()
	_, _ = p.stop()
	return nil, errors.New("reader process startup timed out")
}

func (p *isolationProcess) stop() (isolationProcessResult, error) {
	p.once.Do(func() {
		_ = p.input.Close()
		p.err = p.cmd.Wait()
		p.err = errors.Join(p.err, p.log.Close())
		b, err := os.ReadFile(filepath.Join(p.root, "reader-result.json"))
		if err == nil {
			err = json.Unmarshal(b, &p.result)
		}
		p.err = errors.Join(p.err, err)
	})
	return p.result, p.err
}

func TestPublisherReaderProcess(t *testing.T) {
	root := os.Getenv("PARTS_PUBLISH_CHILD")
	if root == "" {
		t.Skip("child of opt-in publication benchmark")
	}
	readers, err := strconv.Atoi(os.Getenv("PARTS_PUBLISH_CHILD_READERS"))
	if err != nil || readers < 0 || readers > 64 {
		t.Fatal("invalid child readers")
	}
	owners, metrics, err := newIsolationOwners(root, 16)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		for _, o := range owners {
			o.pub.Close()
		}
	}()
	var last [16][]byte
	var generations uint64
	var latest [16]uint64
	var refreshSeconds, maxRefreshSeconds float64
	refresh := func() error {
		start := time.Now()
		defer func() {
			elapsed := time.Since(start).Seconds()
			refreshSeconds += elapsed
			maxRefreshSeconds = max(maxRefreshSeconds, elapsed)
		}()
		for i, o := range owners {
			b, err := os.ReadFile(filepath.Join(root, fmt.Sprintf("snapshot-%02d.json", i)))
			if err != nil {
				return err
			}
			if bytes.Equal(b, last[i]) {
				continue
			}
			var e pp.Envelope
			if err = json.Unmarshal(b, &e); err != nil {
				return err
			}
			plan, err := o.pub.PlanSeal(pp.Request{Envelope: e})
			if err != nil {
				return err
			}
			plan.Commit()
			last[i] = b
			generations++
			latest[i] = e.Generation
		}
		return nil
	}
	if err = refresh(); err != nil {
		t.Fatal(err)
	}
	stop := make(chan struct{})
	go func() { _, _ = io.Copy(io.Discard, os.Stdin); close(stop) }()
	var wg sync.WaitGroup
	var queries, failures atomic.Uint64
	queryStart := time.Now()
	budget := pr.NewBudget(64 << 30)
	for n := 0; n < readers; n++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			for i := n; ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				owner := i % 16
				m := metrics[owner*64+(i/16)%64]
				h, err := owners[owner].pub.Pin(budget, 8<<20)
				if err == nil {
					var s *pr.Series
					s, err = h.Render(ctx, m.Name, 99, 161, 200)
					if err == nil {
						if s == nil || len(s.Values) == 0 || math.Float64bits(s.Values[0]) != math.Float64bits(100) {
							err = errors.New("seed changed")
						} else {
							for j, v := range s.Values {
								if !math.IsNaN(v) && math.Float64bits(v) != math.Float64bits(float64(s.From+int64(j)*s.Step)) {
									err = errors.New("value bits changed")
									break
								}
							}
						}
					}
					h.Close()
				}
				if err != nil {
					failures.Add(1)
				}
				queries.Add(1)
			}
		}(n)
	}
	if err = os.WriteFile(filepath.Join(root, "reader-ready"), []byte("ready"), 0600); err != nil {
		t.Fatal(err)
	}
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	var refreshErr error
loop:
	for {
		select {
		case <-stop:
			break loop
		case <-ticker.C:
			if refreshErr == nil {
				refreshErr = refresh()
			}
		}
	}
	wg.Wait()
	querySeconds := time.Since(queryStart).Seconds()
	finalRefreshStart := time.Now()
	refreshErr = errors.Join(refreshErr, refresh())
	finalRefreshSeconds := time.Since(finalRefreshStart).Seconds()
	result := isolationProcessResult{Queries: queries.Load(), Errors: failures.Load(), Generations: generations, Latest: latest, GOMAXPROCS: runtime.GOMAXPROCS(0), QuerySeconds: querySeconds, RefreshSeconds: refreshSeconds, MaxRefreshSeconds: maxRefreshSeconds, FinalRefreshSeconds: finalRefreshSeconds}
	b, err := json.Marshal(result)
	if err == nil {
		err = os.WriteFile(filepath.Join(root, "reader-result.json"), b, 0600)
	}
	if err = errors.Join(err, refreshErr); err != nil {
		t.Fatal(err)
	}
	if failures.Load() != 0 || (readers > 0 && queries.Load() == 0) {
		t.Fatal("reader queries failed or absent")
	}
}
