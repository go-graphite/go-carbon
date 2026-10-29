//go:build linux

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/internal/whisperio"
	"golang.org/x/sys/unix"
)

// Opt-in read-only native diagnostic: ordinary test runs never walk a live tree.
func TestNativeSourcePathProofThroughput(t *testing.T) {
	source, captures := os.Getenv("NATIVE_CAPTURE_SOURCE"), os.Getenv("NATIVE_CAPTURE_COPY")
	if source == "" || captures == "" {
		t.Skip("set both private native capture roots explicitly")
	}
	var paths []string
	err := filepath.WalkDir(captures, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.Type()&os.ModeSymlink != 0 {
			return fmt.Errorf("unexpected capture alias")
		}
		if !d.IsDir() && strings.HasSuffix(path, ".wsp") {
			rel, err := filepath.Rel(captures, path)
			if err != nil {
				return err
			}
			paths = append(paths, rel)
			if len(paths) == 100000 {
				return fs.SkipAll
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(paths) != 100000 {
		t.Fatalf("need 100,000 retained captures, got %d", len(paths))
	}
	cpu := func() float64 {
		var r unix.Rusage
		if err := unix.Getrusage(unix.RUSAGE_SELF, &r); err != nil {
			t.Fatal(err)
		}
		return float64(r.Utime.Sec+r.Stime.Sec) + float64(r.Utime.Usec+r.Stime.Usec)/1e6
	}
	for _, mode := range []string{"previous", "kernel-proof", "previous-repeat"} {
		t.Run(mode, func(t *testing.T) {
			start, initial := time.Now(), cpu()
			jobs := make(chan string, 16)
			var group sync.WaitGroup
			var same, unknown, failed atomic.Uint64
			for range 16 {
				group.Go(func() {
					for rel := range jobs {
						var err error
						if mode == "kernel-proof" {
							err = validateSourcePath(source, rel)
						} else {
							var resolved string
							resolved, err = filepath.EvalSymlinks(filepath.Join(source, rel))
							if err == nil && !within(source, resolved) {
								err = fmt.Errorf("source escape")
							}
						}
						if err == nil {
							var ok bool
							ok, err = whisperio.SameCapture(context.Background(), filepath.Join(source, rel), filepath.Join(captures, rel))
							if err == nil {
								if ok {
									same.Add(1)
								} else {
									unknown.Add(1)
								}
							}
						}
						if err != nil {
							failed.Add(1)
						}
					}
				})
			}
			for _, rel := range paths {
				jobs <- rel
			}
			close(jobs)
			group.Wait()
			data, _ := json.Marshal(map[string]any{"mode": mode, "checked": len(paths), "same": same.Load(), "changed_or_unsupported": unknown.Load(), "errors": failed.Load(), "seconds": time.Since(start).Seconds(), "cpu_seconds": cpu() - initial, "workers": 16, "limits": "lexical minutely capture sample; shared host; filesystem proof only"})
			t.Log(string(data))
			if failed.Load() != 0 {
				t.Fatalf("path or capture errors: %d", failed.Load())
			}
		})
	}
}
