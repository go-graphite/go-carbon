package persister

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	whisper "github.com/go-graphite/go-whisper"
)

var storageClockProcesses = sync.OnceValue(func() chan struct{} {
	parallel, _ := strconv.Atoi(flag.Lookup("test.parallel").Value.String())
	return make(chan struct{}, max(1, min(10, parallel)))
})

// The real persister uses whisper.Now, so independent clock-driven cases need
// separate processes to run concurrently without changing production clocks.
func storageClockMatrix(t *testing.T) bool {
	t.Helper()
	const child = "GO_CARBON_STORAGE_CLOCK_MATRIX"
	if os.Getenv(child) == t.Name() {
		return false
	}
	t.Parallel()
	parent := t.Name()
	processes := storageClockProcesses()
	// Join concurrent Run calls without occupying a parallel slot per waiter.
	var cases sync.WaitGroup
	for _, method := range []whisper.AggregationMethod{whisper.Average, whisper.Sum, whisper.First, whisper.Last, whisper.Min, whisper.Max} {
		for _, xff := range []float32{0, 0.5, 1} {
			cases.Add(1)
			go func() {
				defer cases.Done()
				processes <- struct{}{}
				defer func() { <-processes }()
				t.Run(fmt.Sprintf("%s/xff%g", method, xff), func(t *testing.T) {
					parts := strings.Split(t.Name(), "/")
					for i := range parts {
						parts[i] = "^" + regexp.QuoteMeta(parts[i]) + "$"
					}
					ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
					defer cancel()
					cmd := exec.CommandContext(ctx, os.Args[0], "-test.run="+strings.Join(parts, "/"), "-test.count=1", "-test.v")
					cmd.Env = append(os.Environ(), child+"="+parent)
					output, err := cmd.CombinedOutput()
					if err != nil {
						t.Fatalf("clock case: %v\n%s", err, output)
					}
					if testing.Verbose() {
						t.Logf("%s", output)
					}
				})
			}()
		}
	}
	cases.Wait()
	return true
}

// Independent storage fixtures can overlap only across process boundaries:
// several install whisper.Now. Reuse the clock matrix pool to bound memory and
// filesystem pressure across both kinds of tests. Coverage and subtest filters
// retain the ordinary in-process execution path.
func isolatedStorageTest(t *testing.T) bool {
	t.Helper()
	const child = "GO_CARBON_STORAGE_TEST"
	if os.Getenv(child) == t.Name() || testing.CoverMode() != "" || strings.Contains(flag.Lookup("test.run").Value.String(), "/") {
		return false
	}
	t.Parallel()
	processes := storageClockProcesses()
	processes <- struct{}{}
	defer func() { <-processes }()
	args := []string{"-test.run=^" + regexp.QuoteMeta(t.Name()) + "$", "-test.count=1", "-test.v"}
	if deadline, ok := t.Deadline(); ok {
		args = append(args, "-test.timeout="+time.Until(deadline).String())
	}
	for _, name := range []string{"test.parallel", "test.short", "test.skip"} {
		args = append(args, "-"+name+"="+flag.Lookup(name).Value.String())
	}
	cmd := exec.CommandContext(t.Context(), os.Args[0], args...)
	cmd.Env = append(os.Environ(), child+"="+t.Name())
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("isolated storage test: %v\n%s", err, output)
	}
	if testing.Verbose() {
		t.Logf("%s", output)
	}
	return true
}
