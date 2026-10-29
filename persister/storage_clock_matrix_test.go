package persister

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	whisper "github.com/go-graphite/go-whisper"
)

var storageClockProcesses = make(chan struct{}, 10)

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
	// Join concurrent Run calls without occupying a parallel slot per waiter.
	var cases sync.WaitGroup
	for _, method := range []whisper.AggregationMethod{whisper.Average, whisper.Sum, whisper.First, whisper.Last, whisper.Min, whisper.Max} {
		for _, xff := range []float32{0, 0.5, 1} {
			cases.Add(1)
			go func() {
				defer cases.Done()
				storageClockProcesses <- struct{}{}
				defer func() { <-storageClockProcesses }()
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
