package carbon

import (
	"flag"
	"os"
	"os/exec"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"
)

// Storage integration processes are memory intensive. Bound their fanout even
// when the test binary inherits a large GOMAXPROCS or -test.parallel setting.
var isolatedTestSlots = make(chan struct{}, 6)
var isolatedTestStorage sync.RWMutex

const isolatedTestCase = "GRAPHITE_TEST_ISOLATED_CASE"

// These integration cases capture process-global logs and hooks. Isolate each
// case so they can run together, using the same binary and race instrumentation.
// Coverage and selective subtest runs stay in process, retaining their profiles
// and Go's complete subtest filter semantics.
func isolatedTestProcess(t *testing.T) bool {
	return isolatedTestProcessMode(t, true)
}

// A matrix may overlap only when its cases will run in separate processes.
// Selective subtest and coverage runs retain serial access to process globals.
func parallelTestMatrix(t *testing.T) {
	t.Helper()
	if os.Getenv(isolatedTestCase) == "" && testing.CoverMode() == "" &&
		!strings.Contains(flag.Lookup("test.run").Value.String(), "/") {
		t.Parallel()
	}
}

// Wall-clock handoff tests model a bounded sender buffer. Run those processes
// exclusively so competing storage startups do not consume the simulated outage
// budget before the handoff itself can run.
func isolatedTestProcessSerial(t *testing.T) bool {
	return isolatedTestProcessMode(t, false)
}

func isolatedTestProcessMode(t *testing.T, parallel bool) bool {
	t.Helper()
	name := os.Getenv(isolatedTestCase)
	if name == t.Name() || strings.HasPrefix(name, t.Name()+"/") || (name != "" && strings.HasPrefix(t.Name(), name+"/")) || testing.CoverMode() != "" ||
		strings.Contains(flag.Lookup("test.run").Value.String(), "/") {
		return false
	}
	if parallel {
		t.Parallel()
	}
	// Serial matrices overlap the other tests, but still count against the
	// storage limit. Acquire only after Parallel releases the parent runner.
	if parallel {
		isolatedTestStorage.RLock()
		defer isolatedTestStorage.RUnlock()
	} else {
		isolatedTestStorage.Lock()
		defer isolatedTestStorage.Unlock()
	}
	isolatedTestSlots <- struct{}{}
	defer func() { <-isolatedTestSlots }()
	parts := strings.Split(t.Name(), "/")
	for i := range parts {
		parts[i] = "^" + regexp.QuoteMeta(parts[i]) + "$"
	}
	args := []string{"-test.run=" + strings.Join(parts, "/"), "-test.v"}
	for _, name := range []string{"test.parallel", "test.short", "test.skip"} {
		args = append(args, "-"+name+"="+flag.Lookup(name).Value.String())
	}
	// The package deadline bounds the process. Individual asynchronous waits
	// observe their own progress; output volume is not a reliable heartbeat.
	cmd := testCommand(t, args...)
	cmd.Env = append(os.Environ(), isolatedTestCase+"="+t.Name())
	// Stream phase and failure output while the child is alive. Buffering it
	// until Wait returns loses the child's diagnostics if the package expires.
	cmd.Stdout = testProcessOutput{t}
	cmd.Stderr = cmd.Stdout
	if err := cmd.Run(); err != nil {
		t.Fatalf("isolated case: %v", err)
	}
	return true
}

type testProcessOutput struct{ t *testing.T }

func (out testProcessOutput) Write(p []byte) (int, error) {
	out.t.Logf("%s", p)
	return len(p), nil
}

// testCommand gives nested crash children the same cancellation and deadlock
// bound as their test. A separate wall-clock timer can expire during useful
// disk work, and the default child timeout could outlive its parent.
func testCommand(t *testing.T, args ...string) *exec.Cmd {
	t.Helper()
	if deadline, ok := t.Deadline(); ok {
		args = append(args, "-test.timeout="+time.Until(deadline).String())
	}
	return exec.CommandContext(t.Context(), os.Args[0], args...)
}
