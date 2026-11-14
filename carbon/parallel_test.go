package carbon

import (
	"flag"
	"os"
	"os/exec"
	"regexp"
	"strings"
	"testing"
	"time"
)

// Storage integration processes are memory intensive. Bound their fanout even
// when the test binary inherits a large GOMAXPROCS or -test.parallel setting.
var isolatedTestSlots = make(chan struct{}, 4)

const isolatedTestCase = "GRAPHITE_TEST_ISOLATED_CASE"

// These integration cases capture process-global logs and hooks. Isolate each
// case so they can run together, using the same binary and race instrumentation.
// Coverage and selective subtest runs stay in process, retaining their profiles
// and Go's complete subtest filter semantics.
func isolatedTestProcess(t *testing.T) bool {
	return isolatedTestProcessMode(t, true)
}

// Wall-clock handoff tests model a bounded sender buffer. Run those processes
// serially so competing storage startups do not consume the simulated outage
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
		isolatedTestSlots <- struct{}{}
		defer func() { <-isolatedTestSlots }()
	}
	parts := strings.Split(t.Name(), "/")
	for i := range parts {
		parts[i] = "^" + regexp.QuoteMeta(parts[i]) + "$"
	}
	args := []string{"-test.run=" + strings.Join(parts, "/"), "-test.v"}
	if deadline, ok := t.Deadline(); ok {
		args = append(args, "-test.timeout="+time.Until(deadline).String())
	}
	for _, name := range []string{"test.parallel", "test.short", "test.skip"} {
		args = append(args, "-"+name+"="+flag.Lookup(name).Value.String())
	}
	// The package deadline bounds the process. Individual asynchronous waits
	// observe their own progress; output volume is not a reliable heartbeat.
	cmd := exec.CommandContext(t.Context(), os.Args[0], args...)
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
