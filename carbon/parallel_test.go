package carbon

import (
	"context"
	"flag"
	"os"
	"os/exec"
	"regexp"
	"strings"
	"testing"
	"time"
)

const isolatedTestCase = "GRAPHITE_TEST_ISOLATED_CASE"

// These integration cases capture process-global logs and hooks. Isolate each
// case so they can run together, using the same binary and race instrumentation.
// Coverage and selective subtest runs stay in process, retaining their profiles
// and Go's complete subtest filter semantics.
func isolatedTestProcess(t *testing.T) bool {
	t.Helper()
	name := os.Getenv(isolatedTestCase)
	if name == t.Name() || strings.HasPrefix(name, t.Name()+"/") || testing.CoverMode() != "" ||
		strings.Contains(flag.Lookup("test.run").Value.String(), "/") {
		return false
	}
	t.Parallel()
	parts := strings.Split(t.Name(), "/")
	for i := range parts {
		parts[i] = "^" + regexp.QuoteMeta(parts[i]) + "$"
	}
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()
	args := []string{"-test.run=" + strings.Join(parts, "/"), "-test.v"}
	for _, name := range []string{"test.parallel", "test.short", "test.skip"} {
		args = append(args, "-"+name+"="+flag.Lookup(name).Value.String())
	}
	cmd := exec.CommandContext(ctx, os.Args[0], args...)
	cmd.Env = append(os.Environ(), isolatedTestCase+"="+t.Name())
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("isolated case: %v\n%s", err, out)
	}
	if testing.Verbose() {
		t.Logf("%s", out)
	}
	return true
}
