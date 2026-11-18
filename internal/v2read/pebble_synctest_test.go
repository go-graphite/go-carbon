package v2read

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"regexp"
	"strings"
	"testing"
	"testing/synctest"
	"time"
)

// pebbleSynctest runs f in a synctest bubble in its own test process. Pebble
// keeps sstable write tasks in a process-wide sync.Pool, and each task carries a
// compressionDone channel owned by the bubble that allocated it. Once another
// bubble, or a test outside any bubble, reuses that task, the runtime aborts the
// whole binary with "send on synctest channel from outside bubble". A child
// process per bubble keeps those channels away from every other test. Keep all
// Pebble work inside f; see chunkstore's pebbleSynctest for the same harness.
func pebbleSynctest(t *testing.T, f func(*testing.T)) {
	t.Helper()
	// The named subtest gives the child an exact -test.run path.
	t.Run("bubble", func(t *testing.T) {
		t.Helper()
		const childEnv = "GO_CARBON_PEBBLE_SYNCTEST_CHILD"
		if os.Getenv(childEnv) == t.Name() {
			synctest.Test(t, f)
			return
		}
		parts := strings.Split(t.Name(), "/")
		for i := range parts {
			parts[i] = "^" + regexp.QuoteMeta(parts[i]) + "$"
		}
		args := []string{
			"-test.run=" + strings.Join(parts, "/"),
			"-test.count=1",
			fmt.Sprintf("-test.short=%t", testing.Short()),
			fmt.Sprintf("-test.v=%t", testing.Verbose()),
		}
		// Real-time limits only catch a hung child; assertions inside the
		// bubble keep the virtual clock. The child times out first so its
		// goroutine dump reaches this test's failure output.
		ctx := t.Context()
		if deadline, ok := t.Deadline(); ok {
			var cancel context.CancelFunc
			ctx, cancel = context.WithDeadline(ctx, deadline)
			defer cancel()
			args = append(args, "-test.timeout="+(time.Until(deadline)*9/10).String())
		}
		cmd := exec.CommandContext(ctx, os.Args[0], args...)
		cmd.Env = append(os.Environ(), childEnv+"="+t.Name())
		output, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("Pebble bubble subprocess: %v\n%s", err, output)
		}
		t.Logf("%s", output)
	})
}
