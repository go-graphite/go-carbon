package v2core

import (
	"os"
	"os/exec"
	"regexp"
	"testing"
)

// coreTestProcess isolates tests that need process-wide environment settings.
// The parent can run in parallel; the child retains the complete test body and
// its normal environment cleanup, without changing other engines' settings.
func coreTestProcess(t *testing.T) bool {
	t.Helper()
	const child = "GO_CARBON_CORE_TEST_CHILD"
	if os.Getenv(child) == t.Name() {
		return false
	}
	t.Parallel()
	cmd := exec.CommandContext(t.Context(), os.Args[0], "-test.run=^"+regexp.QuoteMeta(t.Name())+"$", "-test.count=1", "-test.timeout=10m", "-test.v")
	cmd.Env = append(os.Environ(), child+"="+t.Name())
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("isolated core test: %v\n%s", err, output)
	}
	t.Logf("%s", output)
	return true
}
