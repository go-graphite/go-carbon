package carbonserver

import (
	"flag"
	"os"
	"os/exec"
	"regexp"
	"strings"
	"testing"
	"time"
)

// Tests that change the scan planner's global hooks own a process, so the
// other index tests can run alongside them. Coverage stays in-process.
func isolatedScanTest(t *testing.T) bool {
	t.Helper()
	const key = "GO_CARBONSERVER_ISOLATED_TEST"
	if os.Getenv(key) == t.Name() || testing.CoverMode() != "" {
		return false
	}
	t.Parallel()
	path, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	pattern := "^" + regexp.QuoteMeta(t.Name()) + "$"
	if filter := flag.Lookup("test.run"); filter != nil {
		if parts := strings.SplitN(filter.Value.String(), "/", 2); len(parts) == 2 {
			pattern += "/" + parts[1]
		}
	}
	args := []string{"-test.run=" + pattern, "-test.v"}
	if deadline, ok := t.Deadline(); ok {
		args = append(args, "-test.timeout="+time.Until(deadline).String())
	}
	cmd := exec.Command(path, args...)
	cmd.Env = append(os.Environ(), key+"="+t.Name())
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("isolated scan: %v\n%s", err, output)
	}
	if testing.Verbose() {
		t.Logf("%s", output)
	}
	return true
}
