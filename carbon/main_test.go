package carbon

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"testing"

	"github.com/go-graphite/go-carbon/internal/handoff"
)

const testHandoffSuccessorDir = "GRAPHITE_TEST_HANDOFF_SUCCESSOR_DIR"

// Use persistent immutable stubs so macOS need not assess new executables
// during a handoff. Isolated cases inherit the warmed paths and protocols.
func TestMain(m *testing.M) {
	// Integration fixtures write only a few MiB. Production's percentage-based
	// reserve can exceed all remaining space on a shared development machine,
	// intentionally blocking capture forever. Give fixtures stable headroom,
	// including crash subprocesses, while retaining real statfs errors. Disk
	// pressure tests supply explicit samples through updateDiskSpace.
	diskUsage := startupDiskUsage
	startupDiskUsage = func(path string) (uint64, uint64, error) {
		if os.Getenv("STARTUP_LOAD_GATE") == "1" {
			return diskUsage(path)
		}
		_, _, err := diskUsage(path)
		return 1 << 30, 2 << 30, err
	}

	cpus := min(runtime.GOMAXPROCS(0), 4)
	if os.Getenv(isolatedTestCase) != "" {
		// Four storage processes share the host with other builds. Keep their
		// GC and worker schedulers from each claiming every available CPU.
		cpus = min(cpus, 2)
	}
	runtime.GOMAXPROCS(cpus)
	_ = os.Setenv("GOMAXPROCS", strconv.Itoa(cpus))
	for key, value := range map[string]string{
		"GRAPHITE_TEST_READ_PROTOCOL":        handoff.Protocol,
		"GRAPHITE_TEST_INPUT_PROTOCOL":       handoff.InputProtocol,
		"GRAPHITE_TEST_SHARED_PROTOCOL":      handoff.SharedStoreProtocol,
		"GRAPHITE_TEST_STORE_INPUT_PROTOCOL": handoff.StoreInputProtocol,
	} {
		_ = os.Setenv(key, value)
	}
	if os.Getenv(testHandoffSuccessorDir) == "" {
		dir := filepath.Join("testdata", "handoff-probes")
		if _, err := os.Stat(dir); err != nil {
			// Standalone test binaries may be invoked from the repository root.
			dir = filepath.Join("carbon", dir)
		}
		dir, err := filepath.Abs(dir)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		for _, capabilities := range []string{"read,shared", "read,shared,input", "read,input", "read,input,store-input"} {
			for _, flag := range []string{"-handoff-protocol", "-input-handoff-protocol", "-shared-store-handoff-protocol", "-store-input-handoff-protocol"} {
				// Warm the rejection paths too: macOS executable assessment
				// must not consume a timed rollback handoff's input window.
				if err := exec.Command(filepath.Join(dir, capabilities), flag).Run(); err != nil {
					var exit *exec.ExitError
					if !errors.As(err, &exit) || exit.ExitCode() != 2 {
						fmt.Fprintln(os.Stderr, err)
						os.Exit(1)
					}
				}
			}
		}
		_ = os.Setenv(testHandoffSuccessorDir, dir)
	}
	var seedDir string
	if os.Getenv(incrementalSeedEnv) == "" {
		var err error
		seedDir, err = os.MkdirTemp("", "carbon-test-seed-")
		if err == nil {
			seed := filepath.Join(seedDir, "seed.wsp")
			err = buildIncrementalSeed(seed)
			if err == nil {
				err = os.Setenv(incrementalSeedEnv, seed)
			}
		}
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			_ = os.RemoveAll(seedDir)
			os.Exit(1)
		}
	}
	code := m.Run()
	if seedDir != "" {
		_ = os.RemoveAll(seedDir)
	}
	os.Exit(code)
}
