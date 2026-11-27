package carbon

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"testing"

	"github.com/go-graphite/go-carbon/internal/handoff"
)

const testHandoffSuccessorDir = "GRAPHITE_TEST_HANDOFF_SUCCESSOR_DIR"

// Capability probes execute the already running test binary. This avoids shell
// startup/assessment consuming a handoff deadline on a busy host.
func TestMain(m *testing.M) {
	if protocol, ok := map[string]string{
		"-handoff-protocol":              handoff.Protocol,
		"-input-handoff-protocol":        handoff.InputProtocol,
		"-shared-store-handoff-protocol": handoff.SharedStoreProtocol,
		"-store-input-handoff-protocol":  handoff.StoreInputProtocol,
	}[firstTestArgument()]; ok {
		capability := map[string]string{
			"-handoff-protocol": "read", "-input-handoff-protocol": "input",
			"-shared-store-handoff-protocol": "shared", "-store-input-handoff-protocol": "store-input",
		}[os.Args[1]]
		if !strings.Contains(","+filepath.Base(os.Args[0])+",", ","+capability+",") {
			syscall.Exit(2)
		}
		fmt.Println(protocol)
		// This one-shot helper starts no test goroutines. Bypass the race runtime's
		// exit grace period: the real daemon's capability response has no such delay.
		syscall.Exit(0)
	}

	// Integration fixtures write only a few MiB. Production's percentage-based
	// reserve can exceed all remaining space on a shared development machine,
	// intentionally blocking capture forever. Give fixtures stable headroom,
	// including crash subprocesses, while retaining real statfs errors. Disk
	// pressure tests supply explicit samples through updateDiskSpace.
	diskUsage := startupDiskUsage
	startupDiskUsage = func(path string) (uint64, uint64, error) {
		_, _, err := diskUsage(path)
		return 1 << 30, 2 << 30, err
	}

	cpus := min(runtime.GOMAXPROCS(0), 4)
	if os.Getenv(isolatedTestCase) != "" {
		// Storage subprocesses share the host with other builds. Keep their
		// GC and worker schedulers from each claiming every available CPU.
		cpus = min(cpus, 2)
	}
	runtime.GOMAXPROCS(cpus)
	_ = os.Setenv("GOMAXPROCS", strconv.Itoa(cpus))

	var probeDir string
	if os.Getenv(testHandoffSuccessorDir) == "" {
		var err error
		probeDir, err = os.MkdirTemp("", "carbon-test-probes-")
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		binary, err := os.Executable()
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		for _, capabilities := range []string{"read,shared", "read,shared,input", "read,input", "read,input,store-input"} {
			path := filepath.Join(probeDir, capabilities)
			if err := os.Symlink(binary, path); err != nil {
				fmt.Fprintln(os.Stderr, err)
				os.Exit(1)
			}
		}
		_ = os.Setenv(testHandoffSuccessorDir, probeDir)
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
	// These short storage fixtures mostly wait for disk. Bound both the runner
	// and process fanout explicitly; m.Run still parses an explicit -parallel.
	_ = flag.Set("test.parallel", strconv.Itoa(cap(isolatedTestSlots)))
	code := m.Run()
	if probeDir != "" {
		_ = os.RemoveAll(probeDir)
	}
	if seedDir != "" {
		_ = os.RemoveAll(seedDir)
	}
	os.Exit(code)
}

func firstTestArgument() string {
	if len(os.Args) > 1 {
		return os.Args[1]
	}
	return ""
}
