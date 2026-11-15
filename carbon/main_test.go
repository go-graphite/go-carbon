package carbon

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"

	"github.com/go-graphite/go-carbon/internal/handoff"
)

const testHandoffSuccessorDir = "GRAPHITE_TEST_HANDOFF_SUCCESSOR_DIR"

// Capability probes execute this same test binary through named symlinks.
// This retains real subprocess probing without shell-script assessment during
// a timed handoff. Isolated cases inherit the paths.
func TestMain(m *testing.M) {
	// Answer before storage fixtures, scheduler changes, or testing flag parsing.
	// The symlink basename is the successor's advertised capability set.
	if len(os.Args) > 1 {
		probe := map[string]struct{ capability, protocol string }{
			"-handoff-protocol":              {"read", handoff.Protocol},
			"-input-handoff-protocol":        {"input", handoff.InputProtocol},
			"-shared-store-handoff-protocol": {"shared", handoff.SharedStoreProtocol},
			"-store-input-handoff-protocol":  {"store-input", handoff.StoreInputProtocol},
		}
		if p, ok := probe[os.Args[1]]; ok {
			for _, capability := range strings.Split(filepath.Base(os.Args[0]), ",") {
				if capability == p.capability {
					fmt.Println(p.protocol)
					os.Exit(0)
				}
			}
			os.Exit(2)
		}
	}
	var probeDir string

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
		binary, err := os.Executable()
		if err == nil {
			probeDir, err = os.MkdirTemp("", "carbon-handoff-probes-")
		}
		if err == nil {
			for _, capabilities := range []string{"read,shared", "read,shared,input", "read,input", "read,input,store-input"} {
				if err = os.Symlink(binary, filepath.Join(probeDir, capabilities)); err != nil {
					break
				}
			}
		}
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			_ = os.RemoveAll(probeDir)
			os.Exit(1)
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
	code := m.Run()
	if probeDir != "" {
		_ = os.RemoveAll(probeDir)
	}
	if seedDir != "" {
		_ = os.RemoveAll(seedDir)
	}
	os.Exit(code)
}
