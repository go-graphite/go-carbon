//go:build linux && !race

package cache

import (
	"encoding/json"
	"fmt"
	"math"
	"math/rand/v2"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"

	"github.com/go-graphite/go-carbon/internal/recovery"
	"github.com/go-graphite/go-carbon/points"
)

// Separate processes exclude fixture generation and previous tests from the
// kernel's peak RSS. Do not force GC before measuring: restore's transient heap
// and file residency both count. Race instrumentation has a different RSS model.
func TestPendingRestorePeakRSS(t *testing.T) {
	names := 40000
	if s := os.Getenv("GO_CARBON_RESTORE_PEAK_NAMES"); s != "" {
		var err error
		names, err = strconv.Atoi(s)
		if err != nil || names < 40000 {
			t.Fatal("GO_CARBON_RESTORE_PEAK_NAMES must be >= 40000")
		}
	}
	for _, density := range []int{700, 850} {
		t.Run(strconv.Itoa(density), func(t *testing.T) {
			dir := t.TempDir()
			root := filepath.Join(dir, "root")
			if err := os.Mkdir(root, 0700); err != nil {
				t.Fatal(err)
			}
			writeMemoryCheckpointData(t, dir, root, names, denseRecoveryData(density))
			run := func(mode string) uint64 {
				cmd := exec.Command(os.Args[0], "-test.run=^TestPendingRestorePeakProcess$", "-test.v")
				cmd.Env = append(os.Environ(), "GO_CARBON_PEAK_MODE="+mode, "GO_CARBON_PEAK_DIR="+dir,
					fmt.Sprintf("GO_CARBON_PEAK_NAMES=%d", names), fmt.Sprintf("GO_CARBON_PEAK_DENSITY=%d", density),
					"GOGC=200", "GOMEMLIMIT=off", "GOMAXPROCS=4")
				out, err := cmd.CombinedOutput()
				if err != nil {
					t.Fatalf("%s child: %v\n%s", mode, err, out)
				}
				for _, line := range strings.Split(string(out), "\n") {
					if s, ok := strings.CutPrefix(line, "PEAK "); ok {
						var peak uint64
						if err := json.Unmarshal([]byte(s), &peak); err != nil || peak == 0 {
							t.Fatalf("invalid peak: %q", line)
						}
						return peak
					}
				}
				t.Fatalf("%s child omitted peak: %s", mode, out)
				return 0
			}
			live, restored := run("live"), run("restore")
			count := uint64(names) * uint64(density)
			t.Logf("names=%d points=%d live_peak=%d restore_peak=%d restore_B_per_point=%.2f ratio=%.3f", names, count, live, restored, float64(restored)/float64(count), float64(restored)/float64(live))
			if restored > 16*count+64<<20 {
				t.Errorf("restore peak %d exceeds 16B/point plus 64MiB", restored)
			}
			if restored > live*5/4 {
				t.Errorf("restore peak %d exceeds 1.25 times live peak %d", restored, live)
			}
		})
	}
}

func denseRecoveryData(n int) []points.Point {
	data := make([]points.Point, n)
	r := rand.New(rand.NewPCG(1, 2))
	for i := range data {
		v := r.Uint64()
		if v&0x7ff0000000000000 == 0x7ff0000000000000 {
			v ^= 0x0010000000000000 // finite, high-entropy IEEE values
		}
		data[i] = points.Point{Timestamp: 1700000000 + int64(i), Value: math.Float64frombits(v)}
	}
	return data
}

func TestPendingRestorePeakProcess(t *testing.T) {
	mode := os.Getenv("GO_CARBON_PEAK_MODE")
	if mode == "" {
		t.Skip("fresh-process helper")
	}
	names, err := strconv.Atoi(os.Getenv("GO_CARBON_PEAK_NAMES"))
	if err != nil {
		t.Fatal(err)
	}
	density, err := strconv.Atoi(os.Getenv("GO_CARBON_PEAK_DENSITY"))
	if err != nil {
		t.Fatal(err)
	}
	dir := os.Getenv("GO_CARBON_PEAK_DIR")
	c := New()
	data := denseRecoveryData(density)
	name := func(i int) string {
		return fmt.Sprintf("synthetic.restore.%s%012d", strings.Repeat("long_prefix.", 30), i)
	}
	if mode == "live" {
		for i := range names {
			c.AddRecovered(&points.Points{Metric: name(i), Data: data})
		}
	} else {
		b, err := recovery.OpenBundle(dir, filepath.Join(dir, "root"))
		if err != nil {
			t.Fatal(err)
		}
		defer b.Close()
		c.SetMaxSize(uint64(names*density)*4 + 1)
		if err := c.AttachPendingRecoveryWithPreparedNames(b); err != nil {
			t.Fatal(err)
		}
		if err := c.RecoverPending(make(chan struct{}), 0); err != nil {
			t.Fatal(err)
		}
	}
	if c.Size() != int64(names*density) || c.Len() != int32(names) {
		t.Fatalf("incomplete cache: points=%d names=%d", c.Size(), c.Len())
	}
	raw, err := os.ReadFile("/proc/self/status")
	if err != nil {
		t.Fatal(err)
	}
	var peak uint64
	for _, line := range strings.Split(string(raw), "\n") {
		if strings.HasPrefix(line, "VmHWM:") {
			peak, err = strconv.ParseUint(strings.Fields(line)[1], 10, 64)
			if err != nil {
				t.Fatal(err)
			}
			peak *= 1024
		}
	}
	// Validate decoded points after the measured open/attach/claim interval.
	for i := 0; i < names; i += max(names/256, 1) {
		got := c.Get(name(i))
		if len(got) != len(data) {
			t.Fatalf("point count for %d: %d", i, len(got))
		}
		for j := range data {
			if got[j].Timestamp != data[j].Timestamp || math.Float64bits(got[j].Value) != math.Float64bits(data[j].Value) {
				t.Fatalf("changed point %d/%d", i, j)
			}
		}
	}
	fmt.Printf("PEAK %d\n", peak)
	runtime.KeepAlive(c)
}
