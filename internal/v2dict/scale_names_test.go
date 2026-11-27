package v2dict

import (
	"bufio"
	"compress/gzip"
	"fmt"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/zeebo/xxh3"
)

// TestScaleNames appends V2DICT_SCALE_N real-shaped names (V2DICT_SCALE_NAMES,
// a gzipped name list, extended with _xs<k> copies) from V2DICT_SCALE_OWNERS
// goroutines in 256-name AppendBatch calls, as seal owners do, and reports
// per step: append ns/name, levels (and unfiltered L1 levels), negative and
// positive lookup ns, then dictionary bytes/name, open time and heap after
// open. It measures; it asserts nothing. Run on a bench host, not in CI.
func TestScaleNames(t *testing.T) {
	n, _ := strconv.Atoi(os.Getenv("V2DICT_SCALE_N"))
	src := os.Getenv("V2DICT_SCALE_NAMES")
	if n == 0 || src == "" {
		t.Skip("set V2DICT_SCALE_N and V2DICT_SCALE_NAMES")
	}
	owners := 16
	if v, _ := strconv.Atoi(os.Getenv("V2DICT_SCALE_OWNERS")); v > 0 {
		owners = v
	}
	base := readNames(t, src)
	name := func(i int) string {
		s := base[i%len(base)]
		k := i / len(base)
		if k == 0 {
			return s
		}
		cut := strings.LastIndexByte(s, '.')
		if cut < 0 {
			return fmt.Sprintf("xs%d_%s", k, s)
		}
		return fmt.Sprintf("%s_xs%d%s", s[:cut], k, s[cut:])
	}
	const vshards = 1024
	vs := func(s string) uint16 { return uint16(xxh3.HashString(s) % vshards) }
	dir := t.TempDir()
	if d := os.Getenv("V2DICT_SCALE_DIR"); d != "" {
		dir = d
	}
	d, err := Open(dir, Options{Shards: owners})
	if err != nil {
		t.Fatal(err)
	}
	step := max(n/20, 1)
	start := time.Now()
	last := start
	for lo := 0; lo < n; lo += step {
		hi := min(lo+step, n)
		var wg sync.WaitGroup
		for o := range owners {
			wg.Add(1)
			go func() {
				defer wg.Done()
				batch := make([]NewName, 0, 256)
				for i := lo; i < hi; i++ {
					s := name(i)
					v := vs(s)
					if int(v)%owners != o {
						continue
					}
					batch = append(batch, NewName{Group: "g", VShard: v, Name: s})
					if len(batch) == cap(batch) {
						if err := d.AppendBatch(batch); err != nil {
							t.Error(err)
							return
						}
						batch = batch[:0]
					}
				}
				if len(batch) > 0 {
					if err := d.AppendBatch(batch); err != nil {
						t.Error(err)
					}
				}
			}()
		}
		wg.Wait()
		if err := d.Sync(); err != nil {
			t.Fatal(err)
		}
		now := time.Now()
		st := d.Stats()
		l1 := 0
		for _, l := range d.snap.Load().levels {
			if !l.compact {
				l1++
			}
		}
		// Negative: names past N; positive: the oldest names (deepest level).
		const probes = 2000
		t0 := time.Now()
		for j := range probes {
			s := name(n + j*7919)
			if _, ok := d.Lookup("g", vs(s), []byte(s)); ok {
				t.Fatalf("unexpected hit %s", s)
			}
		}
		neg := time.Since(t0).Nanoseconds() / probes
		t0 = time.Now()
		for j := range probes {
			s := name((j * 7919) % max(hi/4, 1))
			if _, ok := d.Lookup("g", vs(s), []byte(s)); !ok {
				t.Fatalf("missing %s", s)
			}
		}
		pos := time.Since(t0).Nanoseconds() / probes
		fmt.Printf("SCALE names=%d t=%.1fs append_ns=%d levels=%d l1=%d mergers=%d neg_ns=%d pos_ns=%d file_B/name=%.1f\n",
			hi, now.Sub(start).Seconds(), now.Sub(last).Nanoseconds()/int64(hi-lo), st.Levels, l1, mergers(d), neg, pos, float64(st.FileBytes)/float64(hi))
		last = time.Now()
	}
	appendCPU := processCPU()
	t0 := time.Now()
	d.Merge()
	st := d.Stats()
	fmt.Printf("SCALE settle merge_s=%.1f levels=%d file_B/name=%.1f append_wall_s=%.1f append_cpu_s=%.1f settled_cpu_s=%.1f\n",
		time.Since(t0).Seconds(), st.Levels, float64(st.FileBytes)/float64(n), t0.Sub(start).Seconds(), appendCPU.Seconds(), processCPU().Seconds())
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	runtime.GC()
	var m0, m1 runtime.MemStats
	runtime.ReadMemStats(&m0)
	t0 = time.Now()
	d, err = Open(dir, Options{Shards: owners, NoMerge: true})
	if err != nil {
		t.Fatal(err)
	}
	open := time.Since(t0)
	runtime.GC()
	runtime.ReadMemStats(&m1)
	fmt.Printf("SCALE open_ms=%d heap_after_open_MB=%.1f levels=%d\n", open.Milliseconds(), float64(int64(m1.HeapAlloc)-int64(m0.HeapAlloc))/1e6, d.Stats().Levels)
	d.Close()
}

func readNames(t *testing.T, path string) []string {
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	z, err := gzip.NewReader(f)
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	sc := bufio.NewScanner(z)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	for sc.Scan() {
		out = append(out, sc.Text())
	}
	if err := sc.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

// processCPU is the test process's user+system CPU time: appends, flushes
// and merges.
func processCPU() time.Duration {
	var ru syscall.Rusage
	_ = syscall.Getrusage(syscall.RUSAGE_SELF, &ru)
	return time.Duration(ru.Utime.Nano() + ru.Stime.Nano())
}

func mergers(d *Dict) int {
	d.mergeMu.Lock()
	defer d.mergeMu.Unlock()
	return d.mergers
}
