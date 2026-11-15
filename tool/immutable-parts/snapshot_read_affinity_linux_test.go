//go:build linux

package main

import (
	"fmt"
	"golang.org/x/sys/unix"
	"os"
	"runtime"
	"strconv"
	"strings"
)

// Launch the process under taskset on writer CPUs; only locked reader threads
// receive the disjoint reader mask. Restore before returning the thread to Go.
func pinSnapshotReader() (func(), error) {
	spec := os.Getenv("PARTS_READ_BENCH_CPUS")
	if spec == "" {
		return nil, fmt.Errorf("reader CPU list required")
	}
	runtime.LockOSThread()
	var old, mask unix.CPUSet
	fail := func(err error) (func(), error) { runtime.UnlockOSThread(); return nil, err }
	if err := unix.SchedGetaffinity(0, &old); err != nil {
		return fail(err)
	}
	for _, item := range strings.Split(spec, ",") {
		bounds := strings.Split(item, "-")
		lo, err := strconv.Atoi(bounds[0])
		if err != nil {
			return fail(err)
		}
		hi := lo
		if len(bounds) == 2 {
			hi, err = strconv.Atoi(bounds[1])
			if err != nil {
				return fail(err)
			}
		}
		if lo < 0 || hi < lo || hi >= 1024 {
			return fail(fmt.Errorf("invalid reader CPU range"))
		}
		for cpu := lo; cpu <= hi; cpu++ {
			if old.IsSet(cpu) {
				return fail(fmt.Errorf("reader CPU%d overlaps writer affinity", cpu))
			}
			mask.Set(cpu)
		}
	}
	if err := unix.SchedSetaffinity(0, &mask); err != nil {
		return fail(err)
	}
	return func() { _ = unix.SchedSetaffinity(0, &old); runtime.UnlockOSThread() }, nil
}
