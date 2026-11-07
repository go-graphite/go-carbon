package hostmemory

import (
	"math"
	"os"
	"strconv"
	"testing"
)

func TestHeadroomUsesAncestorAndHighLimits(t *testing.T) {
	files := map[string]string{
		"/cg/parent/worker/memory.max": "1000", "/cg/parent/worker/memory.current": "100",
		"/cg/parent/memory.max": "2000", "/cg/parent/memory.current": "1900",
		"/cg/memory.max": "max", "/cg/memory.high": "max",
	}
	read := func(path string) ([]byte, error) {
		if value, ok := files[path]; ok {
			return []byte(value), nil
		}
		return nil, os.ErrNotExist
	}
	const groups = "0::/parent/worker\n"
	const mounts = "1 0 0:1 / /cg rw - cgroup2 cgroup rw\n"
	fraction, err := Headroom(groups, mounts, read)
	if err != nil || math.Abs(fraction-.05) > .000001 {
		t.Fatal("parent charges ignored", fraction, err)
	}
	files["/cg/parent/memory.high"] = "1920"
	fraction, err = Headroom(groups, mounts, read)
	if err != nil || fraction >= .011 {
		t.Fatal("reclaim boundary ignored", fraction, err)
	}
	delete(files, "/cg/parent/memory.current")
	if _, err := Headroom(groups, mounts, read); err == nil {
		t.Fatal("missing usage authorized growth")
	}
}

func TestAvailableCreditsOnlyInactiveFile(t *testing.T) {
	const limit, usage, inactive = uint64(1613978664960), uint64(1521699209216), uint64(1144494227456)
	for _, v1 := range []bool{false, true} {
		t.Run(strconv.FormatBool(v1), func(t *testing.T) {
			groups, mounts := "0::/job", "1 0 0:1 / /cg rw - cgroup2 cgroup rw"
			limitFile, usageFile, key := "memory.max", "memory.current", "inactive_file"
			if v1 {
				groups, mounts = "4:memory:/parent/job", "1 0 0:1 /parent /cg rw - cgroup cgroup rw,memory"
				limitFile, usageFile, key = "memory.limit_in_bytes", "memory.usage_in_bytes", "total_inactive_file"
			}
			files := map[string]string{
				"/cg/job/" + limitFile: strconv.FormatUint(limit, 10),
				"/cg/job/" + usageFile: strconv.FormatUint(usage, 10),
				"/cg/job/memory.stat":  key + " " + strconv.FormatUint(inactive, 10) + "\nfile 1282333937664\nactive_file 137836941312\n",
			}
			read := func(path string) ([]byte, error) {
				if s, ok := files[path]; ok {
					return []byte(s), nil
				}
				return nil, os.ErrNotExist
			}
			got, err := Available(1260000000000, groups, mounts, read)
			want := limit - (usage - inactive) - limit/20
			if err != nil || got != want {
				t.Fatal("reclaimable page cache charged as working set", got, want, err)
			}
			got, err = Available(400000000000, groups, mounts, read)
			if err != nil || got != 400000000000 {
				t.Fatal("host/Go bound exceeded", got, err)
			}
			// Both hard and high boundaries must use their own working set.
			if !v1 {
				files["/cg/job/memory.high"] = strconv.FormatUint(limit-100000000000, 10)
				got, err = Available(1260000000000, groups, mounts, read)
				high := limit - 100000000000
				if err != nil || got != high-(usage-inactive)-high/20 {
					t.Fatal("high boundary lost", got, err)
				}
				delete(files, "/cg/job/memory.high")
			}
			files["/cg/job/memory.stat"] = "file 1282333937664\n"
			got, err = Available(1260000000000, groups, mounts, read)
			if err != nil || got != limit-usage-limit/20 {
				t.Fatal("missing stat authorized growth", got, err)
			}
			files["/cg/job/memory.stat"] = key + " -1\n"
			if _, err = Available(1260000000000, groups, mounts, read); err == nil {
				t.Fatal("invalid stat authorized growth")
			}
			files["/cg/job/memory.stat"] = key + " " + strconv.FormatUint(usage+1, 10) + "\n"
			got, err = Available(limit, groups, mounts, read)
			if err != nil || got != limit-limit/20 {
				t.Fatal("working set underflow", got, err)
			}
		})
	}
}

func TestHeadroomV1RemappedMount(t *testing.T) {
	read := func(path string) ([]byte, error) {
		switch path {
		case "/cg/job/memory.limit_in_bytes":
			return []byte("1000"), nil
		case "/cg/job/memory.usage_in_bytes":
			return []byte("960"), nil
		}
		return nil, os.ErrNotExist
	}
	fraction, err := Headroom("4:memory:/parent/job", "1 0 0:1 /parent /cg rw - cgroup cgroup rw,memory", read)
	if err != nil || math.Abs(fraction-.04) > .000001 {
		t.Fatal(fraction, err)
	}
}

func TestAvailableUsesAncestorChargesAndReserve(t *testing.T) {
	files := map[string]string{"/cg/memory.max": "max", "/cg/parent/memory.max": "85899345920", "/cg/parent/memory.current": "68719476736"}
	read := func(path string) ([]byte, error) {
		if s, ok := files[path]; ok {
			return []byte(s), nil
		}
		return nil, os.ErrNotExist
	}
	available, err := Available(100<<30, "0::/parent", "1 0 0:1 / /cg rw - cgroup2 cgroup rw", read)
	if err != nil || available != 12<<30 {
		t.Fatal("parent remaining bytes/reserve lost", available, err)
	}
	available, err = Available(1<<30, "0::/parent", "1 0 0:1 / /cg rw - cgroup2 cgroup rw", read)
	if err != nil || available != 1<<30 {
		t.Fatal("host free reserve exceeded", available, err)
	}
}
