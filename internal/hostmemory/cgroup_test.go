package hostmemory

import (
	"math"
	"os"
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
