// Package hostmemory resolves the memory limits shared by startup budgets and live admission.
package hostmemory

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// Resolve the process's memory hierarchy through mountinfo, including a
// container's remapped mount root. A parent limit also constrains the process.
func Limit(host uint64, groups, mounts string, read func(string) ([]byte, error)) (uint64, error) {
	return reduce(host, groups, mounts, read, 0)
}

// Headroom returns the smallest remaining fraction of every visible ancestor limit.
// Unlike a host-wide MemAvailable value, this includes memory charged to siblings
// in the process cgroup. memory.high is also a pressure boundary on cgroup v2.
func Headroom(groups, mounts string, read func(string) ([]byte, error)) (float64, error) {
	value, err := reduce(1000000, groups, mounts, read, 1)
	return float64(value) / 1000000, err
}

// Available returns free bytes inside every ancestor limit, retaining 5% or
// 64 MiB per boundary for live input, runtime/native allocations and readers.
func Available(host uint64, groups, mounts string, read func(string) ([]byte, error)) (uint64, error) {
	return reduce(host, groups, mounts, read, 2)
}

func reduce(host uint64, groups, mounts string, read func(string) ([]byte, error), mode int) (uint64, error) {
	paths := make(map[string]string)
	for _, line := range strings.Split(groups, "\n") {
		f := strings.SplitN(line, ":", 3)
		if len(f) != 3 {
			continue
		}
		if f[1] == "" {
			paths["cgroup2"] = f[2]
		}
		if strings.Contains(","+f[1]+",", ",memory,") {
			paths["cgroup"] = f[2]
		}
	}
	unescape := strings.NewReplacer(`\040`, " ", `\011`, "\t", `\012`, "\n", `\134`, `\`)
	for _, line := range strings.Split(mounts, "\n") {
		left, right, ok := strings.Cut(line, " - ")
		if !ok {
			continue
		}
		f, options := strings.Fields(left), strings.Fields(right)
		if len(f) < 5 || len(options) < 3 {
			continue
		}
		kind := options[0]
		group, ok := paths[kind]
		if !ok || kind == "cgroup" && !strings.Contains(","+options[2]+",", ",memory,") {
			continue
		}
		root, mount := filepath.Clean(unescape.Replace(f[3])), filepath.Clean(unescape.Replace(f[4]))
		file := "memory.max"
		if kind == "cgroup" {
			file = "memory.limit_in_bytes"
		}
		path := mount
		if root == "/" || group == root || strings.HasPrefix(group, root+"/") {
			path = filepath.Join(mount, strings.TrimPrefix(group, root))
		}
		// A cgroup namespace may report a path above its visible mount root.
		// In that case only the mount root's limit is visible to this process.
		if path != mount && !strings.HasPrefix(path, mount+string(filepath.Separator)) {
			path = mount
		}
		for {
			data, err := read(filepath.Join(path, file))
			if err != nil && !os.IsNotExist(err) {
				return 0, err
			}
			if err == nil && strings.TrimSpace(string(data)) != "max" {
				n, err := strconv.ParseUint(strings.TrimSpace(string(data)), 10, 64)
				if err != nil || n == 0 {
					return 0, fmt.Errorf("invalid cgroup memory limit in %s", path)
				}
				if mode == 0 {
					host = min(host, n)
				} else {
					usageFile := "memory.current"
					if kind == "cgroup" {
						usageFile = "memory.usage_in_bytes"
					}
					usage, err := read(filepath.Join(path, usageFile))
					if err != nil {
						return 0, err
					}
					used, err := strconv.ParseUint(strings.TrimSpace(string(usage)), 10, 64)
					if err != nil {
						return 0, err
					}
					host = min(host, reduceHeadroom(n, used, mode))
				}
			}
			if mode != 0 && kind == "cgroup2" {
				data, err := read(filepath.Join(path, "memory.high"))
				if err != nil && !os.IsNotExist(err) {
					return 0, err
				}
				if err == nil && strings.TrimSpace(string(data)) != "max" {
					high, err := strconv.ParseUint(strings.TrimSpace(string(data)), 10, 64)
					if err != nil {
						return 0, err
					}
					data, err = read(filepath.Join(path, "memory.current"))
					if err != nil {
						return 0, err
					}
					used, err := strconv.ParseUint(strings.TrimSpace(string(data)), 10, 64)
					if err != nil {
						return 0, err
					}
					host = min(host, reduceHeadroom(high, used, mode))
				}
			}
			if path == mount {
				break
			}
			parent := filepath.Dir(path)
			if parent == path || parent != mount && !strings.HasPrefix(parent, mount+string(filepath.Separator)) {
				break
			}
			path = parent
		}
	}
	return host, nil
}

func reduceHeadroom(limit, used uint64, mode int) uint64 {
	if mode == 2 {
		reserve := max(uint64(64<<20), limit/20)
		if used >= limit || limit-used <= reserve {
			return 0
		}
		return limit - used - reserve
	}
	if used >= limit {
		return 0
	}
	return uint64((1 - float64(used)/float64(limit)) * 1000000)
}
