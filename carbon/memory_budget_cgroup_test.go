package carbon

import (
	"os"
	"testing"
)

func TestCgroupMemory(t *testing.T) {
	for _, tc := range []struct {
		name, groups, mounts string
		files                map[string]string
		want                 uint64
	}{
		{"host", "", "", nil, 100 << 30},
		{"v2 ancestor", "0::/team/app\n", "1 0 0:1 / /sys/fs/cgroup rw - cgroup2 cgroup rw\n", map[string]string{"/sys/fs/cgroup/team/app/memory.max": "max", "/sys/fs/cgroup/team/memory.max": "2147483648", "/sys/fs/cgroup/memory.max": "4294967296"}, 2 << 30},
		{"v1 remapped", "5:cpu,memory:/tenant/app\n", "1 0 0:1 /tenant /limits rw - cgroup cgroup rw,memory\n", map[string]string{"/limits/app/memory.limit_in_bytes": "1073741824", "/limits/memory.limit_in_bytes": "9223372036854771712"}, 1 << 30},
		{"namespace root", "0::/\n", "1 0 0:1 /tenant /limits rw - cgroup2 cgroup rw\n", map[string]string{"/limits/memory.max": "3221225472"}, 3 << 30},
		{"namespace parent", "0::/../../tenant\n", "1 0 0:1 / /limits rw - cgroup2 cgroup rw\n", map[string]string{"/limits/memory.max": "3221225472"}, 3 << 30},
		{"host smaller", "0::/\n", "1 0 0:1 / /limits rw - cgroup2 cgroup rw\n", map[string]string{"/limits/memory.max": "9223372036854771712"}, 100 << 30},
		{"escaped mount", "0::/\n", `1 0 0:1 / /my\040limits rw - cgroup2 cgroup rw`, map[string]string{"/my limits/memory.max": "1073741824"}, 1 << 30},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := cgroupMemory(100<<30, tc.groups, tc.mounts, func(path string) ([]byte, error) {
				v, ok := tc.files[path]
				if !ok {
					return nil, os.ErrNotExist
				}
				return []byte(v), nil
			})
			if err != nil || got != tc.want {
				t.Fatalf("got %d, %v; want %d", got, err, tc.want)
			}
		})
	}
	for _, fail := range []string{"permission", "zero", "invalid"} {
		t.Run(fail, func(t *testing.T) {
			_, err := cgroupMemory(100<<30, "0::/", "1 0 0:1 / /limits rw - cgroup2 cgroup rw", func(string) ([]byte, error) {
				switch fail {
				case "permission":
					return nil, os.ErrPermission
				case "zero":
					return []byte("0"), nil
				default:
					return []byte("invalid"), nil
				}
			})
			if err == nil {
				t.Fatalf("accepted %s", fail)
			}
		})
	}
}
