package carbon

import (
	"path/filepath"
	"testing"
)

func TestV2L0Dir(t *testing.T) {
	shm := t.TempDir()
	for _, c := range []struct {
		name, l0, shm, want string
		bad                 bool
	}{
		{name: "default on shm", shm: shm, want: filepath.Join(shm, "go-carbon-v2-l0", "/var/lib/carbon/v2")},
		{name: "no shm keeps path", shm: filepath.Join(shm, "missing")},
		{name: "explicit", l0: "/run/carbon/l0/", shm: shm, want: "/run/carbon/l0"},
		{name: "relative", l0: "l0", shm: shm, bad: true},
		{name: "same as path", l0: "/var/lib/carbon/v2/", shm: shm, bad: true},
	} {
		got, err := v2L0Dir(v2Config{Path: "/var/lib/carbon/v2", L0Dir: c.l0}, c.shm)
		if c.bad != (err != nil) || got != c.want {
			t.Errorf("%s: %q, %v; want %q (error %v)", c.name, got, err, c.want, c.bad)
		}
	}
	// Stores on one host get distinct defaults.
	a, _ := v2L0Dir(v2Config{Path: "/var/lib/carbon/a"}, shm)
	b, _ := v2L0Dir(v2Config{Path: "/var/lib/carbon/b"}, shm)
	if a == b {
		t.Fatalf("two stores share default L0 dir %s", a)
	}
}
