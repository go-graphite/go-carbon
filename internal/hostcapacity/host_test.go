package hostcapacity

import "testing"

func TestHostSampleExcludesGuestAndIOWait(t *testing.T) {
	s := parseHostSample("cpu  10 20 30 400 50 6 7 8 100 200\n", "MemTotal: 1000 kB\nMemAvailable: 800 kB\n")
	if !s.valid || s.total != 531 || s.idle != 400 || s.ioWait != 50 || s.memory != .8 {
		t.Fatal(s)
	}
	for _, memory := range []string{"", "MemTotal: junk kB", "MemTotal: 100 kB\nMemAvailable: 200 kB"} {
		if parseHostSample("cpu 1 2 3 4 5 6 7 8\n", memory).valid {
			t.Fatal("invalid memory authorized growth", memory)
		}
	}
}
