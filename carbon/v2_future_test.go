package carbon

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/internal/v2daemon"
)

// future-limit defaults to 48h, "0s" disables it, and each dropped point
// counts once in v2_future_dropped_points_total, not as a name refusal.
// Two reads of one file compare equal on V2, which configure() requires to
// accept a reload.
func TestV2FutureLimitConfigAndCounter(t *testing.T) {
	if got := NewConfig().V2.FutureLimit.Value(); got != 48*time.Hour {
		t.Fatalf("default future-limit %v, want 48h", got)
	}
	for body, want := range map[string]time.Duration{"[v2]\nfuture-limit = \"0s\"\n": 0, "[v2]\nfuture-limit = \"3h\"\n": 3 * time.Hour, "[v2]\n": 48 * time.Hour} {
		path := filepath.Join(t.TempDir(), "go-carbon.conf")
		if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
		cfg, err := ReadConfig(path)
		if err != nil {
			t.Fatal(err)
		}
		if got := cfg.V2.FutureLimit.Value(); got != want {
			t.Fatalf("%q: future-limit %v, want %v", body, got, want)
		}
		again, err := ReadConfig(path)
		if err != nil {
			t.Fatal(err)
		}
		if cfg.V2 != again.V2 {
			t.Fatalf("%q: V2 differs between two reads of one file, so a reload would be refused", body)
		}
	}
	v := &v2Runtime{metrics: &v2ModeMetrics{}}
	v.rejectName(nil, "far.future", &v2daemon.FutureTimestampError{Timestamp: 4026531600, Clock: 1800000000})
	v.rejectName(nil, "far.future", &v2daemon.FutureTimestampError{Timestamp: 4026531601, Clock: 1800000000})
	if got, names := v.metrics.futureDropped.Load(), v.metrics.rejectedNames.Load(); got != 2 || names != 0 {
		t.Fatalf("future dropped %d name rejected %d, want 2 and 0", got, names)
	}
	sent := map[string]float64{}
	v.metrics.Stat(func(name string, value float64) { sent[name] = value })
	if sent["futureDroppedPoints"] != 2 {
		t.Fatalf("stats %v", sent)
	}
}
