package carbon

import (
	"os"
	"testing"
	"time"

	"github.com/BurntSushi/toml"
)

func TestOutOfOrderPolicyConfig(t *testing.T) {
	for _, tt := range []struct {
		name        string
		points      int
		age, margin time.Duration
		threshold   int64
		wantError   bool
	}{
		{"legacy", 0, 0, 0, 65536, false},
		{"point policy", 1024, 15 * time.Minute, 5 * time.Minute, 0, false},
		{"negative count", -1, time.Minute, time.Minute, 65536, true},
		{"no age bound", 1024, 0, time.Minute, 65536, true},
		{"negative age", 1024, -time.Minute, time.Minute, 65536, true},
		{"no retention margin", 1024, time.Minute, 0, 65536, true},
		{"invalid legacy threshold", 0, time.Minute, time.Minute, 0, true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			path := TestConfig(t.TempDir())
			cfg, err := ReadConfig(path)
			if err != nil {
				t.Fatal(err)
			}
			cfg.Whisper.Compressed = true
			cfg.Whisper.OutOfOrder = true
			cfg.Whisper.OutOfOrderCompactMinPoints = tt.points
			cfg.Whisper.OutOfOrderCompactMaxPointAge = Duration{tt.age}
			cfg.Whisper.OutOfOrderCompactRetentionMargin = Duration{tt.margin}
			cfg.Whisper.OutOfOrderCompactThreshold = tt.threshold
			f, err := os.Create(path)
			if err != nil {
				t.Fatal(err)
			}
			err = toml.NewEncoder(f).Encode(cfg)
			closeErr := f.Close()
			if err != nil {
				t.Fatal(err)
			}
			if closeErr != nil {
				t.Fatal(closeErr)
			}
			app := New(path)
			err = app.ParseConfig()
			if (err != nil) != tt.wantError {
				t.Fatalf("ParseConfig error=%v, wantError=%v", err, tt.wantError)
			}
			if err == nil && (app.Config.Whisper.OutOfOrderCompactMinPoints != tt.points || app.Config.Whisper.OutOfOrderCompactMaxPointAge.Value() != tt.age || app.Config.Whisper.OutOfOrderCompactRetentionMargin.Value() != tt.margin) {
				t.Fatal("policy changed during TOML round trip")
			}
		})
	}
}
