package carbon

import (
	"runtime/debug"
	"runtime/metrics"
	"sync/atomic"
	"testing"
	"time"
)

func TestV2DefaultGCPercent(t *testing.T) {
	for _, tc := range []struct {
		name, env string
		want      int
	}{
		{"default", "", 75},
		{"explicit", "30", 200},
		{"off", "off", 200},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("GOGC", tc.env)
			old := debug.SetGCPercent(200)
			defer debug.SetGCPercent(old)
			app := v2TestApp(t, t.TempDir())
			// With expiry configured, each owner open reads the core clock to
			// resolve its names' windows: record the growth recovery runs with.
			app.Config.Whisper.StoreExpiration = &Duration{time.Hour}
			var recovering atomic.Bool
			var during atomic.Int64
			recovering.Store(true)
			app.v2Now = func() time.Time {
				if recovering.Load() {
					s := []metrics.Sample{{Name: "/gc/gogc:percent"}}
					metrics.Read(s)
					during.Store(int64(s[0].Value.Uint64()))
				}
				return time.Now()
			}
			if err := app.Start(); err != nil {
				t.Fatal(err)
			}
			recovering.Store(false)
			defer app.Stop()
			if got := during.Load(); got != 200 {
				t.Fatalf("GC percent during v2 recovery = %d, want the runtime's 200", got)
			}
			if got := debug.SetGCPercent(200); got != tc.want {
				t.Fatalf("GC percent after v2 startup = %d, want %d", got, tc.want)
			}
		})
	}
}
