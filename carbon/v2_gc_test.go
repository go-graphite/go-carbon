package carbon

import (
	"runtime/debug"
	"testing"
)

func TestV2DefaultGCPercent(t *testing.T) {
	for _, tc := range []struct {
		name, env string
		want      int
	}{
		{"default", "", v2SteadyGCPercent},
		{"explicit", "30", 200},
		{"off", "off", 200},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("GOGC", tc.env)
			old := debug.SetGCPercent(200)
			defer debug.SetGCPercent(old)
			app := v2TestApp(t, t.TempDir())
			if err := app.Start(); err != nil {
				t.Fatal(err)
			}
			defer app.Stop()
			if got := debug.SetGCPercent(200); got != tc.want {
				t.Fatalf("GC percent after v2 startup = %d, want %d", got, tc.want)
			}
		})
	}
}
