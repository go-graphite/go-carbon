package carbon

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/helper/qa"
	"github.com/stretchr/testify/assert"
)

func TestStartStop(t *testing.T) {
	assert := assert.New(t)

	startGoroutineNum := runtime.NumGoroutine()

	for i := 0; i < 10; i++ {
		qa.Root(t, func(root string) {
			configFile := TestConfig(root)

			app := New(configFile)

			assert.NoError(app.ParseConfig())
			assert.NoError(app.Start())

			app.Stop()
		})
	}

	endGoroutineNum := runtime.NumGoroutine()

	// GC worker etc
	if !assert.InDelta(startGoroutineNum, endGoroutineNum, 4) {
		p := pprof.Lookup("goroutine")
		p.WriteTo(os.Stdout, 1)
	}

}

func TestRestoreStartupOrdering(t *testing.T) {
	tests := []struct {
		name             string
		globalCompressed bool
		schemaCompressed bool
	}{
		{name: "global", globalCompressed: true},
		{name: "schema", schemaCompressed: true},
		{name: "normal whisper"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			qa.Root(t, func(root string) {
				app := New(TestConfig(root))
				assert.NoError(t, app.ParseConfig())

				app.Config.Whisper.Compressed = tt.globalCompressed
				if tt.schemaCompressed {
					compressed := true
					app.Config.Whisper.Schemas[0].Compressed = &compressed
				}
				app.Config.Dump.Enabled = true
				app.Config.Dump.Path = root
				app.Config.Dump.RestorePerSecond = 0
				app.Config.Whisper.MaxUpdatesPerSecond = 10
				app.Config.Cache.WriteoutMinPoints = 8
				app.Config.Cache.WriteoutMaxDelay = Duration{time.Hour}
				app.Config.Udp.Enabled = false
				app.Config.Tcp.Enabled = false
				app.Config.Pickle.Enabled = false
				app.Config.Grpc.Enabled = false
				app.Config.Carbonlink.Enabled = false

				dump := filepath.Join(root, "input.1.1")
				now := time.Now().Unix()
				data := fmt.Sprintf("restore.one 1 %d\nrestore.two 2 %d\n", now, now)
				assert.NoError(t, os.WriteFile(dump, []byte(data), 0o600))
				assert.NoError(t, app.Start())
				defer app.Stop()

				_, err := os.Stat(dump)
				if tt.globalCompressed || tt.schemaCompressed {
					assert.ErrorIs(t, err, os.ErrNotExist)
					assert.True(t, app.Cache.IsEmpty(), "restored cwhisper data must be persisted before receivers start")
					_, err = os.Stat(filepath.Join(root, "restore", "two.wsp"))
					assert.NoError(t, err)
					stats := make(map[string]float64)
					app.Persister.Stat(func(name string, value float64) { stats[name] = value })
					assert.Zero(t, stats["created"], "restore creations must not be reported as live interval activity")
					assert.Zero(t, stats["committedPoints"], "restore commits must not be reported as live interval activity")
					return
				}
				assert.NoError(t, err)

				assert.Eventually(t, func() bool {
					_, err := os.Stat(dump)
					return os.IsNotExist(err)
				}, 30*time.Second, 10*time.Millisecond,
					"dumpFile should be removed after restore")
			})
		})
	}
}

func TestReloadAndCollectorDeadlock(t *testing.T) {
	// go func() {
	//	http.ListenAndServe("localhost:6060", nil)
	// }()

	qa.Root(t, func(root string) {
		configFile := TestConfig(root)
		app := New(configFile)

		assert.NoError(t, app.ParseConfig())

		app.Config.Common.MetricInterval = &Duration{time.Microsecond}
		assert.NoError(t, app.Start())

		reloadChan := make(chan struct{}, 1)
		N := 1024

		// start reload loop
		go func() {
			for i := N; i > 0; i-- {
				app.ReloadConfig()
				reloadChan <- struct{}{}
			}
		}()

		ticker := time.NewTimer(0)

		// goroutine doing reloadConfig should send N notifications if there were no deadlock
		for rN := 0; rN < N; {
			if !ticker.Stop() {
				<-ticker.C
			}
			ticker.Reset(1 * time.Second)

			select {
			case <-reloadChan:
				rN++
			case <-ticker.C:
				t.Fatalf("Collector and SIGHUP handers deadlocked")
			}
		}
	})
}
