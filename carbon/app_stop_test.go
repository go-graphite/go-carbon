package carbon

import (
	"os"
	"runtime"
	"runtime/pprof"
	"testing"
	"time"

	"github.com/lomik/go-carbon/qa"
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
			assert.NoError(app.Start(nil))

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

func TestReloadAndCollectorDeadlock(t *testing.T) {
	//go func() {
	//	http.ListenAndServe("localhost:6060", nil)
	//}()

	qa.Root(t, func(root string) {
		configFile := TestConfig(root)
		app := New(configFile)

		assert.NoError(t, app.ParseConfig())

		app.Config.Common.MetricInterval = &Duration{time.Microsecond}
		assert.NoError(t, app.Start(nil))

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
