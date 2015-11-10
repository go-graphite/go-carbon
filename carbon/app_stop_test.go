package carbon

import (
	"runtime"
	"testing"

	"github.com/lomik/go-carbon/carbon"
	"github.com/lomik/go-carbon/qa"
	"github.com/stretchr/testify/assert"
)

func TestStartStop(t *testing.T) {
	assert := assert.New(t)

	startGoroutineNum := runtime.NumGoroutine()

	for i := 0; i < 10; i++ {
		qa.Root(t, func(root string) {
			configFile := TestConfig(root)

			app := carbon.New(configFile)

			assert.NoError(app.ParseConfig())
			assert.NoError(app.Start())

			// time.Sleep(time.Minute)

			app.Stop()
		})
	}

	// p := pprof.Lookup("goroutine")
	// p.WriteTo(os.Stdout, 1)

	endGoroutineNum := runtime.NumGoroutine()

	// GC worker etc
	assert.InDelta(startGoroutineNum, endGoroutineNum, 2)

}
