package carbon

import (
	"os"
	"runtime"
	"runtime/pprof"
	"testing"

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
