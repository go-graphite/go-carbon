package carbon

import (
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/cache"
	"github.com/go-graphite/go-carbon/carbonserver"
)

// Exercise the App lifecycle: testing CarbonserverListener.Stop alone misses
// the SIGUSR2 path which exits the process as soon as DumpStop returns.
func TestShutdownWaitsForActiveRead(t *testing.T) {
	for _, tc := range []struct {
		name      string
		dump      bool
		hold      time.Duration
		slowInput bool
	}{
		{name: "Stop", hold: 100 * time.Millisecond},
		// DumpStop has a five-second input timeout. Active reads must get the read
		// server's own grace period, even when they exceed that input timeout.
		{name: "DumpStop", dump: true, hold: 5500 * time.Millisecond},
		{name: "DumpStopSlowInput", dump: true, hold: 5500 * time.Millisecond, slowInput: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			app, started, unblock, readDone := startBlockedShutdownRead(t)
			if tc.slowInput {
				releaseInput := make(chan struct{})
				app.FlushTraces = func() { <-releaseInput }
				t.Cleanup(func() { close(releaseInput) })
			}
			select {
			case <-started:
			case <-time.After(5 * time.Second):
				t.Fatal("read did not start")
			}
			stopped := make(chan error, 1)
			go func() {
				if tc.dump {
					stopped <- app.DumpStop()
				} else {
					app.Stop()
					stopped <- nil
				}
			}()
			select {
			case err := <-stopped:
				unblock()
				<-readDone
				t.Fatalf("shutdown returned with an active read: %v", err)
			case <-time.After(tc.hold):
			}
			unblock()
			if err := <-readDone; err != nil {
				t.Fatal(err)
			}
			select {
			case err := <-stopped:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("shutdown did not finish after the read")
			}
		})
	}
}

// TestDumpStopServesReadsDuringInputCleanup checks new reads during blocked cleanup.
func TestDumpStopServesReadsDuringInputCleanup(t *testing.T) {
	app, started, unblock, readDone := startBlockedShutdownRead(t)
	<-started
	inputStarted, releaseInput := make(chan struct{}), make(chan struct{})
	var once sync.Once
	release := func() { once.Do(func() { close(releaseInput) }) }
	t.Cleanup(release)
	app.FlushTraces = func() { close(inputStarted); <-releaseInput }
	stopped := make(chan error, 1)
	go func() { stopped <- app.DumpStop() }()
	<-inputStarted
	// A new read must still be accepted while unrelated input cleanup runs.
	if err := readShutdownResponse("http://" + app.Config.Carbonserver.Listen + "/admin/info?scopes=quick"); err != nil {
		t.Error(err)
	}
	release()
	unblock()
	if err := <-readDone; err != nil {
		t.Error(err)
	}
	if err := <-stopped; err != nil {
		t.Fatal(err)
	}
}

func startBlockedShutdownRead(t *testing.T) (*App, <-chan struct{}, func(), <-chan error) {
	t.Helper()
	app := New("")
	app.Config = NewConfig()
	app.Config.Dump.Enabled = true
	app.Config.Dump.Path = t.TempDir()
	app.Cache = cache.New()
	cs := carbonserver.NewCarbonserverListener(app.Cache.Get)
	app.Carbonserver = cs
	started, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	t.Cleanup(func() { unblock(); cs.Stop() })
	cs.RegisterInternalInfoHandler("blocked", func() map[string]interface{} {
		close(started)
		<-release
		return map[string]interface{}{"complete": true}
	})
	cs.RegisterInternalInfoHandler("quick", func() map[string]interface{} {
		return map[string]interface{}{"complete": true}
	})
	reserved, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := reserved.Addr().String()
	app.Config.Carbonserver.Listen = addr
	reserved.Close()
	if err := cs.Listen(addr); err != nil {
		t.Fatal(err)
	}
	readDone := make(chan error, 1)
	go func() { readDone <- readShutdownResponse("http://" + addr + "/admin/info?scopes=blocked") }()
	return app, started, unblock, readDone
}

func readShutdownResponse(url string) error {
	client := &http.Client{Timeout: 15 * time.Second}
	res, err := client.Get(url)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	body, err := io.ReadAll(res.Body)
	if err != nil {
		return err
	}
	if res.StatusCode != http.StatusOK || !strings.Contains(string(body), `"complete":true`) {
		return fmt.Errorf("read interrupted: status=%d body=%s", res.StatusCode, body)
	}
	return nil
}
