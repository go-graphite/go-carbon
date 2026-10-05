package carbon

import (
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
	for _, dump := range []bool{false, true} {
		name := "Stop"
		if dump {
			name = "DumpStop"
		}
		t.Run(name, func(t *testing.T) {
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
			defer unblock()
			cs.RegisterInternalInfoHandler("blocked", func() map[string]interface{} {
				close(started)
				<-release
				return map[string]interface{}{"complete": true}
			})
			reserved, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			addr := reserved.Addr().String()
			reserved.Close()
			if err := cs.Listen(addr); err != nil {
				t.Fatal(err)
			}
			defer func() { unblock(); cs.Stop() }()
			client := &http.Client{Timeout: 15 * time.Second}
			readDone := make(chan error, 1)
			go func() {
				res, err := client.Get("http://" + addr + "/admin/info?scopes=blocked")
				if err == nil {
					defer res.Body.Close()
					var body []byte
					body, err = io.ReadAll(res.Body)
					if res.StatusCode != http.StatusOK || !strings.Contains(string(body), `"complete":true`) {
						t.Errorf("read interrupted: status=%d body=%s", res.StatusCode, body)
					}
				}
				readDone <- err
			}()
			select {
			case <-started:
			case <-time.After(5 * time.Second):
				t.Fatal("read did not start")
			}
			stopped := make(chan error, 1)
			go func() {
				if dump {
					stopped <- app.DumpStop()
				} else {
					app.Stop()
					stopped <- nil
				}
			}()
			// DumpStop has a five-second timeout for stopping inputs. An active
			// query must get the read server's own grace period, not that timeout.
			wait := 100 * time.Millisecond
			if dump {
				wait = 5500 * time.Millisecond
			}
			select {
			case err := <-stopped:
				unblock()
				<-readDone
				t.Fatalf("shutdown returned with an active read: %v", err)
			case <-time.After(wait):
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
