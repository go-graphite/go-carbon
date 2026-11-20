package carbon

import (
	"errors"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/internal/v2read"
)

func TestV2DiscoveryRefreshInBackground(t *testing.T) {
	if isolatedTestProcess(t) {
		return
	}
	app := v2TestApp(t, t.TempDir())
	if err := app.Start(); err != nil {
		t.Fatal(err)
	}
	defer app.Stop()
	c, err := net.Dial("tcp", app.v2.server.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	fmt.Fprintf(c, "lazy.name 1 %d\n", time.Now().Unix())
	c.Close()
	deadline := time.Now().Add(5 * time.Second)
	for app.v2.server.Stats().Accepted == 0 {
		if time.Now().After(deadline) {
			t.Fatal("no admission")
		}
		time.Sleep(10 * time.Millisecond)
	}
	// New names publish without a discovery request.
	time.Sleep(1200 * time.Millisecond)
	if app.v2.catalog.Catalog.Snapshot().Len() != 1 {
		t.Fatal("background catalog missed publication")
	}
	if b := v2HTTP(t, app, "/metrics/find/?format=json&query=lazy.*"); !strings.Contains(string(b), "lazy.name") {
		t.Fatalf("next read missed publication: %s", b)
	}
}

// A stale candidate can be discovered while fresh input is still in the decoder
// or journal. Concurrent owner fences must recheck the applied activity clock.
func TestV2ExpiryConcurrentIntake(t *testing.T) {
	if isolatedTestProcess(t) {
		return
	}
	app := v2TestApp(t, t.TempDir())
	app.Config.Whisper.StoreExpiration = &Duration{2 * time.Second}
	if err := app.Start(); err != nil {
		t.Fatal(err)
	}
	defer app.Stop()
	c, err := net.Dial("tcp", app.v2.server.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(20 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
				if _, e := fmt.Fprintf(c, "expiry.same 1 %d\n", time.Now().Unix()); e != nil {
					return
				}
			}
		}
	}()
	defer func() { close(stop); <-done }()
	deadline := time.Now().Add(5 * time.Second)
	for app.v2.server.Stats().Accepted == 0 {
		if time.Now().After(deadline) {
			t.Fatal("no admission")
		}
		time.Sleep(10 * time.Millisecond)
	}
	// Acceptance precedes journal sync/publication. Wait for the first visible
	// point before checking that maintenance cannot remove the active name.
	for {
		_, e := app.v2.reader.Metadata(t.Context(), "expiry.same")
		if e == nil {
			break
		}
		if !errors.Is(e, v2read.ErrNotFound) {
			t.Fatal(e)
		}
		if time.Now().After(deadline) {
			t.Fatal("no visible admission")
		}
		time.Sleep(10 * time.Millisecond)
	}
	// Cross multiple inactivity windows while fresh points race maintenance.
	until := time.Now().Add(4 * time.Second)
	for time.Now().Before(until) {
		if _, e := app.v2.reader.Metadata(t.Context(), "expiry.same"); errors.Is(e, v2read.ErrNotFound) {
			t.Fatal("active name expired during intake")
		} else if e != nil {
			t.Fatal(e)
		}
		time.Sleep(20 * time.Millisecond)
	}
}
