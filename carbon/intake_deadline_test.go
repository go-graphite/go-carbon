package carbon

import (
	"fmt"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/cache"
	"github.com/go-graphite/go-carbon/internal/recovery"
)

// Opt-in scale gate: includes the real tail admission path, receiver startup,
// TCP parsing, and visibility through the same cache the read API uses. Saved
// read-index warmup and dump creation precede this boundary and are not timed.
func TestLargeTailIntakeWithinTenSeconds(t *testing.T) {
	testTailIntakeWithinTenSeconds(t, 16000000, 1000000, 11, 0)
}

// Use longer names and more saved history per active metric as well as the
// original high-record-count case. Both fixtures are synthetic, not production
// captures, and leave checkpoint/index warmup outside the measured interval.
func TestPopulatedTailIntakeWithinTenSeconds(t *testing.T) {
	testTailIntakeWithinTenSeconds(t, 9000000, 3000000, 32, 100)
}

func testTailIntakeWithinTenSeconds(t *testing.T, records, metrics, history, padding int) {
	t.Helper()
	if os.Getenv("GO_CARBON_INTAKE_DEADLINE") != "1" {
		t.Skip("opt-in large-tail scale test")
	}
	app, path := intakeTailFixture(t, records, metrics, true, history, padding)
	dir := filepath.Dir(path)
	bundle, err := recovery.OpenBundle(dir, dir)
	if err != nil {
		t.Fatal(err)
	}
	defer bundle.Close()
	core := cache.New()
	core.SetMaxSize(app.Config.Cache.MaxSize)
	if err := core.AttachPendingRecovery(bundle); err != nil {
		t.Fatal(err)
	}
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	app.Config.Tcp.Listen = l.Addr().String()
	l.Close()
	app.Config.Tcp.Enabled = true
	app.Config.Udp.Enabled = false
	app.Config.Pickle.Enabled = false
	app.Config.Grpc.Enabled = false
	app.Config.Prometheus.Enabled = false
	started := time.Now()
	if err := app.startReceivers(core); err != nil {
		t.Fatal(err)
	}
	t.Logf("tail replay and receiver startup completed in %s", time.Since(started))
	defer func() {
		for _, r := range app.Receivers {
			r.Stop()
		}
	}()
	conn, err := net.DialTimeout("tcp", app.Config.Tcp.Listen, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if _, err := fmt.Fprint(conn, "fresh.marker 1 1700001000\nfresh.marker 2 1700001000\n"); err != nil {
		t.Fatal(err)
	}
	for {
		data := core.Get("fresh.marker")
		if len(data) == 2 {
			if data[0].Value != 1 || data[1].Value != 2 {
				t.Fatal("fresh correction reordered", data)
			}
			break
		}
		if time.Since(started) > 10*time.Second {
			t.Fatalf("fresh TCP input not readable within 10 seconds (elapsed %s)", time.Since(started))
		}
		time.Sleep(time.Millisecond)
	}
	elapsed := time.Since(started)
	t.Logf("%d tail points, %d checkpoint metrics: fresh TCP point readable in %s", records, metrics, elapsed)
	if elapsed >= 10*time.Second {
		t.Fatalf("intake deadline exceeded: %s", elapsed)
	}
	if got := core.Size(); got != int64(records+metrics*history+2) {
		t.Fatalf("recovery point count = %d", got)
	}
}
