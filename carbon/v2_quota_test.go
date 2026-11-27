package carbon

import (
	"errors"
	"fmt"
	"net"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/internal/v2read"
	"github.com/go-graphite/go-carbon/persister"
	"github.com/go-graphite/go-whisper"
)

func TestV2TCPUnionQuotaRestartExpiry(t *testing.T) {
	if isolatedTestProcess(t) {
		return
	}
	root := t.TempDir()
	setup := func() *App {
		app := v2TestApp(t, root)
		rets, err := persister.ParseRetentionDefs("1s:1h")
		if err != nil {
			t.Fatal(err)
		}
		app.Config.Whisper.Schemas[0].Retentions = rets
		app.Config.Whisper.StoreExpiration = &Duration{4 * time.Second}
		app.Config.Whisper.Quotas = persister.WhisperQuotas{{Pattern: "/", Metrics: 2, DroppingPolicy: "new"}}
		return app
	}
	app := setup()
	stamp := time.Now().Unix() - 30
	classic, err := whisper.Create(filepath.Join(app.Config.Whisper.DataDir, "historic.wsp"), app.Config.Whisper.Schemas[0].Retentions, whisper.Average, .5)
	if err != nil {
		t.Fatal(err)
	}
	if err = classic.UpdateMany([]*whisper.TimeSeriesPoint{{Time: int(stamp), Value: 3.25}}); err != nil {
		t.Fatal(err)
	}
	if err = classic.Close(); err != nil {
		t.Fatal(err)
	}
	if err = app.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if app != nil {
			app.Stop()
		}
	}()
	send := func(text string, keep bool) net.Conn {
		t.Helper()
		c, err := net.Dial("tcp", app.v2.server.Addr().String())
		if err != nil {
			t.Fatal(err)
		}
		if _, err = fmt.Fprint(c, text); err != nil {
			t.Fatal(err)
		}
		if !keep {
			c.Close()
		}
		return c
	}
	await := func(check func() bool) {
		t.Helper()
		deadline := time.Now().Add(10 * time.Second)
		for !check() {
			if time.Now().After(deadline) {
				t.Fatal("runtime state did not converge")
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
	// Leave the sender open across shutdown, as a relay is during a rolling restart.
	sender := send(fmt.Sprintf("live 11 %d\n", stamp), true)
	// Owners admit independently. Establish the last reservation before sending
	// a name routed to another owner; socket order is not cross-owner order.
	await(func() bool {
		_, err := app.v2.reader.Metadata(t.Context(), "live")
		return err == nil
	})
	if _, err := fmt.Fprintf(sender, "blocked 99 %d\n", stamp); err != nil {
		t.Fatal(err)
	}
	await(func() bool { return app.v2.quotas.Rejected() > 0 })
	// Complete queued intake before checking that refusal left no journal state.
	if err := app.v2.server.Fence(func() error { return nil }); err != nil {
		t.Fatal(err)
	}
	if _, err := app.v2.reader.Metadata(t.Context(), "blocked"); !errors.Is(err, v2read.ErrNotFound) {
		t.Fatalf("rejected name journaled: %v", err)
	}
	before := v2HTTP(t, app, fmt.Sprintf("/render/?format=json&target=historic&from=%d&until=%d", stamp-1, stamp))
	if !strings.Contains(string(before), "3.25") {
		t.Fatalf("classic fixture missing %s", before)
	}
	app.Stop()
	app = nil
	sender.Close()
	app = setup()
	if err := app.Start(); err != nil {
		t.Fatal(err)
	}
	// Rejected input must also stay absent from the recovered journal.
	if _, err := app.v2.reader.Metadata(t.Context(), "blocked"); !errors.Is(err, v2read.ErrNotFound) {
		t.Fatalf("rejected name recovered: %v", err)
	}
	// The received live point survives the restart, then inactivity expires it.
	// A full host quota initially prevented 'blocked'; retirement releases its slot.
	await(func() bool {
		masked, _ := app.v2.reader.Masked(t.Context(), "live")
		return masked && app.v2.catalog.Snapshot().Len() == 1
	})
	send(fmt.Sprintf("blocked 17 %d\n", stamp+1), false)
	await(func() bool {
		s, err := app.v2.reader.Fetch(t.Context(), "blocked", int(stamp), int(stamp+1))
		return err == nil && s != nil && len(s.Values()) == 1 && s.Values()[0] == 17
	})
	await(func() bool { return app.v2.catalog.Snapshot().Len() == 2 })
	after := v2HTTP(t, app, fmt.Sprintf("/render/?format=json&target=historic&from=%d&until=%d", stamp-1, stamp))
	if string(before) != string(after) {
		t.Fatalf("classic changed across expiry/restart:\n%s\n%s", before, after)
	}
	if b := v2HTTP(t, app, "/metrics/find/?format=json&query=*"); !strings.Contains(string(b), "blocked") || !strings.Contains(string(b), "historic") || strings.Contains(string(b), "live") {
		t.Fatalf("quota/expiry find %s", b)
	}
}
