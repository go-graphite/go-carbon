package carbon

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/persister"
	"github.com/go-graphite/go-whisper"
)

func TestV2UnionClassicTCPRestart(t *testing.T) {
	if isolatedTestProcess(t) {
		return
	}
	for _, backend := range []string{"whisper", "pebble-chunk"} {
		t.Run(backend, func(t *testing.T) {
			root := t.TempDir()
			setup := func() *App {
				app := v2TestApp(t, root)
				retentions, err := persister.ParseRetentionDefs("60s:1d")
				if err != nil {
					t.Fatal(err)
				}
				app.Config.Whisper.Schemas[0].Retentions = retentions
				app.Config.Whisper.StorageBackend = backend
				app.Config.Whisper.StoreDir = filepath.Join(root, "classic")
				app.Config.Whisper.StoreEncoding = "exact"
				return app
			}
			app := setup()
			now := time.Now().Unix()
			stamp := now - now%60 - 180
			if backend == "whisper" {
				for _, name := range []string{"union.old", "union.overlap"} {
					path := filepath.Join(app.Config.Whisper.DataDir, "union", strings.TrimPrefix(name, "union.")+".wsp")
					if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
						t.Fatal(err)
					}
					w, err := whisper.Create(path, app.Config.Whisper.Schemas[0].Retentions, whisper.Average, .5)
					if err != nil {
						t.Fatal(err)
					}
					if err = w.UpdateMany([]*whisper.TimeSeriesPoint{{Time: int(stamp), Value: math.Copysign(0, -1)}, {Time: int(stamp + 60), Value: 2}}); err != nil {
						t.Fatal(err)
					}
					if err = w.Close(); err != nil {
						t.Fatal(err)
					}
				}
			} else {
				s, err := store.Open(app.Config.Whisper.StoreDir, store.Options{ExactBlocks: true})
				if err != nil {
					t.Fatal(err)
				}
				for _, name := range []string{"union.old", "union.overlap"} {
					_, err = s.Create(context.Background(), store.MetricConfig{Name: name, Retentions: []store.Retention{{Step: 60, Count: 1440}}, AggregationMethod: store.Average, XFilesFactor: .5})
					if err != nil {
						t.Fatal(err)
					}
					if err = s.UpdateMany(context.Background(), name, []store.Point{{Timestamp: stamp, Value: math.Copysign(0, -1)}, {Timestamp: stamp + 60, Value: 2}}); err != nil {
						t.Fatal(err)
					}
				}
				if err = s.Close(); err != nil {
					t.Fatal(err)
				}
			}
			if err := app.Start(); err != nil {
				t.Fatal(err)
			}
			defer func() {
				if app != nil {
					app.Stop()
				}
			}()
			// Existing history and discovery must work before the first v2 admission.
			waitV2CatalogName(t, app, "union.old")
			if b := v2HTTP(t, app, "/metrics/find/?format=json&query=union.*"); !strings.Contains(string(b), "union.old") {
				t.Fatalf("classic startup discovery: %s", b)
			}
			conn, err := net.Dial("tcp", app.v2.server.Addr().String())
			if err != nil {
				t.Fatal(err)
			}
			_, err = fmt.Fprintf(conn, "union.overlap 11 %d\nunion.new 7 %d\n", stamp+60, stamp+120)
			if err != nil {
				t.Fatal(err)
			}
			conn.Close()
			deadline := time.Now().Add(8 * time.Second)
			for {
				names, err := app.v2.reader.Names(t.Context())
				if err != nil {
					t.Fatal(err)
				}
				if len(names) == 2 && app.v2.catalog.Snapshot().Len() == 3 {
					break
				}
				if time.Now().After(deadline) {
					t.Fatalf("union publication: names=%v", names)
				}
				time.Sleep(10 * time.Millisecond)
			}
			check := func() {
				t.Helper()
				for _, name := range []string{"union.old", "union.overlap", "union.new"} {
					waitV2CatalogName(t, app, name)
				}
				for _, name := range []string{"union.old", "union.overlap", "union.new"} {
					v2HTTP(t, app, "/info/?format=json&target="+name)
					b := v2HTTP(t, app, fmt.Sprintf("/render/?format=json&target=%s&from=%d&until=%d", name, stamp-60, stamp+120))
					var body struct {
						Metrics []struct {
							Values   []float64 `json:"values"`
							IsAbsent []bool    `json:"isAbsent"`
						} `json:"metrics"`
					}
					if err := json.Unmarshal(b, &body); err != nil {
						t.Fatal(err)
					}
					if len(body.Metrics) != 1 || len(body.Metrics[0].Values) != 3 {
						t.Fatalf("shape %s", b)
					}
					values := body.Metrics[0].Values
					absent := body.Metrics[0].IsAbsent
					if name == "union.new" {
						if !absent[0] || !absent[1] || absent[2] || values[2] != 7 {
							t.Fatalf("new: %s", b)
						}
					} else {
						want := float64(2)
						if name == "union.overlap" {
							want = 11
						}
						if absent[0] || math.Float64bits(values[0]) != math.Float64bits(math.Copysign(0, -1)) || absent[1] || values[1] != want {
							t.Fatalf("combined %s: %s", name, b)
						}
					}
				}
				if b := v2HTTP(t, app, "/metrics/find/?format=json&query=union.*"); !strings.Contains(string(b), "union.new") || !strings.Contains(string(b), "union.old") {
					t.Fatalf("union find: %s", b)
				}
			}
			check()
			app.Stop()
			app = nil
			app = setup()
			if err := app.Start(); err != nil {
				t.Fatal(err)
			}
			check()
		})
	}
}

func TestV2UnionExpiryDoesNotResurrectClassic(t *testing.T) {
	if isolatedTestProcess(t) {
		return
	}
	for _, early := range []bool{false, true} {
		t.Run(fmt.Sprintf("revive-before-hydration=%v", early), func(t *testing.T) { testV2UnionExpiryReset(t, early) })
	}
}

// testV2UnionExpiryReset revives an expired name in a later incarnation. With
// early, the reviving point arrives while every owner's hydration is held.
func testV2UnionExpiryReset(t *testing.T, early bool) {
	root := t.TempDir()
	setup := func() *App {
		app := v2TestApp(t, root)
		retentions, err := persister.ParseRetentionDefs("1s:1h")
		if err != nil {
			t.Fatal(err)
		}
		app.Config.Whisper.Schemas[0].Retentions = retentions
		app.Config.Whisper.StoreExpiration = &Duration{5 * time.Second}
		return app
	}
	app := setup()
	stamp := time.Now().Unix() - 10
	for _, name := range []string{"old", "overlap"} {
		path := filepath.Join(app.Config.Whisper.DataDir, name+".wsp")
		w, err := whisper.Create(path, app.Config.Whisper.Schemas[0].Retentions, whisper.Average, .5)
		if err != nil {
			t.Fatal(err)
		}
		if err = w.UpdateMany([]*whisper.TimeSeriesPoint{{Time: int(stamp), Value: 2}}); err != nil {
			t.Fatal(err)
		}
		if err = w.Close(); err != nil {
			t.Fatal(err)
		}
	}
	if err := app.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if app != nil {
			app.Stop()
		}
	}()
	conn, err := net.Dial("tcp", app.v2.server.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	if _, err = fmt.Fprintf(conn, "overlap 11 %d\n", stamp); err != nil {
		t.Fatal(err)
	}
	conn.Close()
	deadline := time.Now().Add(10 * time.Second)
	for {
		s, err := app.v2.reader.Fetch(t.Context(), "overlap", int(stamp-1), int(stamp))
		if err == nil && s != nil && len(s.Values()) == 1 && s.Values()[0] == 11 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("initial v2 value not visible: %v", err)
		}
		time.Sleep(5 * time.Millisecond)
	}
	checkExpired := func() {
		t.Helper()
		deadline := time.Now().Add(10 * time.Second)
		for {
			masked, err := app.v2.reader.Masked(t.Context(), "overlap")
			if err != nil {
				t.Fatal(err)
			}
			names := app.v2.catalog.Snapshot()
			if masked && names.Len() == 1 {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("expiry did not mask classic: masked=%v names=%v", masked, names.Len())
			}
			time.Sleep(10 * time.Millisecond)
		}
		if b := v2HTTP(t, app, "/metrics/find/?format=json&query=*"); !strings.Contains(string(b), "old") || strings.Contains(string(b), "overlap") {
			t.Fatalf("expired union names: %s", b)
		}
		b := v2HTTP(t, app, fmt.Sprintf("/render/?format=json&target=old&from=%d&until=%d", stamp-1, stamp))
		if !strings.Contains(string(b), "2") {
			t.Fatalf("classic-only history disappeared: %s", b)
		}
	}
	checkExpired()
	app.Stop()
	app = nil
	app = setup()
	// Reset authority is durable even when expiry is disabled on restart.
	// This also leaves the resumed incarnation a deterministic query window.
	app.Config.Whisper.StoreExpiration = &Duration{}
	release, entered := make(chan struct{}), make(chan struct{}, 1)
	if early {
		app.v2Stage = func(stage string) {
			if stage == "owner-hydration" {
				select {
				case entered <- struct{}{}:
				default:
				}
				<-release
			}
		}
	} else {
		close(release)
	}
	if err := app.Start(); err != nil {
		t.Fatal(err)
	}
	if !early {
		checkExpired()
	}
	// A later incarnation retains the whole-name classic reset barrier.
	conn, err = net.Dial("tcp", app.v2.server.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	if _, err = fmt.Fprintf(conn, "overlap 17 %d\n", stamp+1); err != nil {
		t.Fatal(err)
	}
	conn.Close()
	checkReset := func() {
		t.Helper()
		masked, err := app.v2.reader.Masked(t.Context(), "overlap")
		if err != nil || !masked {
			t.Fatalf("reset barrier lost: masked=%v err=%v", masked, err)
		}
		s, mask, err := app.v2.reader.FetchWithMask(t.Context(), "overlap", int(stamp-1), int(stamp+1))
		if err == nil && (!mask || s == nil || len(s.Values()) != 2 || !math.IsNaN(s.Values()[0])) {
			t.Fatalf("reset barrier lost: mask=%v series=%v", mask, s)
		}
	}
	if early {
		// The point is admitted while hydration is held: the barrier holds.
		select {
		case <-entered:
		case <-time.After(10 * time.Second):
			t.Fatal("owner hydration never started")
		}
		checkReset()
		close(release)
	}
	deadline = time.Now().Add(10 * time.Second)
	for {
		s, mask, err := app.v2.reader.FetchWithMask(t.Context(), "overlap", int(stamp-1), int(stamp+1))
		if err == nil && s != nil && len(s.Values()) == 2 && s.Values()[1] == 17 {
			if !mask || !math.IsNaN(s.Values()[0]) {
				t.Fatalf("reset barrier lost: mask=%v values=%v", mask, s.Values())
			}
			if app.v2.catalog.Snapshot().Len() == 2 {
				break
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("resumed incarnation unavailable: %v", err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	checkRender := func() {
		t.Helper()
		b := v2HTTP(t, app, fmt.Sprintf("/render/?format=json&target=overlap&from=%d&until=%d", stamp-1, stamp+1))
		var body struct {
			Metrics []struct {
				Values   []float64 `json:"values"`
				IsAbsent []bool    `json:"isAbsent"`
			} `json:"metrics"`
		}
		if err := json.Unmarshal(b, &body); err != nil {
			t.Fatal(err)
		}
		if len(body.Metrics) != 1 || len(body.Metrics[0].Values) != 2 || !body.Metrics[0].IsAbsent[0] || body.Metrics[0].IsAbsent[1] || body.Metrics[0].Values[1] != 17 {
			t.Fatalf("classic resurrected after reset: %s", b)
		}
	}
	checkRender()
	if !early {
		return
	}
	// The revived incarnation keeps the barrier across another restart.
	app.Stop()
	app = nil
	app = setup()
	app.Config.Whisper.StoreExpiration = &Duration{}
	if err := app.Start(); err != nil {
		t.Fatal(err)
	}
	checkReset()
	checkRender()
}
