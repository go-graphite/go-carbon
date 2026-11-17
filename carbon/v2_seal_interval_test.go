package carbon

import (
	"encoding/json"
	"fmt"
	"math"
	"net"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/persister"
)

func TestV2SealIntervalPendingHTTP(t *testing.T) {
	if isolatedTestProcess(t) {
		return
	}
	app := v2TestApp(t, t.TempDir())
	app.Config.V2.SealInterval = &Duration{10 * time.Second}
	ret, err := persister.ParseRetentionDefs("1s:1m,10s:1h")
	if err != nil {
		t.Fatal(err)
	}
	app.Config.Whisper.Schemas[0].Retentions = ret
	if err = app.Start(); err != nil {
		t.Fatal(err)
	}
	defer app.Stop()
	c, err := net.Dial("tcp", app.v2.server.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	now := time.Now().Unix()
	stamp := now - now%10 - 20
	for round := 0; round < 2; round++ {
		begin := time.Now()
		for i := 0; i < 10; i++ {
			if _, err = fmt.Fprintf(c, "seal.pending %d %d\n", i+round*10, stamp+int64(i)); err != nil {
				t.Fatal(err)
			}
		}
		for {
			if _, e := app.v2.reader.Metadata(t.Context(), "seal.pending"); e != nil {
				if time.Since(begin) > 2200*time.Millisecond {
					t.Fatal("pending metadata visibility >2.2s", e)
				}
				time.Sleep(10 * time.Millisecond)
				continue
			}
			if !slices.Contains(slices.Collect(app.v2.catalog.Snapshot().Names()), "seal.pending") {
				if time.Since(begin) > 2200*time.Millisecond {
					t.Fatal("pending catalog visibility >2.2s")
				}
				time.Sleep(10 * time.Millisecond)
				continue
			}
			fine, coarse := false, false
			for _, span := range []int64{10, 120} {
				b := v2HTTP(t, app, fmt.Sprintf("/render/?format=json&target=seal.pending&from=%d&until=%d", stamp+9-span, stamp+9))
				var body struct {
					Metrics []struct {
						Values []float64 `json:"values"`
						Absent []bool    `json:"isAbsent"`
						Step   int       `json:"stepTime"`
					} `json:"metrics"`
				}
				if err = json.Unmarshal(b, &body); err != nil {
					t.Fatal(err)
				}
				if len(body.Metrics) != 1 {
					continue
				}
				m := body.Metrics[0]
				count := 0
				correct := true
				for i, v := range m.Values {
					if m.Absent[i] {
						continue
					}
					count++
					want := 4.5 + float64(round*10)
					if span == 10 {
						want = float64(i + round*10)
					}
					if math.Float64bits(v) != math.Float64bits(want) {
						correct = false
					}
				}
				if span == 10 {
					fine = correct && count == 10 && m.Step == 1
				} else {
					coarse = correct && count == 1 && m.Step == 10
				}
			}
			if fine && coarse {
				if b := v2HTTP(t, app, "/metrics/find/?format=json&query=seal.*"); !strings.Contains(string(b), "seal.pending") {
					t.Fatalf("find %s", b)
				}
				if b := v2HTTP(t, app, "/info/?format=json&target=seal.pending"); !strings.Contains(strings.ToLower(string(b)), "average") {
					t.Fatalf("info %s", b)
				}
				break
			}
			if time.Since(begin) > 2200*time.Millisecond {
				t.Fatalf("round%d pending HTTP visibility >2.2s fine=%v coarse=%v", round, fine, coarse)
			}
			time.Sleep(10 * time.Millisecond)
		}
		if obs := app.v2.store.Core.Observe(); obs.Seals != 0 {
			t.Fatalf("pending test sealed: %+v", obs)
		}
		t.Logf("round%d append through HTTP visible in %s", round, time.Since(begin))
	}
}
