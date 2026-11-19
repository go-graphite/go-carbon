package carbon

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/rand/v2"
	"net"
	"net/http"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/internal/v2index"
)

func TestV2SlowCatalogPublicationDoesNotBlockHTTP(t *testing.T) {
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
	stamp := time.Now().Unix() - 120
	fmt.Fprintf(c, "published.metric 17.25 %d\n", stamp)
	c.Close()
	base := "http://" + app.Carbonserver.HTTPListener().Addr().String()
	// The maintenance loop is the catalog's single writer. Telemetry counts
	// its publication after Apply returned; with no other name queued the loop
	// then stays idle, so this test may take the writer role below.
	deadline := time.Now().Add(2200 * time.Millisecond)
	for app.v2.catalog.Snapshot().Len() == 0 || v2CatalogPublications(t, base) == 0 {
		if time.Now().After(deadline) {
			t.Fatal("new name not published within 2.2s")
		}
		time.Sleep(10 * time.Millisecond)
	}
	entered, release, done := make(chan struct{}), make(chan struct{}), make(chan error, 1)
	go func() {
		done <- app.v2.catalog.ApplyBeforePublish(app.v2.catalog.Snapshot().Revision()+1, []string{"not.published"}, nil, func(*v2index.Snapshot) error { close(entered); <-release; return nil })
	}()
	<-entered
	defer func() {
		close(release)
		if err := <-done; err != nil {
			t.Error(err)
		}
	}()
	client := &http.Client{Timeout: 500 * time.Millisecond}
	var wg sync.WaitGroup
	for i := 0; i < 48; i++ {
		wg.Go(func() {
			paths := []string{"/metrics/find/?format=json&query=published.*", fmt.Sprintf("/render/?format=json&target=published.metric&from=%d&until=%d", stamp-60, stamp+60), "/info/?format=json&target=published.metric"}
			for _, path := range paths {
				response, err := client.Get(base + path)
				if err != nil {
					t.Error(err)
					return
				}
				io.Copy(io.Discard, response.Body)
				response.Body.Close()
				if response.StatusCode != 200 {
					t.Errorf("%s: %d", path, response.StatusCode)
				}
			}
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			req, _ := http.NewRequestWithContext(ctx, http.MethodGet, base+paths[0], nil)
			if response, err := client.Do(req); err == nil {
				response.Body.Close()
				t.Error("canceled request succeeded")
			}
		})
	}
	wg.Wait()
	// A second wave after canceled clients must still complete while the same
	// publication remains blocked. There is no refresh mutex or waiter queue.
	response, err := client.Get(base + "/metrics/find/?format=json&query=published.*")
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
}

func v2CatalogPublications(t *testing.T, base string) uint64 {
	t.Helper()
	response, err := http.Get(base + "/admin/info?scopes=v2-catalog")
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	var info struct {
		Catalog struct{ Publications uint64 } `json:"v2-catalog"`
	}
	if err := json.NewDecoder(response.Body).Decode(&info); err != nil {
		t.Fatal(err)
	}
	return info.Catalog.Publications
}

func TestV2IncrementalCatalogMatchesDiscovery(t *testing.T) {
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
	rng := rand.New(rand.NewPCG(16, 1201))
	for i := 0; i < 500; i++ {
		fmt.Fprintf(c, "random.svc%d.host%d.metric%d %d %d\n", rng.IntN(9), rng.IntN(17), rng.IntN(7), i, time.Now().Unix()-120)
	}
	c.Close()
	deadline := time.Now().Add(2200 * time.Millisecond)
	for {
		names, _, err := app.v2.reader.NamesWithMasks(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		published := slices.Collect(app.v2.catalog.Snapshot().Names())
		if len(names) > 0 && slices.Equal(names, published) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("full discovery %d versus published %d", len(names), len(published))
		}
		time.Sleep(10 * time.Millisecond)
	}
}
