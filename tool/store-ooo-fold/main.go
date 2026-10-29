// store-ooo-fold scans or folds sidecars through the running daemon, sharing
// its per-metric locks and atomic store batches. It never opens Pebble itself.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
)

type checkpoint struct {
	URL   string `json:"url"`
	Phase string `json:"phase"`
	After string `json:"after"`
}
type options struct {
	endpoint, checkpoint string
	dry, scan            bool
	limit                int
	rate                 float64
}

func main() {
	var o options
	flag.StringVar(&o.endpoint, "url", "http://127.0.0.1:8080", "carbonserver URL")
	flag.StringVar(&o.checkpoint, "checkpoint", "", "resumable checkpoint file")
	flag.BoolVar(&o.dry, "dry-run", false, "only count, without changing the store or checkpoint")
	flag.BoolVar(&o.scan, "scan", false, "only scan and report sidecars")
	flag.IntVar(&o.limit, "batch", 1000, "metrics per request (1..10000)")
	flag.Float64Var(&o.rate, "rate", 0, "folds per second; 0 runs as fast as possible")
	flag.Parse()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	err := run(ctx, &http.Client{}, o)
	stop()
	if err != nil {
		fmt.Fprintln(os.Stderr, "store-ooo-fold:", err)
		os.Exit(1)
	}
}
func request(ctx context.Context, client *http.Client, endpoint string, q url.Values, result any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(endpoint, "/")+"/admin/store/sidecars?"+q.Encode(), http.NoBody)
	if err != nil {
		return err
	}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("%s: %s: %s", q.Get("action"), resp.Status, strings.TrimSpace(string(body)))
	}
	return json.NewDecoder(resp.Body).Decode(result)
}
func saveCheckpoint(path string, c checkpoint) error {
	if path == "" {
		return nil
	}
	value, err := json.Marshal(c)
	if err != nil {
		return err
	}
	if err := os.WriteFile(path+".tmp", value, 0600); err != nil {
		return err
	}
	return os.Rename(path+".tmp", path)
}
func run(ctx context.Context, client *http.Client, o options) error {
	if o.limit < 1 || o.limit > 10000 || o.rate < 0 || math.IsNaN(o.rate) || math.IsInf(o.rate, 0) {
		return errors.New("invalid batch or rate")
	}
	c := checkpoint{URL: o.endpoint, Phase: "scan"}
	if o.checkpoint != "" && !o.dry && !o.scan {
		if value, err := os.ReadFile(o.checkpoint); err == nil {
			if err := json.Unmarshal(value, &c); err != nil {
				return err
			}
			if c.URL != o.endpoint {
				return errors.New("checkpoint belongs to a different URL")
			}
			if c.Phase != "scan" && c.Phase != "fold" {
				return errors.New("invalid checkpoint phase")
			}
			// A partial scan cannot establish inventory readiness across a restart.
			if c.Phase == "scan" {
				c.After = ""
			}
		} else if !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	if !o.dry && !o.scan && c.Phase == "scan" {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimRight(o.endpoint, "/")+"/admin/store/sidecars", http.NoBody)
		if err != nil {
			return err
		}
		resp, err := client.Do(req)
		if err != nil {
			return err
		}
		var inventory store.SidecarInventory
		err = json.NewDecoder(resp.Body).Decode(&inventory)
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("inventory: %s", resp.Status)
		}
		if err != nil {
			return err
		}
		if inventory.Ready {
			c.Phase = "fold"
			c.After = ""
		}
	}
	// Dry runs always inspect the complete physical catalog without publishing
	// inventory records. They work even before a legacy store's initial scan.
	total, points := 0, uint64(0)
	for c.Phase == "scan" {
		q := url.Values{"action": {"scan"}, "after": {c.After}, "limit": {strconv.Itoa(o.limit)}, "dry-run": {strconv.FormatBool(o.dry)}}
		var page store.SidecarPage
		if err := request(ctx, client, o.endpoint, q, &page); err != nil {
			return err
		}
		total += len(page.Sidecars)
		for _, s := range page.Sidecars {
			points += s.Points
			if o.scan {
				value, _ := json.Marshal(s)
				fmt.Println(string(value))
			}
		}
		c.After = page.After
		if page.Done {
			c.Phase = "fold"
			c.After = ""
		}
		if !o.dry && !o.scan {
			if err := saveCheckpoint(o.checkpoint, c); err != nil {
				return err
			}
		}
	}
	if o.scan || o.dry {
		fmt.Printf("sidecars=%d points=%d dry-run=%v\n", total, points, o.dry)
		return nil
	}
	for {
		q := url.Values{"action": {"fold"}, "after": {c.After}, "limit": {strconv.Itoa(o.limit)}, "rate": {strconv.FormatFloat(o.rate, 'g', -1, 64)}}
		started := time.Now()
		var page store.SidecarFoldPage
		if err := request(ctx, client, o.endpoint, q, &page); err != nil {
			return err
		}
		value, _ := json.Marshal(page)
		fmt.Println(string(value))
		c.After = page.After
		// Restarting a completed checkpoint runs a fresh pass; a sidecar created
		// behind the cursor during this pass must not be hidden by its checkpoint.
		if page.Done {
			c.After = ""
		}
		if err := saveCheckpoint(o.checkpoint, c); err != nil {
			return err
		}
		if page.Done && page.Inventory.Count == 0 {
			return nil
		}
		if o.rate > 0 {
			budget := time.Duration(math.Min(float64(page.Count)/o.rate*float64(time.Second), float64(math.MaxInt64/2)))
			timer := time.NewTimer(time.Until(started.Add(budget)))
			select {
			case <-ctx.Done():
				timer.Stop()
				return ctx.Err()
			case <-timer.C:
			}
		}
	}
}
