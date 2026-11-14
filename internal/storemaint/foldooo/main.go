// Package foldooo scans or folds sidecars through the running daemon, sharing
// its per-metric locks and atomic store batches. It never opens Pebble itself.
package foldooo

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"math"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"syscall"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/internal/storemaint/remote"
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

func Main(args []string) error {
	fs := flag.NewFlagSet("storectl maintain fold-ooo", flag.ContinueOnError)
	secret := fs.String("secret-file", "", "buckyd auth secret file")
	var o options
	fs.StringVar(&o.endpoint, "url", "http://127.0.0.1:4242", "buckyd URL")
	fs.StringVar(&o.checkpoint, "checkpoint", "", "resumable checkpoint file")
	fs.BoolVar(&o.dry, "dry-run", false, "only count, without changing the store or checkpoint")
	fs.BoolVar(&o.scan, "scan", false, "only scan and report sidecars")
	fs.IntVar(&o.limit, "batch", 1000, "metrics per request (1..10000)")
	fs.Float64Var(&o.rate, "rate", 0, "folds per second; 0 runs as fast as possible")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 0 {
		return errors.New("unexpected positional arguments")
	}
	client, err := remote.New(o.endpoint, *secret, !o.dry)
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	return run(ctx, client, o)
}
func request(ctx context.Context, client *remote.Client, endpoint string, q url.Values, result any) error {
	return client.DoContext(ctx, http.MethodPost, "/v1/maintenance/sidecars?"+q.Encode(), nil, result)
}
func saveCheckpoint(path string, c checkpoint) error {
	if path == "" {
		return nil
	}
	value, err := json.Marshal(c)
	if err != nil {
		return err
	}
	f, err := os.CreateTemp(filepath.Dir(path), ".fold-ooo-*")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	if _, err = f.Write(value); err != nil {
		f.Close()
		return err
	}
	if err = f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err = f.Close(); err != nil {
		return err
	}
	if err = os.Rename(f.Name(), path); err != nil {
		return err
	}
	d, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
func run(ctx context.Context, client *remote.Client, o options) error {
	if o.checkpoint != "" && !o.dry && !o.scan {
		lock, err := os.OpenFile(o.checkpoint+".lock", os.O_CREATE|os.O_RDWR, 0600)
		if err != nil {
			return err
		}
		defer lock.Close()
		if err = syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
			return err
		}
	}
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
		var inventory store.SidecarInventory
		if err := client.DoContext(ctx, http.MethodGet, "/v1/maintenance/sidecars", nil, &inventory); err != nil {
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
		if !page.Done && page.After <= c.After {
			return errors.New("sidecar scan cursor did not advance")
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
		if !page.Done && page.After <= c.After {
			return errors.New("sidecar fold cursor did not advance")
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
