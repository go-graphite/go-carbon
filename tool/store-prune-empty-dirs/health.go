package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"
)

func localURL(raw string) error {
	u, err := url.Parse(raw)
	if err != nil {
		return err
	}
	ip := net.ParseIP(u.Hostname())
	if u.Scheme != "http" || u.User != nil || (u.Hostname() != "localhost" && (ip == nil || !ip.IsLoopback())) {
		return errors.New("only local http endpoints are permitted")
	}
	return nil
}

func get(ctx context.Context, client *http.Client, endpoint string) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, err
	}
	response, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	if response.StatusCode != http.StatusOK {
		response.Body.Close()
		return nil, fmt.Errorf("%s returned %s", endpoint, response.Status)
	}
	return response, nil
}

// isTimeout reports whether a local endpoint request ran out of time, as
// opposed to answering with a refusal, an error status or a malformed body.
func isTimeout(err error) bool {
	var netErr net.Error
	return errors.As(err, &netErr) && netErr.Timeout()
}

// pause holds directory operations while the local gate is unconfirmed.
type pause struct {
	mu      sync.Mutex
	resumed chan struct{} // nil while running
}

func (p *pause) set(paused bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if paused && p.resumed == nil {
		p.resumed = make(chan struct{})
	} else if !paused && p.resumed != nil {
		close(p.resumed)
		p.resumed = nil
	}
}

func (p *pause) wait(ctx context.Context) error {
	p.mu.Lock()
	resumed := p.resumed
	p.mu.Unlock()
	if resumed == nil {
		return nil
	}
	select {
	case <-resumed:
		return nil
	case <-ctx.Done():
		return context.Cause(ctx)
	}
}

func sharedStatus(ctx context.Context, client *http.Client, endpoint string) error {
	response, err := get(ctx, client, endpoint)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	var status struct {
		Storage struct {
			Migration *struct {
				Phase    string `json:"phase"`
				InFlight *int64 `json:"in_flight"`
				Skipped  uint64 `json:"skipped"`
				Error    string `json:"error"`
			} `json:"migration"`
		} `json:"storage"`
	}
	if err := json.NewDecoder(io.LimitReader(response.Body, 4<<20)).Decode(&status); err != nil {
		return err
	}
	m := status.Storage.Migration
	if m == nil || m.Phase != "shared" || m.InFlight == nil || *m.InFlight != 0 || m.Skipped != 0 || m.Error != "" {
		return errors.New("local storage must report migration phase shared, in_flight=0, no skipped sources or error")
	}
	return nil
}

type writes struct{ updates, seconds, failures float64 }

func writeCounters(reader io.Reader) (writes, error) {
	var result writes
	found := make(map[string]bool)
	scanner := bufio.NewScanner(io.LimitReader(reader, 16<<20))
	scanner.Buffer(make([]byte, 4096), 1<<20)
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "#") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}
		name := strings.SplitN(fields[0], "{", 2)[0]
		for key, target := range map[string]*float64{
			"persister_pebble_updates_total":        &result.updates,
			"persister_pebble_write_seconds_total":  &result.seconds,
			"persister_pebble_write_failures_total": &result.failures,
		} {
			if name != key && !strings.HasSuffix(name, "_"+key) {
				continue
			}
			value, err := strconv.ParseFloat(fields[1], 64)
			if err != nil || math.IsNaN(value) || math.IsInf(value, 0) || value < 0 {
				return writes{}, fmt.Errorf("invalid counter %s", key)
			}
			*target += value
			found[key] = true
		}
	}
	if err := scanner.Err(); err != nil {
		return writes{}, err
	}
	if len(found) != 3 {
		return writes{}, errors.New("Pebble persister counters unavailable; auto mode requires Prometheus metrics")
	}
	return result, nil
}

type diskStat struct{ operations, millis, busy float64 }

type controller struct {
	options options
	client  *http.Client
	rate    float64
	status  string
	at      time.Time
	disks   map[string]diskStat
	writes  writes
	healthy int
}

func (c *controller) read(ctx context.Context) (map[string]diskStat, writes, error) {
	disks, err := diskStats(strings.Split(c.options.devices, ","))
	if err != nil {
		return nil, writes{}, err
	}
	response, err := get(ctx, c.client, c.options.metrics)
	if err != nil {
		return nil, writes{}, err
	}
	defer response.Body.Close()
	counters, err := writeCounters(response.Body)
	return disks, counters, err
}

func (c *controller) baseline(ctx context.Context) error {
	var err error
	c.disks, c.writes, err = c.read(ctx)
	c.at = time.Now()
	return err
}

func (c *controller) sample(ctx context.Context) error {
	disks, counters, err := c.read(ctx)
	if err != nil {
		return err
	}
	now := time.Now()
	elapsed := now.Sub(c.at).Seconds()
	var util, await float64
	for name, after := range disks {
		before := c.disks[name]
		if after.operations < before.operations || after.millis < before.millis || after.busy < before.busy {
			return errors.New("disk counters reset")
		}
		util = max(util, (after.busy-before.busy)/elapsed/10)
		if ops := after.operations - before.operations; ops > 0 {
			await = max(await, (after.millis-before.millis)/ops)
		}
	}
	if counters.updates < c.writes.updates || counters.seconds < c.writes.seconds || counters.failures < c.writes.failures {
		return errors.New("persister counters reset; daemon may have restarted")
	}
	updates := counters.updates - c.writes.updates
	latency := float64(0)
	if updates > 0 {
		latency = 1000 * (counters.seconds - c.writes.seconds) / updates
	}
	bad := util > c.options.maxUtil || await > c.options.maxAwait || latency > c.options.maxWrite || counters.failures > c.writes.failures
	c.adjust(bad, updates > 0)
	c.status = fmt.Sprintf("util=%.1f%% await=%.2fms write_mean=%.2fms write_samples=%.0f healthy_windows=%d", util, await, latency, updates, c.healthy)
	c.at, c.disks, c.writes = now, disks, counters
	return nil
}

func (c *controller) adjust(bad, active bool) {
	if bad {
		c.rate = max(1, max(c.options.dirsPerSecond/4, c.rate/2))
		c.healthy = 0
		return
	}
	if !active {
		c.rate = min(c.rate, c.options.dirsPerSecond)
		c.healthy = 0
		return
	}
	c.healthy++
	if c.healthy >= 3 {
		c.rate = min(c.options.maxRate, c.rate*1.25)
		c.healthy = 0
	}
}
