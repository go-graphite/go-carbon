package downsize

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"

	protov3 "github.com/go-graphite/protocol/carbonapi_v3_pb"
)

func readyValueEqual(a, b float64) bool {
	return a == b || math.Abs(a-b) <= math.Max(1e-300, 1e-9*math.Max(math.Abs(a), math.Abs(b)))
}
func (b *syncLiveBackend) Process(ctx context.Context, store string, port int) (float64, error) {
	u, err := url.Parse(store)
	if err != nil {
		return 0, err
	}
	u.Host = net.JoinHostPort(u.Hostname(), strconv.Itoa(port))
	u.Path = "/metrics"
	var start float64
	err = b.wait(ctx, "ready process", store, func() error {
		req, e := http.NewRequestWithContext(ctx, "GET", u.String(), nil)
		if e != nil {
			return e
		}
		resp, e := b.client.Do(req)
		if e != nil {
			return e
		}
		defer resp.Body.Close()
		if resp.StatusCode != 200 {
			return &syncHTTPError{resp.StatusCode, "metrics"}
		}
		scan := bufio.NewScanner(io.LimitReader(resp.Body, 16<<20))
		count := 0
		for scan.Scan() {
			fields := strings.Fields(scan.Text())
			name := ""
			if len(fields) > 0 {
				name, _, _ = strings.Cut(fields[0], "{")
			}
			if len(fields) >= 2 && name == "process_start_time_seconds" {
				start, e = strconv.ParseFloat(fields[1], 64)
				if e != nil {
					return e
				}
				count++
			}
		}
		if e = scan.Err(); e != nil {
			return e
		}
		if count != 1 || math.IsNaN(start) || math.IsInf(start, 0) {
			return fmt.Errorf("expected unique finite process_start_time_seconds")
		}
		return nil
	})
	return start, err
}
func (b *syncLiveBackend) Policies(ctx context.Context, store string, names []string) (map[string]readyPolicy, error) {
	out := map[string]readyPolicy{}
	if len(names) == 0 {
		return out, nil
	}
	base, err := b.carbon(store)
	if err != nil {
		return nil, err
	}
	// Reuse Info's bounded batching/404 handling before requesting full policy
	// metadata; absent names are legitimate on individual donors.
	known, err := b.Info(ctx, store, names)
	if err != nil {
		return nil, err
	}
	var present []string
	for _, name := range names {
		if _, ok := known[name]; ok {
			present = append(present, name)
		}
	}
	limit := 128
	if value, ok := b.infoLimits.Load(store); ok {
		if n := value.(*atomic.Int64).Load(); n > 0 {
			limit = min(limit, int(n))
		}
	}
	for cursor := 0; cursor < len(present); cursor += limit {
		subset := present[cursor:min(cursor+limit, len(present))]
		body, e := (&protov3.MultiGlobRequest{Metrics: subset}).MarshalVT()
		if e != nil {
			return nil, e
		}
		var data []byte
		e = b.wait(ctx, "ready policy", store, func() error { var e error; data, e = b.post(ctx, base+"/info/?format=carbonapi_v3_pb", body); return e })
		if e != nil {
			return nil, e
		}
		var response protov3.MultiMetricsInfoResponse
		if e = response.UnmarshalVT(data); e != nil {
			return nil, e
		}
		if len(response.Metrics) != len(subset) {
			return nil, fmt.Errorf("incomplete policy response from %s", store)
		}
		expected := map[string]bool{}
		for _, name := range subset {
			expected[name] = true
		}
		for _, m := range response.Metrics {
			if !expected[m.Name] {
				return nil, fmt.Errorf("unexpected policy name %s", m.Name)
			}
			delete(expected, m.Name)
			p := readyPolicy{Aggregation: m.ConsolidationFunc, XFilesFactor: m.XFilesFactor}
			if p.Aggregation == "" || math.IsNaN(float64(p.XFilesFactor)) || p.XFilesFactor < 0 || p.XFilesFactor > 1 {
				return nil, fmt.Errorf("invalid policy for %s", m.Name)
			}
			for _, r := range m.Retentions {
				p.Archives = append(p.Archives, syncArchive{r.SecondsPerPoint, r.NumberOfPoints})
			}
			out[m.Name] = p
		}
	}
	return out, nil
}
func (b *syncLiveBackend) Readback(ctx context.Context, base, name string, from, until int64) (syncSeries, error) {
	u, err := url.Parse(base)
	if err != nil {
		return syncSeries{}, err
	}
	u.Path = strings.TrimRight(u.Path, "/") + "/render"
	q := u.Query()
	q.Set("target", name)
	q.Set("from", strconv.FormatInt(from, 10))
	q.Set("until", strconv.FormatInt(until, 10))
	q.Set("format", "json")
	q.Set("noCache", "true")
	u.RawQuery = q.Encode()
	req, err := http.NewRequestWithContext(ctx, "GET", u.String(), nil)
	if err != nil {
		return syncSeries{}, err
	}
	req.Header.Set("Cache-Control", "no-cache")
	resp, err := b.client.Do(req)
	if err != nil {
		return syncSeries{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return syncSeries{}, fmt.Errorf("readback HTTP %d", resp.StatusCode)
	}
	var rows []struct {
		Target     string        `json:"target"`
		Datapoints [][2]*float64 `json:"datapoints"`
	}
	if err = json.NewDecoder(io.LimitReader(resp.Body, 64<<20)).Decode(&rows); err != nil {
		return syncSeries{}, err
	}
	if len(rows) != 1 || rows[0].Target != name {
		return syncSeries{}, fmt.Errorf("readback expected one literal series for %s", name)
	}
	points := rows[0].Datapoints
	s := syncSeries{}
	for i, p := range points {
		if p[1] == nil {
			return s, fmt.Errorf("readback missing timestamp")
		}
		ts := int64(*p[1])
		if float64(ts) != *p[1] {
			return s, fmt.Errorf("readback fractional timestamp")
		}
		if i == 0 {
			s.Start = ts
		} else if i == 1 {
			s.Step = ts - s.Start
		}
		if i > 0 && (s.Step <= 0 || ts != s.Start+int64(i)*s.Step) {
			return s, fmt.Errorf("readback irregular timestamps")
		}
		v := math.NaN()
		if p[0] != nil {
			v = *p[0]
		}
		s.Values = append(s.Values, v)
	}
	return s, nil
}
