// Command replay-bench replays a Graphite plaintext export ("path value timestamp" lines,
// optionally gzipped) into a TCP listener at full rate and measures reads against the same
// data. It is system-agnostic: ingest is plain Graphite TCP; reads speak the carbonserver
// protobuf render API ("carbonserver", also graphite-clickhouse) or the Prometheus
// query_range API with __name__ selectors ("prom", for VictoriaMetrics).
//
//	replay-bench send  -data d.gz -addr 127.0.0.1:2003 [-conns 16]
//	replay-bench read  -data d.gz -api carbonserver -base http://127.0.0.1:8080 -shape 100x24h [-n 100] [-pids 1,2]
//	replay-bench check -data d.gz -api prom -base http://127.0.0.1:8428 [-sample 400]
//
// Server CPU, RSS and bytes at rest are measured by the caller.
package main

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"encoding/json"
	"flag"
	"fmt"
	"hash/fnv"
	"io"
	"math"
	"net"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	pb "github.com/go-graphite/protocol/carbonapi_v2_pb"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: replay-bench send|read|check [flags]")
		os.Exit(2)
	}
	cmd := os.Args[1]
	fs := flag.NewFlagSet(cmd, flag.ExitOnError)
	data := fs.String("data", "", "export file (.gz or plain)")
	addr := fs.String("addr", "127.0.0.1:2003", "send: Graphite TCP address")
	conns := fs.Int("conns", 16, "send: connections (lines are sharded by path)")
	api := fs.String("api", "carbonserver", "read/check: carbonserver or prom")
	base := fs.String("base", "http://127.0.0.1:8080", "read/check: HTTP base URL")
	shape := fs.String("shape", "1x24h", "read: <names>x<24h|1h>, e.g. 1x24h, 100x24h, 1000x1h")
	n := fs.Int("n", 100, "read: timed requests")
	warm := fs.Int("warmup", 5, "read: untimed warmup requests")
	pids := fs.String("pids", "", "read: comma-separated server PIDs whose CPU is read from /proc")
	sample := fs.Int("sample", 400, "check: sampled series")
	step := fs.Int("step", 60, "prom: query_range step seconds")
	_ = fs.Parse(os.Args[2:])
	if *data == "" {
		fatal("-data required")
	}
	var err error
	switch cmd {
	case "send":
		err = send(*data, *addr, *conns)
	case "read":
		err = read(*data, *api, *base, *shape, *n, *warm, *pids, *step)
	case "check":
		err = check(*data, *api, *base, *sample, *step)
	default:
		err = fmt.Errorf("unknown command %q", cmd)
	}
	if err != nil {
		fatal(err.Error())
	}
}

func fatal(s string) { fmt.Fprintln(os.Stderr, "replay-bench:", s); os.Exit(1) }

func open(path string) (io.ReadCloser, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	if !strings.HasSuffix(path, ".gz") {
		return f, nil
	}
	z, err := gzip.NewReader(f)
	if err != nil {
		f.Close()
		return nil, err
	}
	return struct {
		io.Reader
		io.Closer
	}{z, f}, nil
}

// rawLines streams the export without parsing; the slice is only valid during fn.
func rawLines(path string, fn func(line []byte) error) error {
	rc, err := open(path)
	if err != nil {
		return err
	}
	defer rc.Close()
	br := bufio.NewReaderSize(rc, 1<<20)
	for {
		l, err := br.ReadSlice('\n')
		if err == bufio.ErrBufferFull {
			return fmt.Errorf("line longer than 1 MiB")
		}
		if len(l) > 0 {
			if ferr := fn(l); ferr != nil {
				return ferr
			}
		}
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
	}
}

// lines streams the export; every line is "path value ts".
func lines(path string, fn func(line []byte, p, v string, ts int64) error) error {
	rc, err := open(path)
	if err != nil {
		return err
	}
	defer rc.Close()
	br := bufio.NewReaderSize(rc, 1<<20)
	for {
		l, err := br.ReadBytes('\n')
		if len(l) > 0 {
			f := strings.Fields(string(l))
			if len(f) != 3 {
				return fmt.Errorf("bad line %q", l)
			}
			ts, perr := strconv.ParseInt(f[2], 10, 64)
			if perr != nil {
				return perr
			}
			if ferr := fn(l, f[0], f[1], ts); ferr != nil {
				return ferr
			}
		}
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
	}
}

func send(data, addr string, nconn int) error {
	chans := make([]chan []byte, nconn)
	done := make(chan error, nconn)
	var sent [64]int64 // bytes per connection (nconn <= 64)
	if nconn > len(sent) {
		return fmt.Errorf("-conns must be <= %d", len(sent))
	}
	for i := range chans {
		chans[i] = make(chan []byte, 64)
		c, err := net.Dial("tcp", addr)
		if err != nil {
			return err
		}
		go func(i int, c net.Conn) {
			defer c.Close()
			for b := range chans[i] {
				if _, err := c.Write(b); err != nil {
					done <- err
					return
				}
				sent[i] += int64(len(b))
			}
			done <- nil
		}(i, c)
	}
	bufs := make([][]byte, nconn)
	var total int64
	start := time.Now()
	err := rawLines(data, func(l []byte) error {
		sp := bytes.IndexByte(l, ' ')
		if sp < 0 {
			return fmt.Errorf("bad line %q", l)
		}
		h := fnv.New32a()
		h.Write(l[:sp])
		i := int(h.Sum32() % uint32(nconn))
		bufs[i] = append(bufs[i], l...)
		total++
		if len(bufs[i]) >= 64<<10 {
			chans[i] <- bufs[i]
			bufs[i] = make([]byte, 0, 64<<10)
		}
		return nil
	})
	for i := range chans {
		if len(bufs[i]) > 0 {
			chans[i] <- bufs[i]
		}
		close(chans[i])
	}
	for range chans {
		if e := <-done; e != nil && err == nil {
			err = e
		}
	}
	el := time.Since(start).Seconds()
	var b int64
	for _, x := range sent {
		b += x
	}
	out, _ := json.Marshal(map[string]any{"stage": "send", "lines": total, "bytes": b, "seconds": el, "lines_per_s": float64(total) / el})
	fmt.Println(string(out))
	return err
}

// summary of the export needed to choose read windows and names.
type summary struct {
	paths      []string
	tmin, tmax int64
	lines      int64
	lastHour   map[string]bool // paths with a point in the final hour
}

// summarize scans the export once (twice on a cold cache) and keeps the result in <data>.sum.
func summarize(data string) (*summary, error) {
	if f, err := os.Open(data + ".sum"); err == nil {
		defer f.Close()
		var c cachedSummary
		if json.NewDecoder(f).Decode(&c) == nil {
			s := &summary{paths: c.Paths, tmin: c.Tmin, tmax: c.Tmax, lines: c.Lines, lastHour: map[string]bool{}}
			for _, p := range c.LastHour {
				s.lastHour[p] = true
			}
			return s, nil
		}
	}
	s, err := scan(data)
	if err != nil {
		return nil, err
	}
	c := cachedSummary{Paths: s.paths, Tmin: s.tmin, Tmax: s.tmax, Lines: s.lines}
	for p := range s.lastHour {
		c.LastHour = append(c.LastHour, p)
	}
	if b, err := json.Marshal(c); err == nil {
		_ = os.WriteFile(data+".sum", b, 0o644)
	}
	return s, nil
}

type cachedSummary struct {
	Paths      []string
	Tmin, Tmax int64
	Lines      int64
	LastHour   []string
}

func scan(data string) (*summary, error) {
	s := &summary{tmin: math.MaxInt64, lastHour: map[string]bool{}}
	seen := map[string]bool{}
	// two passes: bounds first, then final-hour membership.
	err := lines(data, func(_ []byte, p, _ string, ts int64) error {
		s.lines++
		if !seen[p] {
			seen[p] = true
			s.paths = append(s.paths, p)
		}
		if ts < s.tmin {
			s.tmin = ts
		}
		if ts > s.tmax {
			s.tmax = ts
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Strings(s.paths)
	from := s.tmax - 3600
	err = lines(data, func(_ []byte, p, _ string, ts int64) error {
		if ts > from {
			s.lastHour[p] = true
		}
		return nil
	})
	return s, err
}

// window returns the half-open [from, until) window for a shape: the whole span (at most 24h)
// or the final hour of the export.
func (s *summary) window(span string) (int64, int64) {
	if span == "1h" {
		return s.tmax - 3600 + 1, s.tmax + 1
	}
	u := s.tmax + 1
	f := s.tmin
	if u-f > 86400 {
		f = u - 86400
	}
	return f, u
}

func pick(names []string, n int) []string {
	if n > len(names) {
		n = len(names)
	}
	out := make([]string, 0, n)
	for i := 0; i < n; i++ {
		out = append(out, names[int(int64(i)*int64(len(names))/int64(n))])
	}
	return out
}

type fetched map[string]map[int64]float64

func fetch(api, base string, names []string, from, until int64, step int) (fetched, int, error) {
	form := url.Values{}
	var u string
	switch api {
	case "carbonserver":
		u = base + "/render/"
		form["target"] = names
		form.Set("format", "protobuf")
		form.Set("from", strconv.FormatInt(from, 10))
		form.Set("until", strconv.FormatInt(until, 10))
	case "prom":
		u = base + "/api/v1/query_range"
		q := make([]string, len(names))
		for i, n := range names {
			q[i] = regexp.QuoteMeta(n)
		}
		form.Set("query", `{__name__=~"`+strings.ReplaceAll(strings.Join(q, "|"), `\`, `\\`)+`"}`)
		form.Set("start", strconv.FormatInt(from, 10))
		form.Set("end", strconv.FormatInt(until-1, 10))
		form.Set("step", strconv.Itoa(step))
		form.Set("nocache", "1")
	default:
		return nil, 0, fmt.Errorf("unknown api %q", api)
	}
	cl := &http.Client{Transport: &http.Transport{DisableKeepAlives: true}, Timeout: 10 * time.Minute}
	resp, err := cl.PostForm(u, form)
	if err != nil {
		return nil, 0, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, 0, err
	}
	if resp.StatusCode != 200 {
		return nil, len(body), fmt.Errorf("%s: %s", resp.Status, truncate(body))
	}
	return decode(api, body)
}

func truncate(b []byte) string {
	if len(b) > 200 {
		b = b[:200]
	}
	return string(b)
}

func decode(api string, body []byte) (fetched, int, error) {
	out := fetched{}
	switch api {
	case "carbonserver":
		var m pb.MultiFetchResponse
		if err := m.UnmarshalVT(body); err != nil {
			return nil, len(body), err
		}
		for _, s := range m.Metrics {
			pts := map[int64]float64{}
			for i, v := range s.Values {
				if i < len(s.IsAbsent) && s.IsAbsent[i] {
					continue
				}
				pts[int64(s.StartTime)+int64(i)*int64(s.StepTime)] = v
			}
			out[s.Name] = pts
		}
	case "prom":
		var r struct {
			Data struct {
				Result []struct {
					Metric map[string]string `json:"metric"`
					Values [][2]any          `json:"values"`
				} `json:"result"`
			} `json:"data"`
		}
		if err := json.Unmarshal(body, &r); err != nil {
			return nil, len(body), err
		}
		for _, s := range r.Data.Result {
			pts := map[int64]float64{}
			for _, tv := range s.Values {
				t, _ := tv[0].(float64)
				v, _ := strconv.ParseFloat(fmt.Sprint(tv[1]), 64)
				pts[int64(t)] = v
			}
			out[s.Metric["__name__"]] = pts
		}
	}
	return out, len(body), nil
}

func procCPU(pids string) float64 {
	var tot float64
	for _, p := range strings.Split(pids, ",") {
		b, err := os.ReadFile("/proc/" + strings.TrimSpace(p) + "/stat")
		if err != nil {
			continue
		}
		f := strings.Fields(string(b[strings.LastIndexByte(string(b), ')')+2:]))
		u, _ := strconv.ParseFloat(f[11], 64)
		s, _ := strconv.ParseFloat(f[12], 64)
		tot += (u + s) / 100 // CLK_TCK is 100 on Linux
	}
	return tot
}

func read(data, api, base, shape string, n, warm int, pids string, step int) error {
	parts := strings.SplitN(shape, "x", 2)
	if len(parts) != 2 {
		return fmt.Errorf("bad -shape %q", shape)
	}
	count, err := strconv.Atoi(parts[0])
	if err != nil {
		return err
	}
	s, err := summarize(data)
	if err != nil {
		return err
	}
	from, until := s.window(parts[1])
	cand := s.paths
	if parts[1] == "1h" {
		cand = nil
		for _, p := range s.paths {
			if s.lastHour[p] {
				cand = append(cand, p)
			}
		}
	}
	names := pick(cand, count)
	for i := 0; i < warm; i++ {
		if _, _, err := fetch(api, base, names, from, until, step); err != nil {
			return err
		}
	}
	var ms []float64
	var series, bytes, errs int
	cpu0 := procCPU(pids)
	for i := 0; i < n; i++ {
		for attempt := 0; ; attempt++ {
			t := time.Now()
			f, b, err := fetch(api, base, names, from, until, step)
			if err == nil {
				ms = append(ms, float64(time.Since(t).Microseconds())/1000)
				series, bytes = len(f), b
				break
			}
			errs++
			fmt.Fprintln(os.Stderr, "read:", err)
			if attempt == 2 {
				return err
			}
		}
	}
	cpu := procCPU(pids) - cpu0
	sort.Float64s(ms)
	q := func(p float64) float64 { return ms[int(math.Ceil(p*float64(len(ms))))-1] }
	out, _ := json.Marshal(map[string]any{"stage": "read", "shape": shape, "names": len(names), "series_returned": series,
		"body_bytes": bytes, "from": from, "until": until, "requests": n, "p50_ms": q(0.5), "p99_ms": q(0.99),
		"cpu_ms_per_query": cpu * 1000 / float64(n), "errors": errs})
	fmt.Println(string(out))
	return nil
}

func check(data, api, base string, sample, step int) error {
	s, err := summarize(data)
	if err != nil {
		return err
	}
	names := pick(s.paths, sample)
	want := map[string]map[int64]string{}
	for _, n := range names {
		want[n] = map[int64]string{}
	}
	if err := lines(data, func(_ []byte, p, v string, ts int64) error {
		if m, ok := want[p]; ok {
			m[ts] = v
		}
		return nil
	}); err != nil {
		return err
	}
	// carbonserver windows are (from, until]; query_range evaluates at start, start+step, ...
	from, until := s.tmin-1, s.tmax+1
	if api == "prom" {
		from = s.tmin
	}
	var compared, changed, missing, sampledChanged, sampledMissing, sampledN int
	var maxRel float64
	shown := 0
	for i := 0; i < len(names); i += 25 {
		j := i + 25
		if j > len(names) {
			j = len(names)
		}
		got, _, err := fetch(api, base, names[i:j], from, until, step)
		if err != nil {
			return err
		}
		for _, n := range names[i:j] {
			pts := got[n]
			// one deterministic point per series is the doc-style "sampled value".
			tss := make([]int64, 0, len(want[n]))
			for t := range want[n] {
				tss = append(tss, t)
			}
			sort.Slice(tss, func(a, b int) bool { return tss[a] < tss[b] })
			sampledTS := tss[len(tss)/2]
			sampledN++
			for _, t := range tss {
				exp, _ := strconv.ParseFloat(want[n][t], 64)
				g, ok := pts[t]
				compared++
				bad := false
				if !ok {
					missing++
					bad = true
				} else if math.Float64bits(g) != math.Float64bits(exp) {
					changed++
					bad = true
					if exp != 0 {
						if r := math.Abs(g-exp) / math.Abs(exp); r > maxRel {
							maxRel = r
						}
					}
				}
				if bad && shown < 5 {
					shown++
					fmt.Fprintf(os.Stderr, "check: %s ts=%d want=%s got=%v present=%v\n", n, t, want[n][t], g, ok)
				}
				if bad && t == sampledTS {
					if ok {
						sampledChanged++
					} else {
						sampledMissing++
					}
				}
			}
		}
	}
	out, _ := json.Marshal(map[string]any{"stage": "check", "series": len(names), "sampled_values": sampledN,
		"sampled_changed": sampledChanged, "sampled_missing": sampledMissing, "points_compared": compared,
		"points_changed": changed, "points_missing": missing, "max_rel_error": maxRel})
	fmt.Println(string(out))
	return nil
}
