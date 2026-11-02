package main

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/url"
	"os"
	"slices"
	"strconv"
	"strings"
	"time"
)

type epochParam struct {
	start, end int
	value      int64
}

type request struct {
	URL       string
	At        time.Time
	Runtime   time.Duration
	FromCache bool
	epochs    []epochParam // Original from/until values and their raw URL spans.
}

type inputStats struct {
	Lines         uint64 `json:"lines"`
	Skipped       uint64 `json:"skipped"`
	Malformed     uint64 `json:"malformed"`
	OutsideWindow uint64 `json:"outside_window"`
	Loaded        uint64 `json:"loaded"`
	FromCache     uint64 `json:"from_cache"`
}

func parseTimestamp(s string) (time.Time, error) {
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		t, err = time.Parse("2006-01-02T15:04:05.999999999-0700", s)
	}
	return t.UTC(), err
}

func parseRequest(raw string) (request, error) {
	u, err := url.ParseRequestURI(raw)
	if err != nil || u.IsAbs() || u.Host != "" || u.Fragment != "" || strings.Contains(raw, "#") || (u.Path != "/render" && u.Path != "/render/") {
		return request{}, errors.New("expected a relative /render/ URL")
	}
	r := request{URL: raw}
	q := strings.IndexByte(raw, '?')
	if q < 0 {
		return r, nil
	}
	for start := q + 1; start <= len(raw); {
		end := strings.IndexByte(raw[start:], '&')
		if end < 0 {
			end = len(raw)
		} else {
			end += start
		}
		part := raw[start:end]
		key, value, ok := strings.Cut(part, "=")
		key, err = url.QueryUnescape(key)
		if err != nil {
			return request{}, err
		}
		if ok && (key == "from" || key == "until") {
			decoded, err := url.QueryUnescape(value)
			if err != nil {
				return request{}, err
			}
			// Relative expressions (for example -1h) are already relative to now.
			if epoch, err := strconv.ParseInt(decoded, 10, 64); err == nil {
				// Carbonserver uses epoch seconds. Bound arithmetic before replay.
				if epoch < 0 || epoch > 253402300799 {
					return request{}, errors.New("epoch outside supported range")
				}
				r.epochs = append(r.epochs, epochParam{start + len(part) - len(value), end, epoch})
			}
		}
		if end == len(raw) {
			break
		}
		start = end + 1
	}
	return r, nil
}

func (r request) shifted(now time.Time) string {
	// Round the shifted epoch down to whole seconds, including the original
	// timestamp's fractional part, so window age differs by less than a second.
	shift := now.Unix() - r.At.Unix()
	if now.Nanosecond() < r.At.Nanosecond() {
		shift--
	}
	var b strings.Builder
	b.Grow(len(r.URL) + 20)
	last := 0
	for _, p := range r.epochs {
		b.WriteString(r.URL[last:p.start])
		b.WriteString(strconv.FormatInt(p.value+shift, 10))
		last = p.end
	}
	b.WriteString(r.URL[last:])
	return b.String()
}

func loadRequests(ctx context.Context, paths []string, start, end time.Time) ([]request, inputStats, error) {
	var requests []request
	var stats inputStats
	for _, path := range paths {
		if err := readLog(ctx, path, func(line []byte, oversized bool) {
			stats.Lines++
			// Decode only routing metadata first: unrelated entries never parse
			// timestamps, URLs, targets, or the other render fields.
			var header struct {
				Logger, Message, Handler string
				Code                     int `json:"http_code"`
			}
			line = bytes.TrimSpace(line)
			if oversized || len(line) == 0 || line[0] != '{' || json.Unmarshal(line, &header) != nil {
				stats.Malformed++
				return
			}
			if header.Logger != "carbonserver" || header.Message != "fetch served" || header.Handler != "render" || header.Code != 200 {
				stats.Skipped++
				return
			}
			var entry struct {
				URL, Timestamp string
				Runtime        *float64 `json:"runtime_seconds"`
				FromCache      bool     `json:"from_cache"`
			}
			if json.Unmarshal(line, &entry) != nil || entry.Runtime == nil || *entry.Runtime < 0 || *entry.Runtime >= float64(math.MaxInt64)/float64(time.Second) {
				stats.Malformed++
				return
			}
			at, err := parseTimestamp(entry.Timestamp)
			if err != nil {
				stats.Malformed++
				return
			}
			if (!start.IsZero() && at.Before(start)) || (!end.IsZero() && !at.Before(end)) {
				stats.OutsideWindow++
				return
			}
			r, err := parseRequest(entry.URL)
			if err != nil {
				stats.Malformed++
				return
			}
			r.At, r.Runtime, r.FromCache = at, time.Duration(*entry.Runtime*float64(time.Second)), entry.FromCache
			requests = append(requests, r)
			stats.Loaded++
			if r.FromCache {
				stats.FromCache++
			}
		}); err != nil {
			return nil, stats, fmt.Errorf("%s: %w", path, err)
		}
	}
	slices.SortStableFunc(requests, func(a, b request) int { return a.At.Compare(b.At) })
	if len(requests) == 0 {
		return nil, stats, errors.New("no matching render requests")
	}
	return requests, stats, nil
}

func readLog(ctx context.Context, path string, consume func([]byte, bool)) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	var input io.Reader = f
	if strings.HasSuffix(path, ".gz") {
		gz, err := gzip.NewReader(f)
		if err != nil {
			return err
		}
		defer gz.Close()
		input = gz
	}
	reader := bufio.NewReaderSize(input, 64<<10)
	const maxLine = 8 << 20
	var line []byte
	oversized := false
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		part, err := reader.ReadSlice('\n')
		if len(line)+len(part) > maxLine {
			oversized = true
		}
		if !oversized {
			line = append(line, part...)
		}
		if err == bufio.ErrBufferFull {
			continue
		}
		if len(line) > 0 || oversized {
			consume(line, oversized)
		}
		line = line[:0]
		oversized = false
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
	}
}
