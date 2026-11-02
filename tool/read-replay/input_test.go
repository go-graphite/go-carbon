package main

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func logEntry(at time.Time, raw string, cached bool) string {
	b, _ := json.Marshal(map[string]any{
		"logger": "carbonserver", "message": "fetch served", "handler": "render", "http_code": 200,
		"timestamp": at.Format("2006-01-02T15:04:05.000-0700"), "url": raw,
		"runtime_seconds": .012345, "from_cache": cached,
	})
	return string(b)
}

func writeLog(t *testing.T, name, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(path, []byte(content), 0600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestLoadPlainGzipWindowAndSkipped(t *testing.T) {
	base := time.Date(2026, 11, 2, 19, 0, 0, 0, time.FixedZone("", 3600))
	valid := logEntry(base, "/render/?from=100&until=200&target=a%2Ab", true)
	plain := writeLog(t, "hour.json", strings.Join([]string{
		logEntry(base.Add(time.Minute), "/render/?target=later", false), // Deliberately out of order.
		`{"logger":"persister","timestamp":false,"url":42,"runtime_seconds":"ignored"}`,
		strings.Replace(valid, `"http_code":200`, `"http_code":404`, 1),
		strings.Replace(valid, `"handler":"render"`, `"handler":"find"`, 1),
		strings.Replace(valid, `"message":"fetch served"`, `"message":"error"`, 1),
		`{"broken":`,
		`null`,
		strings.Replace(valid, `"runtime_seconds":0.012345`, `"runtime_seconds":-1`, 1),
		strings.Replace(valid, `"timestamp":"2026-11-02T19:00:00.000+0100"`, `"timestamp":"bad"`, 1),
		strings.Replace(valid, `/render/?`, `https://other.invalid/render/?`, 1),
		valid,
	}, "\n")+"\n")
	gzPath := filepath.Join(t.TempDir(), "hour.json.gz")
	f, err := os.Create(gzPath)
	if err != nil {
		t.Fatal(err)
	}
	gz := gzip.NewWriter(f)
	if _, err := gz.Write([]byte(logEntry(base.Add(2*time.Minute), "/render/?target=end", false))); err != nil {
		t.Fatal(err)
	}
	if err := gz.Close(); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	requests, stats, err := loadRequests(context.Background(), []string{plain, gzPath}, base, base.Add(2*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if stats != (inputStats{Lines: 12, Skipped: 4, Malformed: 5, OutsideWindow: 1, Loaded: 2, FromCache: 1}) {
		t.Fatalf("stats: %+v", stats)
	}
	if len(requests) != 2 || !requests[0].At.Equal(base) || requests[0].Runtime != 12345*time.Microsecond || !requests[0].FromCache || len(requests[0].epochs) != 2 || requests[0].epochs[0].value != 100 {
		t.Fatalf("requests: %+v", requests)
	}
}

func TestOversizedMalformedLineDoesNotStopLoading(t *testing.T) {
	at := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	path := writeLog(t, "oversize.json", strings.Repeat("x", (8<<20)+1)+"\n"+logEntry(at, "/render/?target=a", false))
	r, s, err := loadRequests(context.Background(), []string{path}, time.Time{}, time.Time{})
	if err != nil || len(r) != 1 || s.Malformed != 1 || s.Lines != 2 {
		t.Fatalf("%+v %+v %v", r, s, err)
	}
}

func TestLoadFailures(t *testing.T) {
	for _, tc := range []struct{ name, content string }{{"empty.json", ""}, {"bad.gz", "not gzip"}, {"skip.json", `{}`}} {
		t.Run(tc.name, func(t *testing.T) {
			if _, _, err := loadRequests(context.Background(), []string{writeLog(t, tc.name, tc.content)}, time.Time{}, time.Time{}); err == nil {
				t.Fatal("accepted empty or invalid input")
			}
		})
	}
	if _, _, err := loadRequests(context.Background(), []string{filepath.Join(t.TempDir(), "missing")}, time.Time{}, time.Time{}); err == nil {
		t.Fatal("accepted missing input")
	}
}

func TestLoadingCanBeInterrupted(t *testing.T) {
	path := writeLog(t, "interrupt.json", "{}\n{}\n")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	count := 0
	err := readLog(ctx, path, func([]byte, bool) { count++; cancel() })
	if err != context.Canceled || count != 1 {
		t.Fatalf("count=%d err=%v", count, err)
	}
}

func TestTimeShiftOnlyChangesEpochValues(t *testing.T) {
	const raw = "/render/?format=carbonapi%5Fv2%5Fpb&from=1700000000&target=a.%2A.b%2fc&target=x+y&until=%31%37%30%30%30%30%30%36%30%30&x=1700000000&fr%6fm=1700000010&&empty=&from=-1h&until=now&"
	r, err := parseRequest(raw)
	if err != nil {
		t.Fatal(err)
	}
	r.At = time.Unix(1700001000, 730000000)
	const want = "/render/?format=carbonapi%5Fv2%5Fpb&from=1700086400&target=a.%2A.b%2fc&target=x+y&until=1700087000&x=1700000000&fr%6fm=1700086410&&empty=&from=-1h&until=now&"
	if got := r.shifted(r.At.Add(24 * time.Hour)); got != want {
		t.Fatalf("got  %s\nwant %s", got, want)
	}
	if got := r.shifted(r.At.Add(-24 * time.Hour)); !strings.Contains(got, "from=1699913600&") {
		t.Fatal(got)
	}
	// The log has sub-second precision but render epochs are whole seconds.
	if got := r.shifted(r.At.Add(24*time.Hour - 630*time.Millisecond)); !strings.Contains(got, "from=1700086399&") || !strings.Contains(got, "until=1700086999&") {
		t.Fatal(got)
	}
	for _, raw := range []string{"/render", "/render/?target=a%2fb", "/render/?from=-2h&until=now"} {
		r, err := parseRequest(raw)
		if err != nil || r.shifted(time.Now()) != raw {
			t.Fatalf("%s: %v", raw, err)
		}
	}
	for _, raw := range []string{"http://other.invalid/render/", "//other.invalid/render/", "/admin", "/render/#fragment", "/render/?from=999999999999999", "/render/?from=%xx"} {
		if _, err := parseRequest(raw); err == nil {
			t.Fatalf("accepted %s", raw)
		}
	}
}
