package heartbeat

import (
	"os"
	"path/filepath"
	"sync"
	"testing"
	"testing/synctest"
	"time"
)

type clock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *clock) now() time.Time { c.mu.Lock(); defer c.mu.Unlock(); return c.t }
func (c *clock) add(d time.Duration) {
	c.mu.Lock()
	c.t = c.t.Add(d)
	c.mu.Unlock()
}

func start(t *testing.T, path string, c *clock) *Heartbeat {
	t.Helper()
	h, err := Start(Options{Path: path, Interval: time.Hour, LossWindow: 16 * time.Minute,
		Cache: func() (int64, int64) { return 5, 100 }, Now: c.now})
	if err != nil {
		t.Fatal(err)
	}
	return h
}

func TestUncleanStopDetectedAndCarried(t *testing.T) {
	path := filepath.Join(t.TempDir(), "hb.json")
	c := &clock{t: time.Unix(1_700_000_000, 0).UTC()}

	first := start(t, path, c)
	if first.Unclean() != nil {
		t.Fatal("first run must not report an unclean stop")
	}
	c.add(time.Minute)
	if err := first.Ready(); err != nil {
		t.Fatal(err)
	}
	// Simulate a crash: the last beat stays without the clean mark.
	c.add(time.Minute)
	if err := first.Stop(); err != nil {
		t.Fatal(err)
	}
	lastAlive := c.now()
	s, err := Read(path)
	if err != nil || s.Clean || !s.Updated.Equal(lastAlive) || s.CacheSize != 5 || s.CacheMaxSize != 100 {
		t.Fatalf("unexpected state after stop: %+v %v", s, err)
	}

	c.add(5 * time.Minute)
	second := start(t, path, c)
	w := second.Unclean()
	if w == nil || !w.LastAlive.Equal(lastAlive) || w.LossWindowSeconds != 960 || !w.Restarted.Equal(c.now()) {
		t.Fatalf("unclean window not detected: %+v", w)
	}
	c.add(2 * time.Minute)
	if err := second.Ready(); err != nil {
		t.Fatal(err)
	}
	resumed := c.now()
	from, until := second.Unclean().Range(time.Hour, time.Minute, c.now().Add(time.Hour))
	if !from.Equal(lastAlive.Add(-17*time.Minute)) || !until.Equal(resumed.Add(time.Minute)) {
		t.Fatalf("range %v..%v", from, until)
	}
	if err := second.MarkClean(); err != nil {
		t.Fatal(err)
	}
	if err := second.Stop(); err != nil { // no-op after MarkClean
		t.Fatal(err)
	}
	s, err = Read(path)
	if err != nil || !s.Clean || s.Unclean == nil || !s.Unclean.Resumed.Equal(resumed) {
		t.Fatalf("clean state %+v %v", s, err)
	}

	// A clean stop is not an unclean window, but the last one is kept.
	c.add(time.Minute)
	third := start(t, path, c)
	defer third.Stop()
	if third.Unclean() != nil {
		t.Fatal("clean stop reported as unclean")
	}
	s, _ = Read(path)
	if s.Unclean == nil || !s.Unclean.LastAlive.Equal(lastAlive) || s.Clean {
		t.Fatalf("previous window not carried: %+v", s)
	}
}

func TestFailedStartKeepsOpenWindow(t *testing.T) {
	path := filepath.Join(t.TempDir(), "hb.json")
	c := &clock{t: time.Unix(1_700_000_000, 0).UTC()}
	crashed := start(t, path, c)
	_ = crashed.Ready()
	_ = crashed.Stop()
	lastAlive := c.now()
	c.add(time.Minute)
	failed := start(t, path, c) // detects the crash, then fails before Ready
	_ = failed.Stop()
	c.add(time.Minute)
	ok := start(t, path, c)
	defer ok.Stop()
	if w := ok.Unclean(); w == nil || !w.LastAlive.Equal(lastAlive) {
		t.Fatalf("open window lost after failed start: %+v", w)
	}
}

func TestTickerUpdatesAndCorruptFile(t *testing.T) {
	synctest.Test(t, testTickerUpdatesAndCorruptFile)
}

func testTickerUpdatesAndCorruptFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "hb.json")
	if err := os.WriteFile(path, []byte("{garbage"), 0o600); err != nil {
		t.Fatal(err)
	}
	h, err := Start(Options{Path: path, Interval: 10 * time.Millisecond})
	if err == nil || h == nil {
		t.Fatalf("corrupt file must be reported but not stop the heartbeat: %v", err)
	}
	defer h.Stop()
	first, err := Read(path)
	if err != nil {
		t.Fatal(err)
	}
	synctest.Wait()
	time.Sleep(10 * time.Millisecond)
	synctest.Wait()
	s, err := Read(path)
	if err != nil || !s.Updated.After(first.Updated) {
		t.Fatalf("heartbeat did not tick: %+v, %v", s, err)
	}
	if err := h.MarkClean(); err != nil {
		t.Fatal(err)
	}
	entries, _ := os.ReadDir(dir)
	if len(entries) != 1 {
		t.Fatalf("temporary files left behind: %v", entries)
	}
}

func TestRangeWithoutResume(t *testing.T) {
	now := time.Unix(10_000, 0)
	w := Window{LastAlive: time.Unix(5_000, 0)}
	from, until := w.Range(16*time.Minute, 0, now)
	if from.Unix() != 5_000-960 || !until.Equal(now) {
		t.Fatalf("range %v %v", from, until)
	}
}

func TestStartRejectsUnwritableDirectory(t *testing.T) {
	if _, err := Start(Options{Path: filepath.Join(t.TempDir(), "missing", "hb.json")}); err == nil {
		t.Fatal("expected error")
	}
}

func TestWriteErrorsAndLastBeat(t *testing.T) {
	dir := t.TempDir()
	c := &clock{t: time.Unix(1_700_000_000, 0)}
	h := start(t, filepath.Join(dir, "hb.json"), c)
	if !h.LastBeat().Equal(c.now()) || h.WriteErrors() != 0 {
		t.Fatalf("after start: last beat %v errors %d", h.LastBeat(), h.WriteErrors())
	}
	if err := os.Chmod(dir, 0o500); err != nil {
		t.Fatal(err)
	}
	defer os.Chmod(dir, 0o700)
	c.add(time.Minute)
	if err := h.Ready(); err == nil {
		t.Skip("directory permissions not enforced (running as root?)")
	}
	if h.WriteErrors() != 1 || !h.LastBeat().Equal(c.now().Add(-time.Minute)) {
		t.Fatalf("after failed write: last beat %v errors %d", h.LastBeat(), h.WriteErrors())
	}
	_ = os.Chmod(dir, 0o700)
	if err := h.Stop(); err != nil || !h.LastBeat().Equal(c.now()) {
		t.Fatalf("stop: %v, last beat %v", err, h.LastBeat())
	}
}
