package persister

import (
	"math"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	whisper "github.com/go-graphite/go-whisper"

	"github.com/go-graphite/go-carbon/helper"
	"github.com/go-graphite/go-carbon/points"
)

// fakeCache is the minimum of cache.Cache that store() actually uses.
//
// pop, confirm, and requeue mirror the cache's in-flight write lifecycle.
type fakeCache struct {
	mu           sync.Mutex
	data         map[string]*points.Points
	notConfirmed []*points.Points
	confirmed    int
}

func (c *fakeCache) add(metric string, timestamp int64, value float64) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.data == nil {
		c.data = map[string]*points.Points{}
	}
	p, ok := c.data[metric]
	if !ok {
		p = &points.Points{Metric: metric}
		c.data[metric] = p
	}
	p.Data = append(p.Data, points.Point{Value: value, Timestamp: timestamp})
}

func (c *fakeCache) pop(metric string) (*points.Points, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	p, ok := c.data[metric]
	if !ok {
		return nil, false
	}
	delete(c.data, metric)
	c.notConfirmed = append(c.notConfirmed, p)

	return p, true
}

func (c *fakeCache) requeue(p *points.Points) {
	c.mu.Lock()
	defer c.mu.Unlock()

	found := false
	for i, np := range c.notConfirmed {
		if np == p {
			c.notConfirmed = append(c.notConfirmed[:i], c.notConfirmed[i+1:]...)
			found = true
			break
		}
	}
	if !found {
		return
	}

	if c.data == nil {
		c.data = map[string]*points.Points{}
	}
	if existing, ok := c.data[p.Metric]; ok {
		p.Data = append(p.Data, existing.Data...)
	}
	c.data[p.Metric] = p
}

func (c *fakeCache) confirm(p *points.Points) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.confirmed += len(p.Data)
	for i, np := range c.notConfirmed {
		if np == p {
			c.notConfirmed = append(c.notConfirmed[:i], c.notConfirmed[i+1:]...)
			break
		}
	}
}

// notConfirmedPoints is the count still parked in notConfirmed; anything left
// here after store() returns would be pinned for the process lifetime.
func (c *fakeCache) notConfirmedPoints() int {
	c.mu.Lock()
	defer c.mu.Unlock()

	n := 0
	for _, p := range c.notConfirmed {
		n += len(p.Data)
	}

	return n
}

func (c *fakeCache) confirmedPoints() int {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.confirmed
}

func newOOOTestPersister(t *testing.T, dataDir string, cache *fakeCache) *Whisper {
	t.Helper()

	retentionStr := "1s:2h"
	retentions, err := ParseRetentionDefs(retentionStr)
	if err != nil {
		t.Fatalf("parse retentions: %s", err)
	}

	compressed := true
	p := NewWhisper(
		dataDir,
		WhisperSchemas{{
			Name:         "test",
			Pattern:      regexp.MustCompile(".*"),
			RetentionStr: retentionStr,
			Retentions:   retentions,
			Priority:     10,
			Compressed:   &compressed,
		}},
		NewWhisperAggregation(),
		nil,
		cache.pop,
		cache.confirm,
		cache.pop,
	)
	p.SetRequeue(cache.requeue)
	p.SetCompressed(true)
	p.EnableOutOfOrder(100, 1<<30)
	// Start() would also spawn workers; the tests drive store() directly
	p.outOfOrder.ticker = helper.NewHardThrottleTicker(p.outOfOrder.rate)
	t.Cleanup(p.outOfOrder.ticker.Stop)

	return p
}

func fetchValue(t *testing.T, path string, timestamp int) float64 {
	t.Helper()

	w, err := whisper.OpenWithOptions(path, &whisper.Options{})
	if err != nil {
		t.Fatalf("open %s: %s", path, err)
	}
	defer w.Close()

	ts, err := w.Fetch(timestamp-1, timestamp+1)
	if err != nil {
		t.Fatalf("fetch: %s", err)
	}
	for _, p := range ts.Points() {
		if p.Time == timestamp {
			return p.Value
		}
	}

	return math.NaN()
}

// End to end through the persister: a point too old for the compressed encoder
// is diverted to a sidecar, served from it, and eventually folded back in.
func TestStoreDivertsAndCompactsOutOfOrderPoints(t *testing.T) {
	dir := t.TempDir()
	cache := &fakeCache{}

	// high threshold so the first pass diverts without compacting
	p := newOOOTestPersister(t, dir, cache)

	const metric = "test.ooo"
	path := filepath.Join(dir, "test", "ooo.wsp")
	base := int(time.Now().Unix()) - 3600

	// in-order points, leaving a hole at base+1
	cache.add(metric, int64(base+0), 1)
	cache.add(metric, int64(base+2), 2)
	cache.add(metric, int64(base+4), 3)
	p.store(metric)

	if _, err := os.Stat(path); err != nil {
		t.Fatalf("compressed file was not created: %s", err)
	}

	// late arrival into the hole
	cache.add(metric, int64(base+1), 7)
	p.store(metric)

	sidecar := path + ".ooo"
	if _, err := os.Stat(sidecar); err != nil {
		t.Fatalf("sidecar was not created: %s", err)
	}
	if got := p.oooDiverted; got != 1 {
		t.Errorf("oooDiverted = %d; want 1", got)
	}
	if got := p.oooCompactions; got != 0 {
		t.Errorf("oooCompactions = %d; want 0 (threshold not reached)", got)
	}
	if got := fetchValue(t, path, base+1); got != 7 {
		t.Errorf("value at the hole = %v; want 7 (merged from the sidecar)", got)
	}

	// drop the threshold so the next store folds the sidecar back in
	p.outOfOrder.threshold = 1
	cache.add(metric, int64(base+6), 4)
	p.store(metric)

	if got := p.oooCompactions; got != 1 {
		t.Fatalf("oooCompactions = %d; want 1", got)
	}
	if got := p.oooCompactErrors; got != 0 {
		t.Errorf("oooCompactErrors = %d; want 0", got)
	}
	if _, err := os.Stat(sidecar); !os.IsNotExist(err) {
		t.Errorf("sidecar still present after compaction (stat err = %v)", err)
	}

	// the diverted point is now in the compressed file itself
	if got := fetchValue(t, path, base+1); got != 7 {
		t.Errorf("value at the hole after compaction = %v; want 7", got)
	}
	for _, tc := range []struct {
		ts   int
		want float64
	}{{base + 0, 1}, {base + 2, 2}, {base + 4, 3}, {base + 6, 4}} {
		if got := fetchValue(t, path, tc.ts); got != tc.want {
			t.Errorf("value at %d = %v; want %v", tc.ts, got, tc.want)
		}
	}
}

// With out-of-order off, the persister keeps its historical behaviour: the late
// point is discarded and no sidecar appears.
func TestStoreDiscardsOutOfOrderPointsWhenDisabled(t *testing.T) {
	dir := t.TempDir()
	cache := &fakeCache{}

	p := newOOOTestPersister(t, dir, cache)
	p.outOfOrder.enabled = false

	const metric = "test.ooo"
	path := filepath.Join(dir, "test", "ooo.wsp")
	base := int(time.Now().Unix()) - 3600

	cache.add(metric, int64(base+0), 1)
	cache.add(metric, int64(base+2), 2)
	cache.add(metric, int64(base+4), 3)
	p.store(metric)

	cache.add(metric, int64(base+1), 7)
	p.store(metric)

	if _, err := os.Stat(path + ".ooo"); !os.IsNotExist(err) {
		t.Errorf("sidecar created while out-of-order is disabled (stat err = %v)", err)
	}
	if got := p.oooDiverted; got != 0 {
		t.Errorf("oooDiverted = %d; want 0", got)
	}
	if got := p.oooDiscardedPoints; got != 1 {
		t.Errorf("oooDiscardedPoints = %d; want 1", got)
	}
	if got := fetchValue(t, path, base+1); !math.IsNaN(got) {
		t.Errorf("value at the hole = %v; want NaN (point should be discarded)", got)
	}
}

func TestStoreAlwaysCreatesSparseOutOfOrderSidecar(t *testing.T) {
	dir := t.TempDir()
	cache := &fakeCache{}
	p := newOOOTestPersister(t, dir, cache)
	p.SetSparse(false)

	const metric = "test.sparsity"
	path := filepath.Join(dir, "test", "sparsity.wsp")
	base := int64(time.Now().Unix()) - 3600
	cache.add(metric, base, 1)
	cache.add(metric, base+2, 2)
	p.store(metric)
	cache.add(metric, base+1, 7)
	p.store(metric)

	info, err := os.Stat(path + ".ooo")
	if err != nil {
		t.Fatalf("stat sidecar: %s", err)
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		t.Skip("physical file size is unavailable")
	}
	if physical := stat.Blocks * 512; physical >= info.Size() {
		t.Errorf("sidecar is not sparse: logical=%d physical=%d", info.Size(), physical)
	}
}

func TestStoreLongFilenameDivertsOutOfOrderWithFLock(t *testing.T) {
	dir := t.TempDir()
	cache := &fakeCache{}
	p := newOOOTestPersister(t, dir, cache)
	p.SetFLock(true)

	filenameLength := maxFilenameLength - len(".lock")
	node := strings.Repeat("m", filenameLength-len(".wsp"))
	metric := "test." + node
	path := filepath.Join(dir, "test", node+".wsp")
	if got := len(filepath.Base(path)); got != filenameLength {
		t.Fatalf("filename length = %d; want %d", got, filenameLength)
	}

	base := int64(time.Now().Unix()) - 3600
	cache.add(metric, base, 1)
	cache.add(metric, base+2, 2)
	cache.add(metric, base+4, 3)
	p.store(metric)

	cache.add(metric, base+1, 7)
	p.store(metric)

	if got := p.oooDiverted; got != 1 {
		t.Errorf("oooDiverted = %d; want 1", got)
	}
	if got := cache.confirmedPoints(); got != 4 {
		t.Errorf("confirmed points = %d; want 4", got)
	}
	if _, err := os.Stat(whisper.OutOfOrderSidecarPath(path)); err != nil {
		t.Fatalf("stat sidecar: %s", err)
	}
	if got := fetchValue(t, path, int(base+1)); got != 7 {
		t.Errorf("sidecar value = %v; want 7", got)
	}
}

// A write that fails must not be counted as committed, and must not be left
// parked in the cache's not-confirmed list either: it goes back into the cache
// so the next writeout retries it.
func TestStoreRetriesFailedOutOfOrderWrite(t *testing.T) {
	dir := t.TempDir()
	cache := &fakeCache{}
	p := newOOOTestPersister(t, dir, cache)

	const metric = "test.ooo"
	path := filepath.Join(dir, "test", "ooo.wsp")
	base := int(time.Now().Unix()) - 3600

	cache.add(metric, int64(base), 1)
	cache.add(metric, int64(base+2), 2)
	p.store(metric)
	if got := cache.confirmedPoints(); got != 2 {
		t.Fatalf("confirmed points after initial write = %d; want 2", got)
	}
	if got := cache.notConfirmedPoints(); got != 0 {
		t.Fatalf("not-confirmed points after initial write = %d; want 0", got)
	}

	// a directory where the sidecar belongs makes every divert fail
	if err := os.Mkdir(path+".ooo", 0o755); err != nil {
		t.Fatalf("create invalid sidecar: %s", err)
	}
	cache.add(metric, int64(base+1), 7)
	p.store(metric)

	if got := p.committedPoints; got != 2 {
		t.Errorf("committedPoints = %d; want 2", got)
	}
	if got := cache.confirmedPoints(); got != 2 {
		t.Errorf("confirmed points after failed write = %d; want 2", got)
	}
	if got := cache.notConfirmedPoints(); got != 0 {
		t.Errorf("not-confirmed points after failed write = %d; want 0 (batch leaked)", got)
	}

	// the failed point is back in the cache, so the next writeout retries it
	values, ok := cache.pop(metric)
	if !ok {
		t.Fatalf("failed batch was not returned to the cache")
	}
	if len(values.Data) != 1 || values.Data[0].Timestamp != int64(base+1) {
		t.Errorf("re-added batch = %+v; want the single point at %d", values.Data, base+1)
	}
}

func TestCompactionFailureWithFLockDoesNotDeadlock(t *testing.T) {
	dir := t.TempDir()
	cache := &fakeCache{}
	p := newOOOTestPersister(t, dir, cache)
	p.SetFLock(true)

	const metric = "test.ooo"
	path := filepath.Join(dir, "test", "ooo.wsp")
	base := int(time.Now().Unix()) - 3600

	cache.add(metric, int64(base), 1)
	cache.add(metric, int64(base+2), 2)
	p.store(metric)
	cache.add(metric, int64(base+1), 7)
	p.store(metric)

	p.outOfOrder.threshold = 1
	select {
	case p.outOfOrder.ticker.C <- true:
	default:
	}
	if err := os.Mkdir(path+".compact", 0o755); err != nil {
		t.Fatalf("create invalid compaction target: %s", err)
	}
	if err := os.WriteFile(path+".compact/blocker", []byte("x"), 0o600); err != nil {
		t.Fatalf("populate invalid compaction target: %s", err)
	}

	cache.add(metric, int64(base+4), 3)
	done := make(chan struct{})
	go func() {
		p.store(metric)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("store deadlocked reopening a flocked file after compaction failure")
	}

	if got := p.oooCompactErrors; got != 1 {
		t.Errorf("oooCompactErrors = %d; want 1", got)
	}
	if got := fetchValue(t, path, base+1); got != 7 {
		t.Errorf("sidecar value after failed compaction = %v; want 7", got)
	}
}

func TestOnlineMigrationWithFLockDoesNotDeadlock(t *testing.T) {
	dir := t.TempDir()
	cache := &fakeCache{}
	p := newOOOTestPersister(t, dir, cache)
	p.SetFLock(true)

	const metric = "test.migration"
	path := filepath.Join(dir, "test", "migration.wsp")
	base := int(time.Now().Unix()) - 3600
	cache.add(metric, int64(base), 1)
	p.store(metric)

	p.aggregation.Default.xFilesFactor = 0.75
	p.EnableOnlineMigration(100, []string{"xff"})
	p.onlineMigration.ticker = helper.NewHardThrottleTicker(p.onlineMigration.rate)
	t.Cleanup(p.onlineMigration.ticker.Stop)
	select {
	case p.onlineMigration.ticker.C <- true:
	default:
	}

	cache.add(metric, int64(base+1), 2)
	done := make(chan struct{})
	go func() {
		p.store(metric)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("store deadlocked reopening a flocked file after online migration")
	}

	w, err := whisper.Open(path)
	if err != nil {
		t.Fatalf("open migrated file: %s", err)
	}
	defer w.Close()
	if got := w.XFilesFactor(); got != 0.75 {
		t.Errorf("xFilesFactor = %v; want 0.75", got)
	}
}
