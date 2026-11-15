package downsize

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

type restartWindowWatch struct {
	*fakeSync
	window  syncRange
	mu      sync.Mutex
	outside int
	fetched []string
}

func (b *restartWindowWatch) Digest(ctx context.Context, host string, names []string, from, until int64) (map[string][]syncDigest, error) {
	b.mu.Lock()
	if from < b.window.From || until > b.window.Until {
		b.outside++
	}
	b.mu.Unlock()
	return b.fakeSync.Digest(ctx, host, names, from, until)
}
func (b *restartWindowWatch) Fetch(ctx context.Context, host string, names []string, from, until int64) (map[string]syncSeries, error) {
	b.mu.Lock()
	if from < b.window.From || until > b.window.Until {
		b.outside++
	}
	b.fetched = append(b.fetched, names...)
	b.mu.Unlock()
	return b.fakeSync.Fetch(ctx, host, names, from, until)
}
func TestRestartWindowProofBoundsAndEmptyNames(t *testing.T) {
	for _, legacy := range []bool{false, true} {
		for _, hole := range []bool{false, true} {
			t.Run(fmt.Sprintf("legacy=%v/hole=%v", legacy, hole), func(t *testing.T) {
				f := newFakeSync()
				w := syncRange{"restart", syncTestNow - 600, syncTestNow - 120}
				schema := []syncArchive{{60, 10000}}
				for _, host := range []string{syncT, syncD1, syncD2} {
					f.put(host, "active", schema)
					f.put(host, "inactive", schema)
					f.store(host).points["inactive"][60] = map[int64]float64{w.From - 600: 1}
					f.noDigest[host] = legacy
				}
				// An absent target with no donor data in the gap is already proven for
				// that gap, regardless of its historical donor data or serving index.
				if !legacy {
					delete(f.store(syncT).archives, "inactive")
					delete(f.store(syncT).points, "inactive")
				}
				if hole {
					delete(f.store(syncT).points["active"][60], w.From)
				}
				b := &restartWindowWatch{fakeSync: f, window: w}
				o := syncOptions{to: syncT, from: []string{syncD1, syncD2}, state: t.TempDir(), batch: 10, streams: 1, maxRounds: 1, windows: syncRanges{w}, restartWindow: true, now: func() time.Time { return time.Unix(syncTestNow, 0) }}
				err := runSync(context.Background(), o, b)
				if errors.Is(err, ErrSyncResidual) != hole || err != nil && !errors.Is(err, ErrSyncResidual) {
					t.Fatalf("hole=%v verdict: %v", hole, err)
				}
				if b.outside != 0 {
					t.Fatalf("%d reads escaped restart gap", b.outside)
				}
				if !legacy {
					for _, n := range b.fetched {
						if n == "inactive" {
							t.Fatal("empty donor window caused a point read")
						}
					}
				}
			})
		}
	}
}
func TestRestartWindowRejectsFullHistory(t *testing.T) {
	s := syncer{o: syncOptions{restartWindow: true, fullHistory: true}, b: newFakeSync()}
	if err := s.verifyNames(context.Background(), &syncRow{}, []string{"a"}); err == nil {
		t.Fatal("accepted unbounded restart proof")
	}
}

type restartNewHolder struct {
	*fakeSync
	window syncRange
	once   sync.Once
}

func (b *restartNewHolder) Info(ctx context.Context, host string, names []string) (map[string][]syncArchive, error) {
	if host == syncT {
		b.once.Do(func() {
			b.mu.Lock()
			defer b.mu.Unlock()
			b.store(syncD2).archives["inactive"] = []syncArchive{{60, 10000}}
			b.store(syncD2).points["inactive"] = map[int64]map[int64]float64{60: {b.window.From: 42}}
		})
	}
	return b.fakeSync.Info(ctx, host, names)
}
func TestRestartWindowChecksHolderCreatedAfterDiscovery(t *testing.T) {
	f := newFakeSync()
	w := syncRange{"restart", syncTestNow - 600, syncTestNow - 120}
	for _, host := range []string{syncT, syncD1} {
		f.store(host).archives["inactive"] = []syncArchive{{60, 10000}}
		f.store(host).points["inactive"] = map[int64]map[int64]float64{60: {}}
	}
	b := &restartNewHolder{fakeSync: f, window: w}
	o := syncOptions{to: syncT, from: []string{syncD1, syncD2}, state: t.TempDir(), batch: 10, streams: 1, noCopy: true, windows: syncRanges{w}, restartWindow: true, now: func() time.Time { return time.Unix(syncTestNow, 0) }}
	if err := runSync(context.Background(), o, b); !errors.Is(err, ErrSyncResidual) {
		t.Fatalf("new donor hole was missed: %v", err)
	}
}
