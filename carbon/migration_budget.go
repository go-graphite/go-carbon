package carbon

import (
	"context"
	"encoding/json"
	"io"
	"os"
	"sync"
	"time"
)

// A local operator replaces this small file atomically. Short leases let a
// stopped controller fall back to one worker without restarting the traversal.
type migrationControl struct {
	Version    int    `json:"version"`
	Generation uint64 `json:"generation"`
	Workers    int    `json:"workers"`
	ExpiresAt  int64  `json:"expires_at"`
}

type migrationBudget struct {
	mu       sync.Mutex
	ready    *sync.Cond
	maximum  int
	limit    int
	active   int
	stopped  bool
	accepted migrationControl
}

func newMigrationBudget(maximum int) *migrationBudget {
	b := &migrationBudget{maximum: maximum, limit: 1}
	b.ready = sync.NewCond(&b.mu)
	return b
}

func (b *migrationBudget) acquire() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	for !b.stopped && b.active >= b.limit {
		b.ready.Wait()
	}
	if b.stopped {
		return false
	}
	b.active++
	return true
}

func (b *migrationBudget) release() {
	b.mu.Lock()
	b.active--
	b.ready.Signal()
	b.mu.Unlock()
}

func (b *migrationBudget) status() (workers, active int, generation uint64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.limit, b.active, b.accepted.Generation
}

func (b *migrationBudget) stop() {
	b.mu.Lock()
	b.stopped = true
	b.ready.Broadcast()
	b.mu.Unlock()
}

func (b *migrationBudget) refresh(path string, now time.Time) {
	var control migrationControl
	valid := false
	if info, err := os.Lstat(path); err == nil && info.Mode().IsRegular() && info.Size() <= 4096 {
		if f, err := os.Open(path); err == nil {
			data, readErr := io.ReadAll(io.LimitReader(f, 4097))
			closeErr := f.Close()
			valid = readErr == nil && closeErr == nil && len(data) <= 4096 && json.Unmarshal(data, &control) == nil &&
				control.Version == 1 && control.Generation != 0 && control.Workers >= 0 && control.Workers <= b.maximum &&
				control.ExpiresAt > now.Unix() && control.ExpiresAt <= now.Add(5*time.Minute).Unix()
		}
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	limit := 1
	if valid && (control.Generation > b.accepted.Generation || control == b.accepted) {
		b.accepted = control
		limit = control.Workers
	}
	if b.limit != limit {
		b.limit = limit
		b.ready.Broadcast()
	}
}

func (b *migrationBudget) run(ctx context.Context, path string) {
	defer b.stop()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			b.refresh(path, now)
		}
	}
}
