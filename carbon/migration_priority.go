package carbon

import (
	"context"
	"strings"
	"sync"
)

const migrationPriorityCapacity = 4096

// Hints taken before the next cold-scan path, unless configured otherwise.
const migrationPriorityBurst = 3

// Hints only change traversal order. The full walk remains authoritative and
// covers dropped hints. Pending includes in-flight hints to bound retained names
// and avoid repeated work when one source receives many writes during import.
type migrationPriority struct {
	mu      sync.Mutex
	ready   chan string
	pending map[string]struct{}
	// burst is the number of consecutive hints preferred over the cold scan:
	// zero converts cold paths first, a large value converts hints first.
	burst int
}

func newMigrationPriority() *migrationPriority {
	return newMigrationPriorityBurst(migrationPriorityBurst)
}

func newMigrationPriorityBurst(burst int) *migrationPriority {
	return &migrationPriority{
		ready:   make(chan string, migrationPriorityCapacity),
		pending: make(map[string]struct{}),
		burst:   burst,
	}
}

func (p *migrationPriority) offer(name string) {
	if len(name) == 0 || len(name) > 4096 || !p.mu.TryLock() {
		return
	}
	defer p.mu.Unlock()
	if len(p.pending) == migrationPriorityCapacity {
		return
	}
	if _, exists := p.pending[name]; exists {
		return
	}
	name = strings.Clone(name)
	p.pending[name] = struct{}{}
	// Every queued item is also pending, so admission above guarantees space.
	p.ready <- name
}

func (p *migrationPriority) complete(name string) {
	p.mu.Lock()
	delete(p.pending, name)
	p.mu.Unlock()
}

func (p *migrationPriority) runWorker(ctx context.Context, cold <-chan string, admit func() bool, release func(), convert func(string) error) error {
	streak := 0
	pendingCold := ""
	for {
		name, hot, ok := pendingCold, false, true
		pendingCold = ""
		if name == "" {
			name, hot, ok = p.next(ctx, cold, streak)
		}
		if !ok {
			return ctx.Err()
		}
		if hot {
			// A paused worker must leave hints visible to the next admitted
			// worker. Pending includes this name, so returning it cannot exceed
			// channel capacity even when writers offer more hints concurrently.
			p.ready <- name
			name = ""
		}
		if !admit() {
			return ctx.Err()
		}
		if name == "" {
			select {
			case name = <-p.ready:
			default:
				// Another admitted worker already consumed the notification.
				release()
				continue
			}
		} else if streak < p.burst {
			select {
			case hint := <-p.ready:
				pendingCold, name, hot = name, hint, true
			default:
			}
		}
		err := convert(name)
		release()
		if hot {
			p.complete(name)
			streak++
		} else {
			streak = 0
		}
		if err != nil {
			return err
		}
	}
}

// next prefers three hints before checking the cold scan. Workers recheck hints
// after admission so a pause cannot leave an old cold job ahead of a newer hint.
// A closed cold scan means all source paths have already been offered;
// residual hints are optional and never keep a completed conversion running.
func (p *migrationPriority) next(ctx context.Context, cold <-chan string, streak int) (name string, priority, ok bool) {
	if ctx.Err() != nil {
		return "", false, false
	}
	if streak < p.burst {
		select {
		case name := <-p.ready:
			return name, true, true
		default:
		}
	}
	select {
	case name, ok := <-cold:
		return name, false, ok
	default:
	}
	select {
	case <-ctx.Done():
		return "", false, false
	case name := <-p.ready:
		return name, true, true
	case name, ok := <-cold:
		return name, false, ok
	}
}
