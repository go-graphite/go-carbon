package carbon

import (
	"encoding/json"
	"sync"
	"sync/atomic"

	"github.com/go-graphite/go-carbon/internal/v2core"
)

// Owners already serialize successful publication callbacks. A sparse bit per
// journal identity suppresses repeated pending-head names without a second
// string-keyed all-name map. The queue contains membership changes only.
type v2CatalogUpdates struct {
	owners     []map[uint64]*[16]uint64
	mu         sync.Mutex
	added      []string
	expired    []string
	expiredIDs sync.Map
	hasExpiry  atomic.Bool
}

func newV2CatalogUpdates(owners int) *v2CatalogUpdates {
	u := &v2CatalogUpdates{owners: make([]map[uint64]*[16]uint64, owners)}
	for i := range u.owners {
		u.owners[i] = make(map[uint64]*[16]uint64)
	}
	return u
}
func (u *v2CatalogUpdates) visible(owner int, batches []v2core.Batch) {
	var added []string
	for _, b := range batches {
		page := u.owners[owner][b.ID>>10]
		if page == nil {
			page = new([16]uint64)
			u.owners[owner][b.ID>>10] = page
		}
		word, bit := (b.ID&1023)>>6, uint64(1)<<(b.ID&63)
		if page[word]&bit != 0 {
			if !u.hasExpiry.Load() {
				continue
			}
			if _, ok := u.expiredIDs.LoadAndDelete(b.ID); !ok {
				continue
			}
		}
		page[word] |= bit
		var name string
		if b.Registered != nil {
			name = b.Registered.Name()
		} else {
			var policy v2core.RollupPolicy
			// The core validated this canonical policy before publishing it.
			if err := json.Unmarshal(b.Policy, &policy); err != nil {
				panic(err)
			}
			name = string(policy.Name)
		}
		added = append(added, name)
	}
	if len(added) > 0 {
		u.mu.Lock()
		u.added = append(u.added, added...)
		u.mu.Unlock()
	}
}
func (u *v2CatalogUpdates) expire(id uint64, name string) {
	u.expiredIDs.Store(id, true)
	u.hasExpiry.Store(true)
	u.mu.Lock()
	u.expired = append(u.expired, name)
	u.mu.Unlock()
}
func (u *v2CatalogUpdates) drain() (added, expired []string) {
	u.mu.Lock()
	defer u.mu.Unlock()
	added, expired = u.added, u.expired
	u.added, u.expired = nil, nil
	return
}
