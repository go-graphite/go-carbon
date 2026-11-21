package carbon

import (
	"encoding/json"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-graphite/go-carbon/internal/v2core"
)

// Owners already serialize successful publication callbacks. A sparse bit per
// journal identity suppresses repeated pending-head names without a second
// string-keyed all-name map. The queue contains membership changes only.
type v2CatalogUpdates struct {
	owners     []map[uint64]*[16]uint64
	mu         sync.Mutex
	added      []string
	stamps     []v2NameStamp // aligned with added
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
func (u *v2CatalogUpdates) visible(batches []v2core.Batch) {
	u.visiblePending(0, v2core.PendingBatches(batches))
}

// visiblePending registers names newly visible in an owner's pending view.
func (u *v2CatalogUpdates) visiblePending(owner int, pending v2core.PendingView) {
	var added []string
	var stamps []v2NameStamp
	now := time.Now().UnixNano()
	pending.Each(func(b *v2core.PendingApplication) bool {
		page := u.owners[owner][b.ID>>10]
		if page == nil {
			page = new([16]uint64)
			u.owners[owner][b.ID>>10] = page
		}
		word, bit := (b.ID&1023)>>6, uint64(1)<<(b.ID&63)
		if page[word]&bit != 0 {
			if !u.hasExpiry.Load() {
				return true
			}
			if _, ok := u.expiredIDs.LoadAndDelete(b.ID); !ok {
				return true
			}
		}
		page[word] |= bit
		var name string
		if h := b.Registered(); h != nil {
			name = h.Name()
		} else {
			var policy v2core.RollupPolicy
			// The core validated this canonical policy before publishing it.
			if err := json.Unmarshal(b.Policy, &policy); err != nil {
				panic(err)
			}
			name = string(policy.Name)
		}
		added = append(added, name)
		stamps = append(stamps, v2NameStamp{registered: b.Clock, visible: now})
		return true
	})
	if len(added) > 0 {
		u.mu.Lock()
		u.added = append(u.added, added...)
		u.stamps = append(u.stamps, stamps...)
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

// v2CatalogChunk bounds the names one catalog publication takes, so a
// fresh store's multi-million-name backlog becomes visible in many short
// publications instead of one long clone, sort and merge.
const v2CatalogChunk = 256 << 10

// drain takes up to limit added and limit expired names (0 = all) and reports
// whether more are queued. The remainder keeps its arrival order.
func (u *v2CatalogUpdates) drain(limit int) (added []string, stamps []v2NameStamp, expired []string, more bool) {
	u.mu.Lock()
	defer u.mu.Unlock()
	if limit <= 0 || len(u.added) <= limit {
		added, stamps = u.added, u.stamps
		u.added, u.stamps = nil, nil
	} else {
		// Full slice expressions: callers append to the returned slices.
		added, stamps = u.added[:limit:limit], u.stamps[:limit:limit]
		u.added, u.stamps = u.added[limit:], u.stamps[limit:]
	}
	if limit <= 0 || len(u.expired) <= limit {
		expired = u.expired
		u.expired = nil
	} else {
		expired = u.expired[:limit:limit]
		u.expired = u.expired[limit:]
	}
	return added, stamps, expired, len(u.added) > 0 || len(u.expired) > 0
}

// Copied names and expiry candidates both need committed metadata confirmation.
func (u *v2CatalogUpdates) changed(names []string) {
	u.mu.Lock()
	u.expired = append(u.expired, names...)
	u.mu.Unlock()
}

// Stamps cover a prefix of added; reconfirmed copy/expiry names have none.
func (u *v2CatalogUpdates) retry(added []string, stamps []v2NameStamp, expired []string) {
	u.mu.Lock()
	u.added = append(u.added, added...)
	u.stamps = append(u.stamps, stamps...)
	for len(u.stamps) < len(u.added) {
		u.stamps = append(u.stamps, v2NameStamp{})
	}
	u.expired = append(u.expired, expired...)
	u.mu.Unlock()
}

// v2NameStamp attributes new-name delay: registration clock (whole seconds,
// from the admitted batch) and reader-visible time (publication callback).
type v2NameStamp struct{ registered, visible int64 }

type v2NameTrace struct {
	Name                string `json:"name"`
	RegisteredUnix      int64  `json:"registered_unix"`
	ReaderVisibleUnixMs int64  `json:"reader_visible_unix_ms"`
	PublishedUnixMs     int64  `json:"catalog_published_unix_ms"`
}

// v2CatalogTelemetry observes the single catalog owner; it never gates it.
type v2CatalogTelemetry struct {
	visibleToPublished, registeredToPublished v2WriteHistogram
	mu                                        sync.Mutex
	publications                              uint64
	publishSeconds, publishMax                float64
	levels, names                             int
	recent                                    [256]v2NameTrace
	next                                      int
	probes                                    [256]v2NameTrace
	probeNext                                 int
}

// v2ProbeMarker selects synthetic visibility-probe names; they get their own
// trace ring so a busy stream cannot overwrite them within milliseconds.
const v2ProbeMarker = ".visibility."

func (t *v2CatalogTelemetry) published(start time.Time, added []string, stamps []v2NameStamp, levels, names int) {
	now := time.Now()
	t.mu.Lock()
	defer t.mu.Unlock()
	d := now.Sub(start).Seconds()
	t.publications++
	t.publishSeconds += d
	t.publishMax = max(t.publishMax, d)
	t.levels, t.names = levels, names
	for i, s := range stamps {
		if s.visible == 0 {
			continue
		}
		t.visibleToPublished.add(now.Sub(time.Unix(0, s.visible)), 1)
		t.registeredToPublished.add(now.Sub(time.Unix(s.registered, 0)), 1)
		trace := v2NameTrace{added[i], s.registered, s.visible / 1e6, now.UnixMilli()}
		if strings.Contains(added[i], v2ProbeMarker) {
			t.probes[t.probeNext] = trace
			t.probeNext = (t.probeNext + 1) % len(t.probes)
			continue
		}
		t.recent[t.next] = trace
		t.next = (t.next + 1) % len(t.recent)
	}
}

func (t *v2CatalogTelemetry) report() map[string]interface{} {
	t.mu.Lock()
	defer t.mu.Unlock()
	recent := make([]v2NameTrace, 0, len(t.recent))
	for i := range t.recent {
		if r := t.recent[(t.next+i)%len(t.recent)]; r.Name != "" {
			recent = append(recent, r)
		}
	}
	for i := range t.probes {
		if r := t.probes[(t.probeNext+i)%len(t.probes)]; r.Name != "" {
			recent = append(recent, r)
		}
	}
	return map[string]interface{}{
		"publications": t.publications, "publish_seconds_total": t.publishSeconds, "publish_seconds_max": t.publishMax,
		"levels": t.levels, "names": t.names, "recent": recent,
		"reader_visible_to_published": t.visibleToPublished.report(), "registered_to_published": t.registeredToPublished.report(),
	}
}
