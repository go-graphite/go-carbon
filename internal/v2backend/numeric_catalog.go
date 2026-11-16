package v2backend

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"path/filepath"
	"sync"

	"github.com/go-graphite/go-carbon/internal/daemonstore"
	"github.com/go-graphite/go-carbon/internal/partlog"
	"github.com/go-graphite/go-carbon/internal/partsformat"
	"github.com/go-graphite/go-carbon/internal/v2core"
)

// One daemon-wide durable identity map. Stream-local PDC4 ordinals never enter
// the core. The mutex covers misses and durability, not resolved point intake.
// Journal records remain self-describing; seal syncs this map before publishing
// numeric data/frontiers. Replay may safely reassign an unsynced private suffix.
// Policies are interned once, rather than repeated in every metric definition.
type numericCatalog struct {
	mu            sync.Mutex
	log, policies *partlog.NameCatalog
	next          uint64
	byID          sync.Map
	closed        bool
}

func openNumericCatalog(s *Store) (*numericCatalog, error) {
	dir := filepath.Join(s.opts.DataDir, "global-catalog")
	log, err := partlog.OpenNameCatalog(dir)
	if err != nil {
		return nil, err
	}
	c := &numericCatalog{log: log}
	fail := func(err error) (*numericCatalog, error) {
		_ = log.Close()
		if c.policies != nil {
			_ = c.policies.Close()
		}
		return nil, err
	}
	c.policies, err = partlog.OpenNameCatalog(filepath.Join(dir, "policies"))
	if err != nil {
		return fail(err)
	}
	type policy struct {
		value daemonstore.Metric
		json  []byte
	}
	policies := make(map[uint64]policy)
	for ordinal := uint64(1); ordinal <= c.policies.Count(); ordinal++ {
		key, _, ok := c.policies.Lookup(ordinal)
		if !ok {
			return fail(errors.New("missing global numeric policy"))
		}
		var value daemonstore.Metric
		if err = json.Unmarshal([]byte(key), &value); err != nil {
			return fail(err)
		}
		if _, err = rollupConfig(value); err != nil {
			return fail(err)
		}
		policies[ordinal] = policy{value: value, json: []byte(key)}
	}
	for ordinal := uint64(1); ordinal <= log.Count(); ordinal++ {
		name, def, ok := log.Lookup(ordinal)
		if !ok || len(def) < 26 || string(def[:4]) != "V2N2" {
			return fail(errors.New("invalid global numeric catalog"))
		}
		id := binary.BigEndian.Uint64(def[4:12])
		vs := binary.BigEndian.Uint16(def[12:14])
		pid := binary.BigEndian.Uint64(def[14:22])
		gn := uint64(binary.BigEndian.Uint32(def[22:26]))
		p, ok := policies[pid]
		if !ok || id == 0 || gn == 0 || gn > partsformat.MaxNameBytes || gn != uint64(len(def)-26) || vs != partsformat.Route(partsformat.ID(name)) {
			return fail(errors.New("invalid global numeric identity"))
		}
		group := partsformat.Group(string(def[26:]))
		r := &resolvedMetric{name: name, globalID: id, owner: s.route(name), group: group, policy: p.value, prefix: numericPolicyPrefix(p.json, group)}
		current, err := s.opts.Policy(name)
		if err != nil {
			return fail(err)
		}
		if !samePolicy(current, r.policy) || v2core.Shard(id, s.opts.Shards) != r.owner%s.opts.Shards {
			return fail(errors.New("global catalog policy/geometry mismatch"))
		}
		if _, loaded := c.byID.LoadOrStore(id, r); loaded {
			return fail(errors.New("duplicate global numeric ID"))
		}
		s.resolved.Store(name, r)
		c.next = max(c.next, id)
	}
	return c, nil
}

func numericPolicyPrefix(policy []byte, group partsformat.Group) []byte {
	b := binary.BigEndian.AppendUint32([]byte{'V', '2', 'B', 1}, uint32(len(policy)))
	b = binary.BigEndian.AppendUint32(b, uint32(len(group)))
	b = append(b, policy...)
	return append(b, group...)
}

func (c *numericCatalog) intern(s *Store, r *resolvedMetric) (*resolvedMetric, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil, errors.New("global numeric catalog closed")
	}
	if old, ok := s.resolved.Load(r.name); ok && old.(*resolvedMetric).globalID != 0 {
		return old.(*resolvedMetric), nil
	}
	// Recovery-time receipt pointers are immutable and may still be in readers.
	// Publish a resolved replacement rather than mutating their private ID zero.
	copyMetric := *r
	r = &copyMetric
	// Choose the routing residue once; the value remains a unique global identity,
	// never a name hash. This retains uneven journal-owner/database geometry.
	for {
		c.next++
		if c.next == 0 {
			return nil, errors.New("global numeric ID exhausted")
		}
		if v2core.Shard(c.next, s.opts.Shards) == r.owner%s.opts.Shards {
			break
		}
	}
	r.globalID = c.next
	n := binary.BigEndian.Uint32(r.prefix[4:8])
	policyID, err := c.policies.Intern(string(r.prefix[12:12+n]), nil)
	if err != nil {
		return nil, err
	}
	def := binary.BigEndian.AppendUint64([]byte("V2N2"), r.globalID)
	def = binary.BigEndian.AppendUint16(def, partsformat.Route(partsformat.ID(r.name)))
	def = binary.BigEndian.AppendUint64(def, policyID)
	def = binary.BigEndian.AppendUint32(def, uint32(len(r.group)))
	def = append(def, r.group...)
	if _, err = c.log.Intern(r.name, def); err != nil {
		return nil, err
	}
	c.byID.Store(r.globalID, r)
	s.resolved.Store(r.name, r)
	return r, nil
}

func (c *numericCatalog) syncLocked() error {
	// A metric's referenced policy becomes durable first. The journal does not
	// reference either numeric catalog, so its independent group sync is safe.
	if err := c.policies.Sync(); err != nil {
		return err
	}
	return c.log.Sync()
}
func (c *numericCatalog) sync() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return errors.New("global numeric catalog closed")
	}
	return c.syncLocked()
}
func (c *numericCatalog) close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil
	}
	c.closed = true
	return errors.Join(c.syncLocked(), c.log.Close(), c.policies.Close())
}
