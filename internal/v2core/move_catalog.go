package v2core

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/objstorage/objstorageprovider"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/go-graphite/go-carbon/internal/sealrollup"
	"github.com/go-graphite/go-carbon/internal/v2part"
)

// Move precedence belongs to the receiving catalog, never the immutable bytes.
// Unmarked original parts retain the existing single-owner live semantics.
const (
	CopiedPart uint8 = iota
	LivePart
	hiddenPart
)

var movePartPrefix = []byte{255, 'V', '2', 'M', 0}

type partImport struct {
	BaseOrder  uint64 `json:",omitempty"`
	Version    int
	Origin     partOrigin
	Descriptor v2part.Descriptor
	Pending    bool `json:",omitempty"`
	Owner      int  `json:",omitempty"`
	Owners     int  `json:",omitempty"`
}

type partOrigin struct {
	Class       uint8
	Incarnation v2part.PartID
	Stream      string
}

func partScopeKey(k v2part.Key) ([]byte, error) {
	prefix, err := v2part.PartitionPrefixForGroup(k.Group, k.VShard, k.Partition)
	if err != nil {
		return nil, err
	}
	return append(prefix, k.Part[:]...), nil
}

func moveCatalogKey(k v2part.Key) ([]byte, error) {
	k.VShard = 0 // Dense files span vshards; precedence binds the whole immutable part.
	scope, err := partScopeKey(k)
	if err != nil {
		return nil, err
	}
	return append(bytes.Clone(movePartPrefix), scope...), nil
}

func readPartOrigin(snapshot *pebble.Snapshot, k v2part.Key) (partOrigin, error) {
	key, err := moveCatalogKey(k)
	if err != nil {
		return partOrigin{}, err
	}
	b, closer, err := snapshot.Get(key)
	if errors.Is(err, pebble.ErrNotFound) {
		return partOrigin{Class: LivePart}, nil
	}
	if err != nil {
		return partOrigin{}, err
	}
	defer closer.Close()
	var binding partImport
	if err = json.Unmarshal(b, &binding); err != nil {
		return partOrigin{}, err
	}
	origin := binding.Origin
	if binding.Version != 1 || binding.Descriptor.Validate() != nil {
		return origin, errors.New("invalid copied part binding")
	}
	if binding.Descriptor.Scope.Group != k.Group || binding.Descriptor.Scope.Partition != k.Partition || binding.Descriptor.Scope.Part != k.Part {
		return origin, errors.New("copied part scope mismatch")
	}
	if origin.Class != CopiedPart || origin.Incarnation == (v2part.PartID{}) || origin.Stream == "" || origin.Incarnation != binding.Descriptor.Source.Incarnation || origin.Stream != binding.Descriptor.Source.Stream {
		return origin, errors.New("invalid copied-part precedence")
	}
	if binding.Pending {
		origin.Class = hiddenPart
	}
	return origin, nil
}

// newerOrigin resolves one physical slot: the newest logical timestamp wins.
// Class breaks equal timestamps; orders are comparable only within one source.
func newerOrigin(a partOrigin, ar sealrollup.Record, b partOrigin, br sealrollup.Record) (bool, error) {
	if ar.Sample.Time != br.Sample.Time {
		return ar.Sample.Time > br.Sample.Time, nil
	}
	return newerSource(a, ar.Order, b, br.Order)
}

// Metadata has no physical slot or logical timestamp. Its authority remains
// source-local, with the receiving catalog taking precedence over a donor.
func newerSource(a partOrigin, ao uint64, b partOrigin, bo uint64) (bool, error) {
	if a.Class != b.Class {
		return a.Class > b.Class, nil
	}
	if a != b {
		return false, errors.New("incomparable part source orders")
	}
	return ao > bo, nil
}

// InstallCopiedPart atomically ingests verified, unchanged SST bytes and their
// class-0 catalog entry. It is an OFFLINE read-store primitive: the caller owns
// exclusive installation, coherent inventory, lifecycle authority and identity
// adoption. Do not call it on Engine.DBs during intake: use PrepareMove and the
// serial producer's AdoptMove instead. Writable recovery refuses this catalog
// when it has no atomically installed receiving identity/reducer authority.
//
// Input path is retained by hard link; neither successful ingest nor retries
// consume it. A crash exposes both the data and precedence, or neither. Repeating
// a completed installation is a no-op. No point keys or values are rewritten.
func InstallCopiedPart(ctx context.Context, db *pebble.DB, dir, path string, d v2part.Descriptor) (bool, error) {
	return installCopiedPart(ctx, db, dir, path, d, false)
}

func installCopiedPart(ctx context.Context, db *pebble.DB, dir, path string, d v2part.Descriptor, pending bool) (bool, error) {
	return installCopiedPartVerified(ctx, db, dir, path, d, pending, false)
}

// VerifyMovePart certifies transport bytes and rollup semantics once before a durable receipt.
func VerifyMovePart(ctx context.Context, path string, d v2part.Descriptor) error {
	return validateCopiedRollupPart(ctx, path, d)
}

func installCopiedPartVerified(ctx context.Context, db *pebble.DB, dir, path string, d v2part.Descriptor, pending, verified bool) (bool, error) {
	if err := d.Validate(); err != nil {
		return false, err
	}
	if d.Scope.VShard != v2part.GroupVShard || d.Encoding != v2part.DenseEncoding {
		return false, errors.New("copied catalog requires a dense group rollup part")
	}
	if descriptorByID(d) {
		return false, errIDKeyedMove
	}
	k, err := v2part.Decode(d.FirstKey)
	if err != nil {
		return false, err
	}
	key, err := moveCatalogKey(k)
	if err != nil {
		return false, err
	}
	origin := partOrigin{Class: CopiedPart, Incarnation: d.Source.Incarnation, Stream: d.Source.Stream}
	value, err := json.Marshal(partImport{Version: 1, Origin: origin, Descriptor: d, Pending: pending})
	if err != nil {
		return false, err
	}
	// Catalog identity is stable across replay and cannot reclassify local data.
	if b, closer, e := db.Get(key); e == nil {
		same := bytes.Equal(b, value)
		e = closer.Close()
		if !same {
			return false, errors.New("copied part source changed")
		}
		return false, e
	} else if !errors.Is(e, pebble.ErrNotFound) {
		return false, e
	}
	prefix := []byte{'G', 'C', 'P', 3}
	it, err := db.NewIter(&pebble.IterOptions{LowerBound: prefix, UpperBound: v2part.PrefixEnd(prefix)})
	if err != nil {
		return false, err
	}
	exists := false
	for ok := it.First(); ok; ok = it.Next() {
		old, e := v2part.Decode(it.Key())
		if e != nil {
			_ = it.Close()
			return false, e
		}
		if old.Group == k.Group && old.Partition == k.Partition && old.Part == k.Part {
			exists = true
			break
		}
	}
	err = errors.Join(it.Error(), it.Close())
	if err != nil {
		return false, err
	}
	if exists {
		return false, errors.New("cannot reclassify an existing live part")
	}
	// SHA256 and all interior keys are checked before any visible mutation.
	if !verified {
		if err = validateCopiedRollupPart(ctx, path, d); err != nil {
			return false, err
		}
	}
	if err = ctx.Err(); err != nil {
		return false, err
	}
	if err = os.MkdirAll(dir, 0700); err != nil {
		return false, err
	}
	stage, err := os.MkdirTemp(dir, ".move-install-")
	if err != nil {
		return false, err
	}
	defer os.RemoveAll(stage)
	data := filepath.Join(stage, "data.sst")
	if err = os.Link(path, data); err != nil {
		return false, err
	}
	catalog := filepath.Join(stage, "catalog.sst")
	f, err := vfs.Default.Create(catalog)
	if err != nil {
		return false, err
	}
	w := sstable.NewWriter(objstorageprovider.NewFileWritable(f), sstable.WriterOptions{})
	if err = w.Set(key, value); err != nil {
		_ = w.Close()
		return false, err
	}
	if err = w.Close(); err != nil {
		return false, err
	}
	if err = ctx.Err(); err != nil {
		return false, err
	}
	if err = db.Ingest([]string{data, catalog}); err != nil {
		return false, fmt.Errorf("install copied part: %w", err)
	}
	return true, nil
}

// A valid transport checksum alone cannot certify archive/frontier semantics.
// This bounded pass validates values without reconstructing a name digest.
func validateCopiedRollupPart(ctx context.Context, path string, d v2part.Descriptor) error {
	v2part.RegisterPolicies(d.Coverage.Policies)
	type limits struct {
		order  uint64
		slots  map[uint32]int64
		meta   *RollupMetadata
		policy RollupPolicy
	}
	names := map[v2part.ID]*limits{}
	err := v2part.Read(ctx, path, &d, func(k v2part.Key, b []byte) error {
		if k.Ordinal != 0 {
			return errors.New("invalid copied rollup key")
		}
		entry := names[k.Metric]
		if entry == nil {
			entry = &limits{slots: map[uint32]int64{}}
			names[k.Metric] = entry
		}
		if k.Archive == v2part.MetadataArchive {
			m, decodeErr := DecodeRollupMetadata(k.Metric, k.Group, b)
			if err := decodeErr; err != nil {
				return err
			}
			if k.BlockStart != 0 || entry.meta != nil || (m.Version != 1 && m.Version != 2 && m.Version != 3) || m.ID == 0 || m.Frontier.Order == 0 || m.ResetOrder > m.Frontier.Order || (m.Version == 1 && (m.ResetOrder != 0 || m.Expired)) || (m.Expired && m.ResetOrder != m.Frontier.Order) {
				return errors.New("invalid copied rollup metadata")
			}
			if err := json.Unmarshal(m.Policy, &entry.policy); err != nil {
				return err
			}
			canonical, err := EncodeRollupPolicy(entry.policy)
			if err != nil {
				return err
			}
			if entry.policy.Name != k.Metric || entry.policy.Group != k.Group || !bytes.Equal(canonical, m.Policy) {
				return errors.New("copied rollup policy mismatch")
			}
			digest, err := sealrollup.PolicyDigest(entry.policy.Config)
			if err != nil {
				return err
			}
			if digest != m.Frontier.Policy || m.Frontier.Order < d.Source.MinOrder || m.Frontier.Order > d.Source.MaxOrder {
				return errors.New("copied rollup frontier mismatch")
			}
			entry.meta = &m
			return nil
		}
		records, err := v2part.DecodeSlots(k.Archive, k.BlockStart, b)
		if err != nil {
			return err
		}
		for _, r := range records {
			if r.Order < d.Source.MinOrder || r.Order > d.Source.MaxOrder {
				return errors.New("copied record outside source frontier")
			}
			entry.order = max(entry.order, r.Order)
			entry.slots[k.Archive] = max(entry.slots[k.Archive], r.Slot)
		}
		return nil
	})
	if err != nil {
		return err
	}
	for _, entry := range names {
		if entry.meta == nil || entry.order > entry.meta.Frontier.Order {
			return errors.New("copied record lacks its source metadata")
		}
		for archive, slot := range entry.slots {
			if uint64(archive) >= uint64(len(entry.policy.Config.Archives)) || slot >= entry.policy.Config.Archives[archive].Count {
				return errors.New("copied rollup slot outside policy")
			}
		}
	}
	return nil
}

func snapshotHasCopiedParts(snapshot *pebble.Snapshot) (bool, error) {
	it, err := snapshot.NewIter(&pebble.IterOptions{LowerBound: movePartPrefix, UpperBound: v2part.PrefixEnd(movePartPrefix)})
	if err != nil {
		return false, err
	}
	found := it.First()
	return found, errors.Join(it.Error(), it.Close())
}

// copiedNameVisible limits a shared immutable file to the receiving owner's
// canonical placement. Hard links let owners share the unchanged physical bytes.
func copiedNameVisible(snapshot *pebble.Snapshot, k v2part.Key) (bool, error) {
	key, err := moveCatalogKey(k)
	if err != nil {
		return false, err
	}
	b, closer, err := snapshot.Get(key)
	if errors.Is(err, pebble.ErrNotFound) {
		return true, nil
	}
	if err != nil {
		return false, err
	}
	defer closer.Close()
	var binding partImport
	if err := json.Unmarshal(b, &binding); err != nil {
		return false, err
	}
	if binding.Owners < 0 || binding.Owner < 0 || (binding.Owners > 0 && binding.Owner >= binding.Owners) {
		return false, errors.New("invalid copied owner placement")
	}
	return binding.Owners == 0 || int(v2part.Route(k.Metric))%binding.Owners == binding.Owner, nil
}

// The comparison class occupies one bit in the query's order space. This is
// deliberately below Tick's four archive bits, and is never written into an SST.
// Receiver writes, including pre-attach writes, win equal-time conflicts.
const moveLiveOrder = uint64(1) << 58

func moveVersionRecord(r sealrollup.Record, binding *partImport) (sealrollup.Record, error) {
	mapOrder := func(order uint64) (uint64, error) {
		if order == 0 {
			return 0, nil
		}
		if order >= moveLiveOrder {
			return 0, errors.New("move version order exhausted")
		}
		if binding == nil {
			return moveLiveOrder + order, nil
		}
		base := binding.BaseOrder
		if base == 0 {
			base = binding.Descriptor.Source.MinOrder
		}
		minOrder := binding.Descriptor.Source.MinOrder
		if base < minOrder || order >= moveLiveOrder-(base-minOrder) {
			return 0, errors.New("copied version order exhausted")
		}
		return base + order - minOrder, nil
	}
	var err error
	if r.Order, err = mapOrder(r.Order); err != nil {
		return r, err
	}
	r.Sample.Sequence = r.Order
	if r.Upto != 0 {
		order, e := mapOrder(r.Upto >> 4)
		if e != nil {
			return r, e
		}
		r.Upto = sealrollup.Tick(order, int(r.Upto&15))
	}
	return r, nil
}

// Allocate one receiving range per donor stream, retaining holes so bounds on
// older events (Upto) translate by the same offset as records. Bindings and the
// receiving identity are published in the same manifest edit.
func moveOrderBases(db *pebble.DB, parts []MovePart) (map[partOrigin]uint64, error) {
	bases := map[partOrigin]uint64{}
	next := uint64(0)
	it, err := db.NewIter(&pebble.IterOptions{LowerBound: movePartPrefix, UpperBound: v2part.PrefixEnd(movePartPrefix)})
	if err != nil {
		return nil, err
	}
	defer it.Close()
	for valid := it.First(); valid; valid = it.Next() {
		var b partImport
		if err = json.Unmarshal(it.Value(), &b); err != nil {
			return nil, err
		}
		if b.Pending || b.BaseOrder == 0 {
			continue
		}
		if b.BaseOrder < b.Descriptor.Source.MinOrder {
			return nil, errors.New("invalid copied order base")
		}
		offset := b.BaseOrder - b.Descriptor.Source.MinOrder
		if old, ok := bases[b.Origin]; ok && old != offset {
			return nil, errors.New("copied stream order range changed")
		}
		bases[b.Origin] = offset
		next = max(next, offset+b.Descriptor.Source.MaxOrder)
	}
	if err = it.Error(); err != nil {
		return nil, err
	}
	maxima := map[partOrigin]uint64{}
	var streams []partOrigin
	for _, part := range parts {
		d := part.Descriptor
		o := partOrigin{CopiedPart, d.Source.Incarnation, d.Source.Stream}
		if _, ok := maxima[o]; !ok {
			streams = append(streams, o)
		}
		maxima[o] = max(maxima[o], d.Source.MaxOrder)
	}
	for _, o := range streams {
		if _, ok := bases[o]; ok {
			continue
		}
		if maxima[o] >= moveLiveOrder-next {
			return nil, errors.New("copied order range exhausted")
		}
		bases[o] = next
		next += maxima[o]
	}
	return bases, nil
}
