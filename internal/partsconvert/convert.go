// Package partsconvert builds verified immutable history parts from a fenced,
// read-only legacy source. It does not recompute aggregates or delete history.
package partsconvert

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"unicode/utf8"

	"github.com/go-graphite/go-carbon/internal/partsformat"
)

type Unit struct {
	Group  string
	VShard uint16
	// Optional catalogue batch [Lower,Upper). Final marks the last unbounded
	// range; empty bounds with Final false preserve whole-vshard conversion.
	Lower, Upper string
	Final        bool
}

// Record contains an already resolved archive block, including coarse values.
// Value is copied byte for byte. Key.Part is assigned by the converter. Records
// within a Scan must have unique (partition, metric, archive, start, ordinal).
type Record struct {
	Key    partsformat.Key
	Value  []byte
	Points uint64
}

type Source interface {
	// Identity binds the immutable snapshot, cutover frontier, policies and codec.
	Identity() string
	// Estimate is an upper bound for all output SST bytes for the unit, including
	// table overhead. Underestimates abort conversion before ingestion.
	Estimate(context.Context, Unit) (uint64, error)
	Scan(context.Context, Unit, func(Record) error) error
}

type Target interface {
	// Identity is the durable destination incarnation, not just its path.
	Identity() string
	// Ingest is atomic for all tables, durable on return, and may be retried
	// after an ambiguous crash. Reingestion of these exact keys must be safe.
	Ingest(context.Context, []Table) error
	// Verify reads durable target records, not the staging SSTs.
	Verify(context.Context, []Table) error
}

// Publisher must durably and idempotently install this exact receipt, and pin
// old reader generations before allowing reclamation. The converter has no
// authority to delete a source. A published receipt alone is not a GC fence.
type Publisher interface {
	Publish(context.Context, Receipt) error
}

// Reclaimer owns legacy deletion. It must wait for old reader/source pins,
// durably delete the actual legacy ID/bucket spans, and wait until their
// physical disk space is reclaimable before returning. Tombstones alone are
// insufficient. The operation must be idempotent across an ambiguous crash.
// It cannot mutate a source concurrently opened with Pebble's ReadOnly LOCK.
type Reclaimer interface {
	Reclaim(context.Context, Receipt) error
}

type Reservation interface{ Reserve(uint64) (func(), error) }

type Converter struct {
	Directory string
	Source    Source
	Target    Target
	Publisher Publisher
	Reclaimer Reclaimer
	// Required with Reclaimer: reject a unit larger than this output bound.
	// The scheduler must additionally charge cumulative target-size growth.
	MaxBatchBytes uint64
	Space         Reservation
	// ChunkBytes bounds copied key/value bytes per sort buffer. A source block
	// larger than this limit fails loudly rather than exceeding the bound.
	ChunkBytes int
	MaxTables  int
	mu         sync.Mutex
	active     map[Unit]bool
}

// Receipt is the resumable per-vshard cursor. The ready state is written only
// after durable target readback; published is written after the routing callback.
// A crash between either action and its journal write safely repeats that action.
type Receipt struct {
	Version     int
	Source      string
	Destination string
	Unit        Unit
	Token       string
	State       string
	Tables      []Table
	Points      uint64
	OutputBytes uint64
}

func (c *Converter) Run(ctx context.Context, u Unit) (Receipt, error) {
	var zero Receipt
	if c.Source == nil || c.Target == nil || c.Space == nil || c.Directory == "" || c.ChunkBytes < 1024 || c.MaxTables < 1 || u.VShard >= partsformat.VirtualShards || c.Source.Identity() == "" || c.Target.Identity() == "" {
		return zero, errors.New("invalid conversion configuration")
	}
	if !utf8.ValidString(c.Source.Identity()) || !utf8.ValidString(c.Target.Identity()) || !utf8.ValidString(c.Directory) {
		return zero, errors.New("conversion identities/directory must be UTF-8; hash opaque identities first")
	}
	if c.Reclaimer != nil && (c.Publisher == nil || c.MaxBatchBytes == 0) {
		return zero, errors.New("reclamation requires verified route publication and a batch byte bound")
	}
	if u.Upper != "" && (u.Upper <= u.Lower || u.Final) {
		return zero, errors.New("invalid name batch bounds")
	}
	c.mu.Lock()
	if c.active == nil {
		c.active = make(map[Unit]bool)
	}
	if c.active[u] {
		c.mu.Unlock()
		return zero, errors.New("conversion already running for vshard")
	}
	c.active[u] = true
	c.mu.Unlock()
	defer func() { c.mu.Lock(); delete(c.active, u); c.mu.Unlock() }()
	dir := filepath.Join(c.Directory, unitName(u))
	if err := makeDurableDirectory(dir); err != nil {
		return zero, err
	}
	// A process-level flock also excludes a second converter using this journal.
	unlock, err := lockDirectory(dir)
	if err != nil {
		return zero, err
	}
	defer unlock()
	r, err := readReceipt(dir)
	if errors.Is(err, os.ErrNotExist) {
		var token [16]byte
		if _, err = rand.Read(token[:]); err != nil {
			return zero, err
		}
		r = Receipt{Version: 1, Source: c.Source.Identity(), Destination: c.Target.Identity(), Unit: u, Token: hex.EncodeToString(token[:]), State: "building"}
		err = writeReceipt(dir, r)
	}
	if err != nil {
		return zero, err
	}
	if r.Version != 1 || r.Source != c.Source.Identity() || r.Destination != c.Target.Identity() || r.Unit != u {
		return zero, errors.New("conversion snapshot/destination/format identity mismatch")
	}
	if err := validateReceipt(r, dir); err != nil {
		return zero, err
	}
	if r.State == "reclaimed" {
		return r, cleanStage(dir)
	}
	if r.State == "published" && c.Reclaimer == nil {
		return r, nil
	}
	if err := ctx.Err(); err != nil {
		return r, err
	}
	if r.State == "building" {
		bound, err := c.Source.Estimate(ctx, u)
		if err != nil {
			return r, err
		}
		if c.Reclaimer != nil && bound > c.MaxBatchBytes {
			return r, errors.New("conversion unit exceeds reclaim batch byte bound")
		}
		if bound > ^uint64(0)/2 {
			return r, errors.New("conversion size bound overflows")
		}
		// Same-filesystem ingest normally links SSTs. Reserve an additional
		// complete copy as well, so a cross-filesystem target remains safe.
		rawRelease, err := c.Space.Reserve(bound * 2)
		if err != nil {
			return r, err
		}
		release := sync.OnceFunc(rawRelease)
		defer release()
		// Only this unit's uncommitted staging files may be removed on restart.
		if err := cleanStage(dir); err != nil {
			return r, err
		}
		r.Tables, r.Points, r.OutputBytes = nil, 0, 0
		var buffer []Record
		used := 0
		flush := func() error {
			if len(buffer) == 0 {
				return nil
			}
			tables, err := buildTables(dir, r.Token, len(r.Tables), c.MaxTables-len(r.Tables), buffer)
			if err != nil {
				return err
			}
			for _, t := range tables {
				r.OutputBytes += t.Bytes
			}
			if r.OutputBytes > bound {
				return fmt.Errorf("output %d exceeds reserved upper bound %d", r.OutputBytes, bound)
			}
			r.Tables = append(r.Tables, tables...)
			if len(r.Tables) > c.MaxTables {
				return errors.New("conversion exceeds table manifest bound")
			}
			buffer, used = nil, 0
			return nil
		}
		err = c.Source.Scan(ctx, u, func(rec Record) error {
			if err := ctx.Err(); err != nil {
				return err
			}
			if rec.Key.VShard != u.VShard {
				return errors.New("source emitted another vshard")
			}
			if string(rec.Key.Metric) < u.Lower || (u.Upper != "" && string(rec.Key.Metric) >= u.Upper) {
				return errors.New("source emitted outside name batch")
			}
			if err := validateGroup(rec.Key, u.Group); err != nil {
				return err
			}
			key, err := partsformat.Encode(rec.Key)
			if err != nil {
				return err
			}
			n := len(key) + len(rec.Value) + 128
			if n > c.ChunkBytes {
				return errors.New("source block exceeds chunk byte bound")
			}
			if used+n > c.ChunkBytes {
				if err := flush(); err != nil {
					return err
				}
			}
			rec.Value = bytes.Clone(rec.Value)
			buffer = append(buffer, rec)
			used += n
			r.Points += rec.Points
			return nil
		})
		if err == nil {
			err = flush()
		}
		if err != nil {
			return r, err
		}
		r.State = "prepared"
		if err = writeReceipt(dir, r); err != nil {
			return r, err
		}
		release()
	}
	if r.State == "prepared" {
		release, err := c.Space.Reserve(r.OutputBytes)
		if err != nil {
			return r, err
		}
		defer release()
		// Retain original SST links until publication: an ambiguous ingest can
		// be replayed after a crash without reading or rebuilding legacy data.
		if err := c.Target.Ingest(ctx, r.Tables); err != nil {
			return r, fmt.Errorf("ingest: %w", err)
		}
		r.State = "ingested"
		if err := writeReceipt(dir, r); err != nil {
			return r, err
		}
	}
	if r.State == "ingested" {
		if err := c.Target.Verify(ctx, r.Tables); err != nil {
			return r, fmt.Errorf("verify: %w", err)
		}
		r.State = "ready"
		if err := writeReceipt(dir, r); err != nil {
			return r, err
		}
	}
	if c.Publisher != nil && r.State == "ready" {
		if err := c.Publisher.Publish(ctx, r); err != nil {
			return r, fmt.Errorf("publish: %w", err)
		}
		r.State = "published"
		if err := writeReceipt(dir, r); err != nil {
			return r, err
		}
	}
	if c.Reclaimer != nil && r.State == "published" {
		if err := c.Reclaimer.Reclaim(ctx, r); err != nil {
			return r, fmt.Errorf("reclaim legacy: %w", err)
		}
		r.State = "reclaimed"
		if err := writeReceipt(dir, r); err != nil {
			return r, err
		}
		if err := cleanStage(dir); err != nil {
			return r, err
		}
	}
	return r, nil
}
