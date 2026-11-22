package v2daemon

import (
	"bufio"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"syscall"

	"github.com/go-graphite/go-carbon/internal/partlog"
	"github.com/go-graphite/go-carbon/internal/v2replay"
)

type restartGeometry struct{ Version, Layer, Producers, Shards int }

// Version 3 binds producer ownership and fingerprints to XXH3-64. Older
// journals and catalogues must be rebuilt, never replayed under another hash.
const restartVersion = 3

// lockRoot prevents two writable opens, including before core has opened its DBs.
// Geometry is independent of tuning knobs and survives a process crash.
func lockRoot(root string, c Config) (*os.File, bool, error) {
	f, err := os.OpenFile(filepath.Join(root, "restart.lock"), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, false, err
	}
	fail := func(err error) (*os.File, bool, error) { _ = f.Close(); return nil, false, err }
	if err = syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		return fail(err)
	}
	path := filepath.Join(root, "restart.json")
	want := restartGeometry{restartVersion, c.Layer, c.Producers, c.Shards}
	b, err := os.ReadFile(path)
	if err == nil {
		var got restartGeometry
		if json.Unmarshal(b, &got) != nil || got.Version != restartVersion {
			return fail(errors.New("unsupported restart format version"))
		}
		if got != want {
			return fail(errors.New("restart geometry mismatch"))
		}
		if c.Layer < 2 {
			return fail(errors.New("restart requires durable journal layer"))
		}
		if err = validateMoveRecords(root); err != nil {
			return fail(err)
		}
		return f, true, nil
	}
	if !os.IsNotExist(err) {
		return fail(err)
	}
	// An older experiment has no restart contract or exact L2 sequences.
	if _, err = os.Stat(filepath.Join(root, "experiment.lock")); !os.IsNotExist(err) {
		return fail(errors.New("legacy experiment has no lossless restart contract"))
	}
	b, _ = json.Marshal(want)
	out, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return fail(err)
	}
	_, err = out.Write(b)
	err = errors.Join(err, out.Sync(), out.Close(), syncDir(root))
	if err != nil {
		return fail(err)
	}
	return f, false, nil
}

// recoverProducer ports the earlier loggedCore replay loop: ordered per-stream
// decode -> the same AppendBatches queue -> synchronous final seal. Owners alone
// decide which packets were already committed. Every segment resets predictors.
func (p *Producer) recoverProducer(ctx context.Context, dir string) error {
	paths, err := journalSegments(dir)
	if err != nil {
		return err
	}
	// The journal layer keeps every segment (one per process run) and replays
	// them all; core skips input its frontiers already cover.
	p.journalDir = dir
	if err := p.openCatalog(); err != nil {
		return err
	}
	for i, path := range paths {
		decoder := partlog.CatalogDecoder{Catalog: p.catalog}
		_, err = v2replay.Scan(path, i == len(paths)-1, func(record []byte) error {
			if err := ctx.Err(); err != nil {
				return err
			}
			inner, stamp, err := decodePlacementEnvelope(record)
			if err != nil {
				return err
			}
			if p.placement != nil {
				if stamp == nil {
					return errors.New("placement stamp missing from retained journal")
				}
				if err = p.placement.Replay(*stamp); err != nil {
					return fmt.Errorf("journal placement authority: %w", err)
				}
			} else if stamp != nil {
				return errors.New("placed journal requires placement authority")
			}
			if stamp != nil && stamp.RelaySequence != 0 {
				if err := p.rememberRelay(*stamp); err != nil {
					return err
				}
			}
			batches, cursor, err := DecodeApplications(inner, &decoder, p.index, p.count)
			if err != nil {
				return err
			}
			if p.placement != nil {
				names := make([]string, len(batches))
				for j, batch := range batches {
					id := (batch.ID-uint64(p.index)-1)/uint64(p.count) + 1
					name, _, ok := p.catalog.Lookup(id)
					if !ok {
						return errors.New("journal placement name missing")
					}
					names[j] = name
				}
				if err = p.placement.CheckNames(names); err != nil {
					return fmt.Errorf("journal placement names: %w", err)
				}
			}
			var n uint64
			for _, b := range batches {
				n += uint64(len(b.Points))
			}
			// Packet boundaries are durable cursor boundaries. Never split an original
			// application to accommodate a partially committed owner vector.
			if cursor <= p.cursor {
				return errors.New("repeated or regressing journal cursor")
			}
			if cursor-p.cursor != n {
				return errors.New("journal cursor gap")
			}
			for _, b := range batches {
				if b.Order <= p.applyOrder {
					return errors.New("journal application order regressed")
				}
				p.applyOrder = b.Order
			}
			if err = p.engine.AppendBatchesPipelined(p.index, batches, cursor, false); err != nil {
				return err
			}
			p.cursor = cursor
			return nil
		})
		decoder.Close()
		if err != nil {
			return err
		}
	}
	// A frontier beyond the retained journal would silently reuse cursors.
	for _, frontier := range p.engine.DurableFrontiers() {
		if frontier[p.index] > p.cursor {
			return errors.New("core frontier exceeds retained journal")
		}
	}
	// No replay means the frontiers already cover every record. An empty seal
	// here would rewrite owner checkpoints without making any input durable.
	if p.cursor > 0 && len(paths) > 0 {
		if err = p.engine.AppendBatches(p.index, nil, p.cursor, true); err != nil {
			return err
		}
	}
	if len(paths) > 0 && filepath.Base(paths[len(paths)-1]) != "segment.pdc4" {
		generation := strings.TrimSuffix(strings.TrimPrefix(filepath.Base(paths[len(paths)-1]), "segment-"), ".pdc4")
		n, err := strconv.Atoi(generation)
		if err != nil || n < 1 || n == int(^uint(0)>>1) {
			return errors.New("invalid journal generation")
		}
		p.segment = n + 1
	} else {
		p.segment = len(paths)
	}
	if err = p.openSegment(); err != nil {
		return err
	}
	p.writer = bufio.NewWriterSize(p.journal, 1<<20)
	return nil
}

// journalSegments lists the original segment before fixed-width generations.
func journalSegments(dir string) ([]string, error) {
	paths, err := filepath.Glob(filepath.Join(dir, "segment*.pdc4"))
	if err != nil {
		return nil, err
	}
	slices.Sort(paths)
	original := filepath.Join(dir, "segment.pdc4")
	if i := slices.Index(paths, original); i >= 0 {
		paths = append([]string{original}, append(paths[:i], paths[i+1:]...)...)
	}
	return paths, nil
}

// openSegment creates the next fixed-width segment; names sort in write order.
func (p *Producer) openSegment() error {
	name := "segment.pdc4"
	if p.segment > 0 {
		name = fmt.Sprintf("segment-%020d.pdc4", p.segment)
	}
	f, err := os.OpenFile(filepath.Join(p.journalDir, name), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	if err = syncDir(p.journalDir); err != nil {
		return errors.Join(err, f.Close())
	}
	p.journal, p.segment = f, p.segment+1
	return nil
}

// segmentEnd returns the cursor, last application order and offset of a
// segment's last complete record, reading only record lengths and that record.
// Like Scan, it truncates a torn tail of the newest segment and rejects one
// anywhere else. ok is false for a segment without a complete record.
func segmentEnd(path string, newest bool) (end, order uint64, last int64, ok bool, err error) {
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		return 0, 0, 0, false, err
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return 0, 0, 0, false, err
	}
	var offset int64
	last = -1
	for offset < st.Size() {
		var h [4]byte
		if _, err = f.ReadAt(h[:], offset); err == nil {
			size := binary.LittleEndian.Uint32(h[:])
			if size == 0 || size > v2replay.MaxRecord {
				return 0, 0, 0, false, errors.New("invalid journal length")
			}
			if offset+4+int64(size) <= st.Size() {
				last, offset = offset, offset+4+int64(size)
				continue
			}
		} else if !errors.Is(err, io.EOF) {
			return 0, 0, 0, false, err
		}
		if !newest {
			return 0, 0, 0, false, fmt.Errorf("torn journal record at %d", offset)
		}
		if err = errors.Join(f.Truncate(offset), f.Sync()); err != nil {
			return 0, 0, 0, false, err
		}
		break
	}
	if last < 0 {
		return 0, 0, -1, false, nil
	}
	record := make([]byte, offset-last-4)
	if _, err = f.ReadAt(record, last+4); err != nil {
		return 0, 0, 0, false, err
	}
	end, _, _, order, err = applicationHeader(record)
	return end, order, last, err == nil, err
}

// applicationHeader verifies an original application record's checksum and
// reads its plain header: the cursor after the record, its point count and its
// first and last application orders. The coded payload is not decoded.
func applicationHeader(record []byte) (cursor, n, first, last uint64, err error) {
	invalid := errors.New("invalid original application envelope")
	if len(record) < 8 || !isApplicationRecord(record) || crc32.Checksum(record[:len(record)-4], applicationCRC) != binary.LittleEndian.Uint32(record[len(record)-4:]) {
		return 0, 0, 0, 0, invalid
	}
	v1 := record[3] == 1
	b := record[4 : len(record)-4]
	read := func() uint64 {
		v, m := binary.Uvarint(b)
		if m <= 0 {
			err = invalid
			return 0
		}
		b = b[m:]
		return v
	}
	cursor = read()
	count := read()
	if count == 0 || count > 32768 {
		return 0, 0, 0, 0, invalid
	}
	var base uint64 // V2A2 lean form: one point per application, orders base, base+1, ...
	if !v1 {
		if base = read(); base > math.MaxUint64-count+1 {
			return 0, 0, 0, 0, invalid
		}
	}
	if base != 0 {
		n, first, last = count, base, base+count-1
	}
	for i := uint64(0); base == 0 && i < count && err == nil; i++ {
		if v1 && read() == 0 {
			err = invalid
		}
		previous := last
		if last = read(); i == 0 {
			first = last
		}
		points := read()
		if last <= previous || points == 0 || points > 32768-n {
			return 0, 0, 0, 0, invalid
		}
		n += points
		for j := uint64(0); j < points && err == nil; j++ {
			read() // sequence
		}
	}
	size := read()
	if err == nil && (size != uint64(len(b)) || n > cursor) {
		err = invalid
	}
	return cursor, n, first, last, err
}

// ErrLegacyJournal refuses a producer journal holding input no seal covers.
// L0 builds never replay it: stop the journal build cleanly (its Close seals
// every record) and start this build on the sealed store.
var ErrLegacyJournal = errors.New("producer journal has unsealed records: stop the previous build cleanly before starting with L0 run files")

// recoverL0 opens the producer's catalog, which registers every handle the L0
// files reference, and continues its order above the sealed frontier. A
// fully sealed legacy journal stays in place, read-only: its last record
// seeds the order on every start, because the frontier counts points, not
// orders, until the first L0 seal.
func (p *Producer) recoverL0(dir string) error {
	paths, err := journalSegments(dir)
	if err != nil {
		return err
	}
	// Only the newest record matters: the core frontier must cover it, and
	// its last order seeds the producer's order.
	durable := p.engine.DurableFrontier(p.index)
	for k := len(paths) - 1; k >= 0; k-- {
		end, order, _, ok, err := segmentEnd(paths[k], k == len(paths)-1)
		if err != nil {
			return fmt.Errorf("journal segment %s: %w", filepath.Base(paths[k]), err)
		}
		if !ok {
			continue
		}
		if end > durable {
			return fmt.Errorf("%s: %w", dir, ErrLegacyJournal)
		}
		p.applyOrder = max(p.applyOrder, order)
		break
	}
	p.journalDir = dir
	if err := p.openCatalog(); err != nil {
		return err
	}
	for _, frontier := range p.engine.DurableFrontiers() {
		p.applyOrder = max(p.applyOrder, frontier[p.index])
	}
	p.cursor = p.applyOrder
	return nil
}
