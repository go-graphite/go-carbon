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
	// A cut interrupted before its rename leaves the segment whole.
	if err := os.Remove(filepath.Join(dir, seedTemp)); err != nil && !os.IsNotExist(err) {
		return err
	}
	paths, err := journalSegments(dir)
	if err != nil {
		return err
	}

	skip, err := p.skipSegments(paths)
	if err != nil {
		return err
	}
	// A sealed journal needs no names to recover. Open its catalog on the first
	// new application instead; a retained tail still needs it before decoding.
	p.journalDir = dir
	if skip < len(paths) || len(paths) == 0 {
		if err := p.openCatalog(); err != nil {
			return err
		}
	}
	p.RecoverySkippedSegments, p.RecoveryReplayedSegments = skip, len(paths)-skip
	for i, path := range paths {
		if i < skip {
			continue
		}
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
	if p.cursor > 0 && skip < len(paths) {
		if err = p.engine.AppendBatches(p.index, nil, p.cursor, true); err != nil {
			return err
		}
	}
	p.journalDir = dir
	if p.retires() {
		if p.closed, err = retainedSegments(paths); err != nil {
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
	p.journal, p.segment, p.segmentBytes = f, p.segment+1, 0
	p.segmentPath, p.segmentLast = f.Name(), -1
	return nil
}

// journalSegment is a retained closed segment. end is the cursor after its
// last record (the previous end while empty); last is that record's offset,
// -1 when the segment holds none.
type journalSegment struct {
	path string
	end  uint64
	last int64
}

// seedTemp holds a cut segment's last record until it replaces the segment.
const seedTemp = "seed.tmp"

// replaysAll reports consumers whose replay obligations are independent of
// the core frontier: placement/relay keep and replay every segment.
func (p *Producer) replaysAll() bool {
	return p.placement != nil
}

// retires reports a standalone journal that rotates and so can retire input.
func (p *Producer) retires() bool { return p.layer >= 2 && p.segmentLimit > 0 && !p.replaysAll() }

// rotateJournal starts a new segment with fresh predictor state. The caller has
// flushed and synced every record of the current segment and holds none encoded.
func (p *Producer) rotateJournal() error {
	if err := p.journal.Close(); err != nil {
		return err
	}
	p.journal = nil
	closed := journalSegment{path: p.segmentPath, end: p.cursor, last: p.segmentLast}
	if err := p.openSegment(); err != nil {
		return err
	}
	p.writer.Reset(p.journal)
	p.encoder.Close()
	p.encoder = partlog.CatalogEncoder{Catalog: p.catalog, SeriesClocks: true, SparseClocks: true}
	if p.retires() {
		p.closed = append(p.closed, closed)
	}
	return nil
}

// retireJournal unlinks the closed segments every owner has durably committed;
// core parts hold their input. Recovery seeds the cursor and application order
// from the first retained record, or from the last one when the frontiers cover
// all input. So while the open segment is empty, the newest covered segment
// holding a record stays, cut to that record; a covered open segment rotates
// first, and an idle producer keeps one record. It runs on the producer's
// caller with every record synced: after a rotation and on each idle Drain.
func (p *Producer) retireJournal() error {
	if !p.retires() || p.journal == nil || p.cursor == 0 || len(p.records) > 0 {
		return nil
	}
	durable := p.engine.DurableFrontier(p.index)
	if p.segmentLast > 0 && durable >= p.cursor {
		if err := p.rotateJournal(); err != nil {
			return err
		}
	}
	n := 0
	for n < len(p.closed) && p.closed[n].end <= durable {
		n++
	}
	keep := -1
	if p.segmentLast < 0 && n == len(p.closed) {
		for keep = n - 1; keep >= 0 && p.closed[keep].last < 0; keep-- {
		}
		if keep < 0 {
			return nil // nothing retained holds a record yet
		}
		if p.closed[keep].last > 0 {
			if err := p.cutToLastRecord(&p.closed[keep]); err != nil {
				return err
			}
		}
	}
	removed := 0
	for i, segment := range p.closed[:n] {
		if i == keep {
			continue
		}
		if p.stage != nil {
			p.stage("retire " + segment.path)
		}
		if err := os.Remove(segment.path); err != nil {
			return err
		}
		if p.stage != nil {
			p.stage("retired")
		}
		removed++
	}
	if keep >= 0 {
		p.closed[0] = p.closed[keep]
		p.closed = p.closed[:1]
	} else {
		p.closed = p.closed[:copy(p.closed, p.closed[n:])]
	}
	if removed > 0 {
		return syncDir(p.journalDir)
	}
	return nil
}

// cutToLastRecord atomically replaces a covered closed segment by its last
// record, byte for byte. Recovery reads only that record's checksummed plain
// header (segmentEnd) and never decodes a covered segment, whose predictor
// state the cut removed.
func (p *Producer) cutToLastRecord(segment *journalSegment) error {
	f, err := os.Open(segment.path)
	if err != nil {
		return err
	}
	st, err := f.Stat()
	if err != nil {
		return errors.Join(err, f.Close())
	}
	record := make([]byte, st.Size()-segment.last)
	_, err = f.ReadAt(record, segment.last)
	if err = errors.Join(err, f.Close()); err != nil {
		return err
	}
	if len(record) < 4 || int(binary.LittleEndian.Uint32(record)) != len(record)-4 {
		return fmt.Errorf("journal segment %s: last record does not end the segment", filepath.Base(segment.path))
	}
	if end, _, _, _, err := applicationHeader(record[4:]); err != nil || end != segment.end {
		return fmt.Errorf("journal segment %s: last record does not end at cursor %d", filepath.Base(segment.path), segment.end)
	}
	temp := filepath.Join(p.journalDir, seedTemp)
	out, err := os.OpenFile(temp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	_, err = out.Write(record)
	if err = errors.Join(err, out.Sync(), out.Close()); err != nil {
		return err
	}
	if p.stage != nil {
		p.stage("seed " + segment.path)
	}
	if err = os.Rename(temp, segment.path); err != nil {
		return err
	}
	if err = syncDir(p.journalDir); err != nil {
		return err
	}
	if p.stage != nil {
		p.stage("seeded")
	}
	segment.last = 0
	return nil
}

// retainedSegments lists every recovered segment as closed for retirement.
func retainedSegments(paths []string) ([]journalSegment, error) {
	out := make([]journalSegment, 0, len(paths))
	var end uint64
	for k, path := range paths {
		e, _, last, ok, err := segmentEnd(path, k == len(paths)-1)
		if err != nil {
			return nil, fmt.Errorf("journal segment %s: %w", filepath.Base(path), err)
		}
		if ok {
			end = e
		} else {
			last = -1
		}
		out = append(out, journalSegment{path: path, end: end, last: last})
	}
	return out, nil
}

// skipSegments uses the same durability boundary as retirement. A retired
// prefix needs its cursor/order seed even when no further segment can be skipped.
func (p *Producer) skipSegments(paths []string) (int, error) {
	if p.replaysAll() || len(paths) == 0 {
		return 0, nil
	}
	j, err := p.durablePrefix(paths)
	if err != nil || len(paths) == 0 {
		return j, err
	}
	// Retirement keeps a nonempty anchor segment. Fully sealed recovery can
	// skip that anchor too, restoring the end cursor/order from retained input.
	durable := p.engine.DurableFrontier(p.index)
	for k := len(paths) - 1; k >= 0; k-- {
		end, order, _, ok, err := segmentEnd(paths[k], k == len(paths)-1)
		if err != nil {
			return 0, fmt.Errorf("journal segment %s: %w", filepath.Base(paths[k]), err)
		}
		if !ok {
			continue
		}
		if end > durable {
			break
		}
		if order <= p.applyOrder {
			return 0, errors.New("journal application order regressed")
		}
		p.cursor, p.applyOrder = end, order
		return len(paths), nil
	}
	if j == 0 && filepath.Base(paths[0]) == "segment.pdc4" {
		return 0, nil
	}
	start, order, ok, err := segmentStart(paths[j])
	if err != nil {
		return 0, err
	}
	if !ok {
		return 0, errors.New("retained journal has no starting record")
	}
	if start > durable {
		return 0, errors.New("retained journal starts beyond core frontier")
	}
	if order <= p.applyOrder {
		return 0, errors.New("journal application order regressed")
	}
	p.cursor, p.applyOrder = start, order-1
	return j, nil
}

// durablePrefix is the leading prefix every owner has committed. Segment j-1
// ends at segment j's first record. Keep j and later, including the active file.
// Placement/relay and classic mirrors need their entire independent replay log.
// This helper never changes the live producer cursor or application order.
func (p *Producer) durablePrefix(paths []string) (int, error) {
	if p.replaysAll() || len(paths) == 0 {
		return 0, nil
	}
	durable := p.engine.DurableFrontier(p.index)
	// Skipped segments are not decoded, so keep full replay's rejection of
	// repeated or regressing segments: first-record cursors must increase.
	var last uint64
	for _, path := range paths {
		cursor, ok, err := segmentCursor(path)
		if err != nil {
			return 0, fmt.Errorf("journal segment %s: %w", filepath.Base(path), err)
		}
		if !ok {
			continue
		}
		if cursor <= last {
			return 0, errors.New("repeated or regressing journal cursor")
		}
		last = cursor
	}
	for j := len(paths) - 1; j >= 0; j-- {
		start, _, ok, err := segmentStart(paths[j])
		if err != nil {
			return 0, fmt.Errorf("journal segment %s: %w", filepath.Base(paths[j]), err)
		}
		if !ok || start > durable {
			continue
		}
		return j, nil
	}
	return 0, nil
}

// segmentCursor reads the first record's cursor from its plain header without
// decoding the record. ok is false for an empty segment or a short prefix.
func segmentCursor(path string) (uint64, bool, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, false, err
	}
	defer f.Close()
	var b [4 + 4 + binary.MaxVarintLen64]byte
	n, err := io.ReadFull(f, b[:])
	if err != nil && !errors.Is(err, io.ErrUnexpectedEOF) && !errors.Is(err, io.EOF) {
		return 0, false, err
	}
	if n < 9 {
		return 0, false, nil
	}
	if !isApplicationRecord(b[4:n]) {
		return 0, false, errors.New("invalid original application envelope")
	}
	cursor, m := binary.Uvarint(b[8:n])
	if m <= 0 {
		return 0, false, nil
	}
	if cursor == 0 {
		return 0, false, errors.New("invalid original application envelope")
	}
	return cursor, true, nil
}

// segmentStart reads a segment's first complete record. It returns the cursor
// before that record and the record's first application order; ok is false for
// an empty or torn segment, which can only be the newest.
func segmentStart(path string) (start, order uint64, ok bool, err error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, 0, false, err
	}
	defer f.Close()
	var h [4]byte
	if _, err = io.ReadFull(f, h[:]); err != nil {
		return 0, 0, false, nil
	}
	size := binary.LittleEndian.Uint32(h[:])
	if size == 0 || size > v2replay.MaxRecord {
		return 0, 0, false, errors.New("invalid journal length")
	}
	record := make([]byte, size)
	if _, err = io.ReadFull(f, record); err != nil {
		return 0, 0, false, nil
	}
	cursor, n, first, _, err := applicationHeader(record)
	return cursor - n, first, err == nil, err
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
	if len(paths) > 0 {
		skip, err := p.skipSegments(paths)
		if err != nil {
			return err
		}
		if skip < len(paths) {
			return fmt.Errorf("%s: %w", dir, ErrLegacyJournal)
		}
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
