package v2daemon

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"slices"
	"syscall"

	"github.com/go-graphite/go-carbon/internal/partlog"
	"github.com/go-graphite/go-carbon/internal/v2replay"
)

type restartGeometry struct{ Version, Layer, Producers, Shards int }

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
	want := restartGeometry{1, c.Layer, c.Producers, c.Shards}
	b, err := os.ReadFile(path)
	if err == nil {
		var got restartGeometry
		if json.Unmarshal(b, &got) != nil || (got.Version != 1 && got.Version != 2) {
			return fail(errors.New("unsupported restart format version"))
		}
		want.Version = got.Version
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
	paths, err := filepath.Glob(filepath.Join(dir, "segment*.pdc4"))
	if err != nil {
		return err
	}
	// The original segment sorts first; suffixes use fixed-width generations.
	slices.Sort(paths)
	original := filepath.Join(dir, "segment.pdc4")
	if i := slices.Index(paths, original); i >= 0 {
		paths = append([]string{original}, append(paths[:i], paths[i+1:]...)...)
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
			batches, cursor, err := DecodeApplications(inner, &decoder, p.catalog, p.index, p.count)
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
			if p.durableApplications != nil {
				if err := p.durableApplications(p.index, batches); err != nil {
					return err
				}
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
	p.journalDir, p.segment = dir, len(paths)
	if err = p.openSegment(); err != nil {
		return err
	}
	p.writer = bufio.NewWriterSize(p.journal, 1<<20)
	return nil
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
	return nil
}

// rotateJournal starts a new segment with fresh predictor state. The caller has
// flushed and synced every record of the current segment and holds none encoded.
func (p *Producer) rotateJournal() error {
	if err := p.journal.Close(); err != nil {
		return err
	}
	p.journal = nil
	if err := p.openSegment(); err != nil {
		return err
	}
	p.writer.Reset(p.journal)
	p.encoder.Close()
	p.encoder = partlog.CatalogEncoder{Catalog: p.catalog, SeriesClocks: true, SparseClocks: true}
	return nil
}

// skipSegments returns how many leading segments hold only records that every
// owner has durably committed: all of them when the newest record is durable.
// Segment j-1 ends where segment j's first record begins, so only plain record
// headers are read; each segment starts with fresh predictor state. Placement
// and classic-mirror stores replay everything: relay chains and the mirror
// consume every retained record.
func (p *Producer) skipSegments(paths []string) (int, error) {
	if p.fullReplay || p.placement != nil || p.durableApplications != nil || len(paths) == 0 {
		return 0, nil
	}
	frontiers := p.engine.DurableFrontiers()
	if len(frontiers) == 0 {
		return 0, nil
	}
	durable := frontiers[0][p.index]
	for _, f := range frontiers[1:] {
		durable = min(durable, f[p.index])
	}
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
	// A quiesced store: every owner sealed past the newest record, so no segment
	// holds a row to replay. Empty newest segments are passed over.
	for j := len(paths) - 1; j >= 0; j-- {
		end, order, ok, err := segmentEnd(paths[j], j == len(paths)-1)
		if err != nil {
			return 0, fmt.Errorf("journal segment %s: %w", filepath.Base(paths[j]), err)
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
	for j := len(paths) - 1; j > 0; j-- {
		start, order, ok, err := segmentStart(paths[j])
		if err != nil {
			return 0, fmt.Errorf("journal segment %s: %w", filepath.Base(paths[j]), err)
		}
		if !ok || start > durable {
			continue
		}
		if order <= p.applyOrder {
			return 0, errors.New("journal application order regressed")
		}
		p.cursor, p.applyOrder = start, order-1
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
	if err != nil && err != io.ErrUnexpectedEOF && err != io.EOF {
		return 0, false, err
	}
	if n < 9 {
		return 0, false, nil
	}
	if !bytes.Equal(b[4:8], applicationMagic) {
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

// segmentEnd returns the cursor and last application order of a segment's last
// complete record, reading only record lengths and that record. Like Scan, it
// truncates a torn tail of the newest segment and rejects one anywhere else.
// ok is false for a segment without a complete record.
func segmentEnd(path string, newest bool) (end, order uint64, ok bool, err error) {
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		return 0, 0, false, err
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return 0, 0, false, err
	}
	var offset, last int64 = 0, -1
	for offset < st.Size() {
		var h [4]byte
		if _, err = f.ReadAt(h[:], offset); err == nil {
			size := binary.LittleEndian.Uint32(h[:])
			if size == 0 || size > v2replay.MaxRecord {
				return 0, 0, false, errors.New("invalid journal length")
			}
			if offset+4+int64(size) <= st.Size() {
				last, offset = offset, offset+4+int64(size)
				continue
			}
		} else if err != io.EOF {
			return 0, 0, false, err
		}
		if !newest {
			return 0, 0, false, fmt.Errorf("torn journal record at %d", offset)
		}
		if err = errors.Join(f.Truncate(offset), f.Sync()); err != nil {
			return 0, 0, false, err
		}
		break
	}
	if last < 0 {
		return 0, 0, false, nil
	}
	record := make([]byte, offset-last-4)
	if _, err = f.ReadAt(record, last+4); err != nil {
		return 0, 0, false, err
	}
	end, _, _, order, err = applicationHeader(record)
	return end, order, err == nil, err
}

// applicationHeader verifies an original application record's checksum and
// reads its plain header: the cursor after the record, its point count and its
// first and last application orders. The coded payload is not decoded.
func applicationHeader(record []byte) (cursor, n, first, last uint64, err error) {
	invalid := errors.New("invalid original application envelope")
	if len(record) < 8 || !bytes.Equal(record[:4], applicationMagic) || crc32.Checksum(record[:len(record)-4], applicationCRC) != binary.LittleEndian.Uint32(record[len(record)-4:]) {
		return 0, 0, 0, 0, invalid
	}
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
	for i := uint64(0); i < count && err == nil; i++ {
		if read() == 0 {
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
