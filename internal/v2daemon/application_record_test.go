package v2daemon

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"hash"
	"hash/crc32"
	"io"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/go-graphite/go-carbon/internal/partlog"
	"github.com/go-graphite/go-carbon/internal/v2core"
	"github.com/go-graphite/go-carbon/points"
)

// appendApplicationRecordV1 writes the V2A1 layout older journals hold: per
// application the catalog ID, order, point count and every sequence.
func appendApplicationRecordV1(cursor uint64, ids []uint64, apps []Application, pdc4 []byte) []byte {
	b := binary.AppendUvarint([]byte{'V', '2', 'A', 1}, cursor)
	b = binary.AppendUvarint(b, uint64(len(apps)))
	for i, a := range apps {
		b = binary.AppendUvarint(b, ids[i])
		b = binary.AppendUvarint(b, a.Order)
		b = binary.AppendUvarint(b, uint64(len(a.Points)))
		for _, seq := range a.Sequence {
			b = binary.AppendUvarint(b, seq)
		}
	}
	b = binary.AppendUvarint(b, uint64(len(pdc4)))
	b = append(b, pdc4...)
	return binary.LittleEndian.AppendUint32(b, crc32.Checksum(b, applicationCRC))
}

// TestApplicationRecordForms pins the V2A2 envelope. A TCP-shaped packet (one
// point per application, consecutive orders, sequence = order) stores only its
// base order; any other packet keeps explicit orders and sequences. V2A2 and
// V2A1 records over the same PDC4 stream decode to the same original batches,
// and the plain header reports the same cursor, point count and orders.
func TestApplicationRecordForms(t *testing.T) {
	catalog, err := partlog.OpenNameCatalog(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer catalog.Close()
	ids := make([]uint64, 3)
	for i := range ids {
		if ids[i], err = catalog.Intern("lean."+strconv.Itoa(i), []byte{byte(i), 9}); err != nil {
			t.Fatal(err)
		}
	}
	tcp := func(base uint64, n int, edit func([]Application)) []Application {
		apps := make([]Application, n)
		for i := range apps {
			o := base + uint64(i)
			apps[i] = Application{Order: o, Clock: 50 + int64(i), Points: []points.Point{{Timestamp: 1000 + int64(i), Value: float64(i) / 3}}, Sequence: []uint64{o}}
		}
		if edit != nil {
			edit(apps)
		}
		return apps
	}
	cases := []struct {
		name string
		apps []Application
		lean bool
	}{
		{"tcp", tcp(41, 5, nil), true},
		{"single large order", tcp(1<<40, 1, nil), true},
		{"sequence differs", tcp(60, 3, func(a []Application) { a[1].Sequence[0] = 7 }), false},
		{"order gap", tcp(70, 3, func(a []Application) { a[2].Order, a[2].Sequence[0] = 73, 73 }), false},
		{"two points", tcp(80, 2, func(a []Application) {
			a[1].Points = append(a[1].Points, points.Point{Timestamp: 1002, Value: math.Inf(-1)})
			a[1].Sequence = []uint64{81, 82}
		}), false},
		{"tcp after explicit", tcp(90, 4, nil), true},
	}
	encoder := partlog.CatalogEncoder{Catalog: catalog, SeriesClocks: true, SparseClocks: true}
	defer encoder.Close()
	decoders := [2]partlog.CatalogDecoder{{Catalog: catalog}, {Catalog: catalog}}
	defer decoders[0].Close()
	defer decoders[1].Close()
	var cursor uint64
	for _, c := range cases {
		frames := make([]partlog.CatalogFrame, len(c.apps))
		recordIDs := make([]uint64, len(c.apps))
		want := make([]v2core.Batch, len(c.apps))
		n := 0
		for i, a := range c.apps {
			id := ids[i%len(ids)]
			recordIDs[i] = id
			frames[i] = partlog.CatalogFrame{ID: id, Clock: a.Clock, Points: a.Points}
			want[i] = v2core.Batch{ID: id, Order: a.Order, Clock: a.Clock, Policy: []byte{byte(i % len(ids)), 9}}
			for j, p := range a.Points {
				want[i].Points = append(want[i].Points, v2core.Row{ID: id, Time: p.Timestamp, Bits: math.Float64bits(p.Value), Seq: a.Sequence[j]})
			}
			n += len(a.Points)
		}
		pdc4, err := encoder.EncodeFrames(frames)
		if err != nil {
			t.Fatal(err)
		}
		cursor += uint64(n)
		v2 := appendApplicationRecord(nil, cursor, c.apps, pdc4)
		_, k1 := binary.Uvarint(v2[4:])
		_, k2 := binary.Uvarint(v2[4+k1:])
		base, k3 := binary.Uvarint(v2[4+k1+k2:])
		if (base != 0) != c.lean || c.lean && (base != c.apps[0].Order || len(v2) != 4+k1+k2+k3+applicationVarintLen(uint64(len(pdc4)))+len(pdc4)+4) {
			t.Fatalf("%s: base=%d lean=%v", c.name, base, c.lean)
		}
		for k, record := range [][]byte{v2, appendApplicationRecordV1(cursor, recordIDs, c.apps, pdc4)} {
			got, end, err := DecodeApplications(record, &decoders[k], 0, 1)
			if err != nil || end != cursor || !reflect.DeepEqual(got, want) {
				t.Fatalf("%s V2A%d: err=%v cursor=%d/%d\ngot  %+v\nwant %+v", c.name, 2-k, err, end, cursor, got, want)
			}
			end, points, first, last, err := applicationHeader(record)
			if err != nil || end != cursor || points != uint64(n) || first != c.apps[0].Order || last != c.apps[len(c.apps)-1].Order {
				t.Fatalf("%s V2A%d header: %d %d %d %d %v", c.name, 2-k, end, points, first, last, err)
			}
		}
	}
	// A record whose last order would pass 2^64 is malformed.
	bad := binary.AppendUvarint(binary.AppendUvarint(binary.AppendUvarint(append([]byte(nil), applicationMagic...), 2), 2), math.MaxUint64)
	bad = binary.AppendUvarint(bad, 0)
	bad = binary.LittleEndian.AppendUint32(bad, crc32.Checksum(bad, applicationCRC))
	if _, _, _, _, err := applicationHeader(bad); err == nil {
		t.Fatal("accepted overflowing base order")
	}
}

// TestJournalFixtureReplay replays a retained V2A1 journal (one directory per
// producer-NNN with catalog/ and segment.pdc4) when V2_JOURNAL_FIXTURE names
// a disposable copy of it. Every V2A1 record must decode, and the same
// applications re-framed as V2A2 over the same PDC4 bytes must decode to
// identical batches. The digest of the V2A1 decode is logged for comparison
// with older builds; V2_JOURNAL_FIXTURE_DIGEST pins it.
func TestJournalFixtureReplay(t *testing.T) {
	root := os.Getenv("V2_JOURNAL_FIXTURE")
	if root == "" {
		t.Skip("V2_JOURNAL_FIXTURE not set")
	}
	producers := 96
	if v := os.Getenv("V2_JOURNAL_FIXTURE_PRODUCERS"); v != "" {
		var err error
		if producers, err = strconv.Atoi(v); err != nil {
			t.Fatal(err)
		}
	}
	dirs, err := filepath.Glob(filepath.Join(root, "producer-*"))
	if err != nil || len(dirs) == 0 {
		t.Fatal("no producer directories", err)
	}
	type result struct {
		digest                                  [32]byte
		points, records, lean, oldSize, newSize uint64
		err                                     error
	}
	results := make([]result, len(dirs))
	var wg sync.WaitGroup
	for i, dir := range dirs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r := &results[i]
			producer, err := strconv.Atoi(strings.TrimPrefix(filepath.Base(dir), "producer-"))
			if err != nil {
				r.err = err
				return
			}
			h := sha256.New()
			r.points, r.records, r.lean, r.oldSize, r.newSize, r.err = replayFixtureSegment(dir, producer, producers, h)
			h.Sum(r.digest[:0])
		}()
	}
	wg.Wait()
	total := sha256.New()
	var sum result
	for i, r := range results {
		if r.err != nil {
			t.Fatalf("%s: %v", dirs[i], r.err)
		}
		total.Write([]byte(filepath.Base(dirs[i])))
		total.Write(r.digest[:])
		sum.points += r.points
		sum.records += r.records
		sum.lean += r.lean
		sum.oldSize += r.oldSize
		sum.newSize += r.newSize
	}
	digest := total.Sum(nil)
	t.Logf("producers %d points %d records %d lean %d | V2A1 %.3f B/pt, V2A2 %.3f B/pt | V2A1 decode digest %x",
		len(dirs), sum.points, sum.records, sum.lean, float64(sum.oldSize)/float64(sum.points), float64(sum.newSize)/float64(sum.points), digest)
	if want := os.Getenv("V2_JOURNAL_FIXTURE_DIGEST"); want != "" && want != hex.EncodeToString(digest) {
		t.Fatalf("V2A1 decode digest %x, want %s", digest, want)
	}
}

// replayFixtureSegment decodes one producer's V2A1 records, digests the
// batches, and checks that the V2A2 framing of each record decodes the same.
func replayFixtureSegment(dir string, producer, producers int, h hash.Hash) (pts, records, lean, oldSize, newSize uint64, err error) {
	catalog, err := partlog.OpenNameCatalog(filepath.Join(dir, "catalog"))
	if err != nil {
		return
	}
	defer catalog.Close()
	v1 := partlog.CatalogDecoder{Catalog: catalog}
	v2 := partlog.CatalogDecoder{Catalog: catalog}
	defer v1.Close()
	defer v2.Close()
	data, err := os.ReadFile(filepath.Join(dir, "segment.pdc4"))
	if err != nil {
		return
	}
	var word [8]byte
	put := func(v uint64) {
		binary.LittleEndian.PutUint64(word[:], v)
		h.Write(word[:])
	}
	for len(data) > 0 {
		if len(data) < 4 || int(binary.LittleEndian.Uint32(data)) > len(data)-4 {
			return 0, 0, 0, 0, 0, io.ErrUnexpectedEOF
		}
		record := data[4 : 4+binary.LittleEndian.Uint32(data)]
		data = data[4+len(record):]
		if len(record) < 4 || string(record[:4]) != "V2A\x01" {
			return 0, 0, 0, 0, 0, errors.New("fixture record is not V2A1")
		}
		batches, cursor, err := DecodeApplications(record, &v1, producer, producers)
		if err != nil {
			return 0, 0, 0, 0, 0, err
		}
		put(cursor)
		apps := make([]Application, len(batches))
		for i, b := range batches {
			put(b.ID)
			put(b.Order)
			put(uint64(b.Clock))
			put(uint64(len(b.Policy)))
			h.Write(b.Policy)
			put(uint64(len(b.Points)))
			apps[i] = Application{Order: b.Order, Clock: b.Clock, Points: make([]points.Point, len(b.Points)), Sequence: make([]uint64, len(b.Points))}
			for j, row := range b.Points {
				put(row.ID)
				put(uint64(row.Time))
				put(row.Bits)
				put(row.Seq)
				apps[i].Points[j] = points.Point{Timestamp: row.Time, Value: math.Float64frombits(row.Bits)}
				apps[i].Sequence[j] = row.Seq
			}
			pts += uint64(len(b.Points))
		}
		pdc4 := v1PDC4(record)
		lean2 := appendApplicationRecord(nil, cursor, apps, pdc4)
		again, end, err := DecodeApplications(lean2, &v2, producer, producers)
		if err != nil || end != cursor || !reflect.DeepEqual(again, batches) {
			return 0, 0, 0, 0, 0, errors.Join(errors.New("V2A2 framing changed the decode"), err)
		}
		if applicationBase(apps) != 0 {
			lean++
		}
		records++
		oldSize += uint64(4 + len(record))
		newSize += uint64(4 + len(lean2))
	}
	return
}

// v1PDC4 returns the PDC4 frame of a V2A1 record that DecodeApplications accepted.
func v1PDC4(record []byte) []byte {
	b := record[4 : len(record)-4]
	read := func() uint64 {
		v, n := binary.Uvarint(b)
		b = b[n:]
		return v
	}
	read() // cursor
	for range read() {
		read() // ID
		read() // order
		for range read() {
			read() // sequence
		}
	}
	size := read()
	return b[:size]
}
