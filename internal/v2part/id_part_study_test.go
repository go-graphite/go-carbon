//go:build partstudy && cgo

package v2part

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/DataDog/zstd"
	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/go-graphite/go-carbon/internal/namefp"
	"github.com/zeebo/xxh3"
)

// TestIDPartStudy is the ID-keyed part prototype (test-only). It re-encodes
// captured parts with vshard-local numeric IDs in place of canonical names in
// every key, the VictoriaMetrics TSID-block way, with the names held once in a
// per-vshard dictionary. Values (runs, orders, envelopes) stay byte-identical.
// It proves the re-encoding lossless through the ordinary SST reader, checks
// that a dictionary mutation is caught, and reports bytes, column attribution,
// encode/decode CPU per record, block index size and name -> ID lookup cost.
//
// ID_PART_STUDY: comma-separated part files or directories of *.sst.
// ID_PART_OUT: scratch directory for re-encoded files (kept for inspection).
// ID_PART_LEVEL: Zstd level of the re-encode (1 seal, 3 merge/rest).
func TestIDPartStudy(t *testing.T) {
	spec := os.Getenv("ID_PART_STUDY")
	out := os.Getenv("ID_PART_OUT")
	if spec == "" || out == "" {
		t.Skip("ID_PART_STUDY, ID_PART_OUT")
	}
	level := denseRestLevel
	if os.Getenv("ID_PART_LEVEL") == "1" {
		level = denseSealLevel
	}
	var paths []string
	for _, p := range strings.Split(spec, ",") {
		if st, err := os.Stat(p); err == nil && st.IsDir() {
			m, _ := filepath.Glob(filepath.Join(p, "*.sst"))
			paths = append(paths, m...)
		} else {
			paths = append(paths, p)
		}
	}
	if err := os.MkdirAll(out, 0o755); err != nil {
		t.Fatal(err)
	}
	for _, path := range paths {
		r := studyIDPart(t, path, out, level)
		b, _ := json.Marshal(r)
		t.Logf("IDPART %s", b)
	}
}

type idRec struct {
	key   Key
	raw   []byte // original GCP3 key
	value []byte
}

type idDict struct {
	id    map[string]uint32   // canonical name -> vshard-local ID
	names map[uint16][]string // vshard -> names by ID
}

// Layout versions of the prototype. Full keeps every GCP3 field and swaps the
// length-prefixed name for a 4-byte ID; compact also narrows archive to one
// byte (0xff = envelope), the physical slot to 4 bytes and drops the ordinal.
var idMagicFull = []byte{'G', 'C', 'I', 1}
var idMagicCompact = []byte{'G', 'C', 'I', 2}

func appendIDKey(dst []byte, k Key, id uint32, compact bool) ([]byte, error) {
	if compact {
		dst = append(dst, idMagicCompact...)
	} else {
		dst = append(dst, idMagicFull...)
	}
	dst = binary.BigEndian.AppendUint32(dst, uint32(len(k.Group)))
	dst = append(dst, k.Group...)
	dst = binary.BigEndian.AppendUint16(dst, k.VShard)
	dst = binary.BigEndian.AppendUint64(dst, uint64(k.Partition)^(1<<63))
	dst = append(dst, k.Part[:]...)
	dst = binary.BigEndian.AppendUint32(dst, id)
	if !compact {
		dst = binary.BigEndian.AppendUint32(dst, k.Archive)
		dst = binary.BigEndian.AppendUint64(dst, uint64(k.BlockStart)^(1<<63))
		return binary.BigEndian.AppendUint32(dst, k.Ordinal), nil
	}
	a := byte(k.Archive)
	if k.Archive == MetadataArchive {
		a = 0xff
	} else if k.Archive >= 0xff {
		return nil, errors.New("archive does not fit the compact key")
	}
	if k.Ordinal != 0 || k.BlockStart < 0 || k.BlockStart > 0xffffffff {
		return nil, errors.New("slot/ordinal does not fit the compact key")
	}
	dst = append(dst, a)
	return binary.BigEndian.AppendUint32(dst, uint32(k.BlockStart)), nil
}

func decodeIDKey(b []byte, compact bool) (Key, uint32, error) {
	var k Key
	magic, tail := idMagicFull, 20
	if compact {
		magic, tail = idMagicCompact, 9
	}
	if len(b) < 8 || !bytes.Equal(b[:4], magic) {
		return k, 0, errors.New("id key magic")
	}
	gn := int(binary.BigEndian.Uint32(b[4:8]))
	if len(b) != 8+gn+26+tail {
		return k, 0, errors.New("id key length")
	}
	k.Group = Group(b[8 : 8+gn])
	p := 8 + gn
	k.VShard = binary.BigEndian.Uint16(b[p:])
	k.Partition = int64(binary.BigEndian.Uint64(b[p+2:]) ^ (1 << 63))
	copy(k.Part[:], b[p+10:p+26])
	p += 26
	id := binary.BigEndian.Uint32(b[p:])
	p += 4
	if compact {
		k.Archive = uint32(b[p])
		if b[p] == 0xff {
			k.Archive = MetadataArchive
		}
		k.BlockStart = int64(binary.BigEndian.Uint32(b[p+1:]))
	} else {
		k.Archive = binary.BigEndian.Uint32(b[p:])
		k.BlockStart = int64(binary.BigEndian.Uint64(b[p+4:]) ^ (1 << 63))
		k.Ordinal = binary.BigEndian.Uint32(b[p+12:])
	}
	return k, id, nil
}

func readIDPart(t *testing.T, path string) []idRec {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	rd, err := sstable.NewSimpleReadable(f)
	if err != nil {
		t.Fatal(err)
	}
	r, err := sstable.NewReader(rd, sstable.ReaderOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	it, err := r.NewIter(nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer it.Close()
	names := map[string]ID{}
	var recs []idRec
	for ik, lv := it.First(); ik != nil; ik, lv = it.Next() {
		k, name, err := DecodeName(ik.UserKey, "")
		if err != nil {
			t.Fatal(err)
		}
		id, ok := names[string(name)]
		if !ok {
			id = ID(name)
			names[string(id)] = id
		}
		k.Metric = id
		v, _, err := lv.Value(nil)
		if err != nil {
			t.Fatal(err)
		}
		recs = append(recs, idRec{key: k, raw: bytes.Clone(ik.UserKey), value: bytes.Clone(v)})
	}
	return recs
}

// buildIDDict assigns dense IDs per vshard: in name order (a sorted bulk load)
// or in a hash order that models admission order (first seen, unrelated to the
// name sort).
func buildIDDict(recs []idRec, admission bool) idDict {
	d := idDict{id: map[string]uint32{}, names: map[uint16][]string{}}
	for _, r := range recs {
		n := string(r.key.Metric)
		if _, ok := d.id[n]; !ok {
			d.id[n] = 0
			d.names[r.key.VShard] = append(d.names[r.key.VShard], n)
		}
	}
	for _, list := range d.names {
		if admission {
			sort.Slice(list, func(i, j int) bool { return xxh3.HashString(list[i]) < xxh3.HashString(list[j]) })
		} else {
			sort.Strings(list)
		}
		for i, n := range list {
			d.id[n] = uint32(i)
		}
	}
	return d
}

type idKV struct {
	key []byte
	rec int
}

func encodeIDStream(recs []idRec, d idDict, compact bool) ([]idKV, error) {
	kvs := make([]idKV, len(recs))
	for i, r := range recs {
		k, err := appendIDKey(nil, r.key, d.id[string(r.key.Metric)], compact)
		if err != nil {
			return nil, err
		}
		kvs[i] = idKV{key: k, rec: i}
	}
	slices.SortFunc(kvs, func(a, b idKV) int { return bytes.Compare(a.key, b.key) })
	return kvs, nil
}

// verifyIDStream maps every ID key back through the dictionary to the exact
// original GCP3 key and checks strict order and value identity.
func verifyIDStream(recs []idRec, kvs []idKV, d idDict, compact bool, read func(i int) ([]byte, []byte)) error {
	if len(kvs) != len(recs) {
		return fmt.Errorf("records %d != %d", len(kvs), len(recs))
	}
	seen := make([]bool, len(recs))
	var last []byte
	for i := range kvs {
		key, value := read(i)
		if last != nil && bytes.Compare(last, key) >= 0 {
			return errors.New("id keys not strictly increasing")
		}
		last = key
		k, id, err := decodeIDKey(key, compact)
		if err != nil {
			return err
		}
		names := d.names[k.VShard]
		if int(id) >= len(names) {
			return errors.New("id outside the vshard dictionary")
		}
		k.Metric = ID(names[id])
		raw, err := Encode(k)
		if err != nil {
			return err
		}
		r := kvs[i].rec
		if seen[r] || !bytes.Equal(raw, recs[r].raw) || !bytes.Equal(value, recs[r].value) {
			return fmt.Errorf("record %d does not round-trip", i)
		}
		seen[r] = true
	}
	return nil
}

func writeIDSST(t *testing.T, path string, keys [][]byte, values [][]byte, level int) (int64, time.Duration) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	buffer := partOutputBuffers.Get().(*bufio.Writer)
	buffer.Reset(f)
	out := &hashedOutput{f: f, buf: buffer, h: sha256.New()}
	start := time.Now()
	w, err := newDenseSSTWriter(out)
	if err != nil {
		t.Fatal(err)
	}
	w.level = level
	for i := range keys {
		if err := w.Set(keys[i], values[i]); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return int64(out.n), time.Since(start)
}

// encodeTime is the minimum over rounds of building every key from its fields
// and writing the SST to a discarded output (no fsync).
func encodeTime(t *testing.T, recs []idRec, order []int, values [][]byte, level int, key func(r *idRec, dst []byte) []byte) (time.Duration, int64) {
	t.Helper()
	best := time.Duration(1 << 62)
	var size int64
	for round := 0; round < 3; round++ {
		f, err := os.OpenFile(os.DevNull, os.O_WRONLY, 0)
		if err != nil {
			t.Fatal(err)
		}
		buffer := partOutputBuffers.Get().(*bufio.Writer)
		buffer.Reset(f)
		out := &hashedOutput{f: f, buf: buffer, h: sha256.New()}
		start := time.Now()
		w, err := newDenseSSTWriter(out)
		if err != nil {
			t.Fatal(err)
		}
		w.level = level
		var scratch []byte
		for i, j := range order {
			scratch = key(&recs[j], scratch[:0])
			if err := w.Set(scratch, values[i]); err != nil {
				t.Fatal(err)
			}
		}
		_ = w.Close() // /dev/null cannot fsync; the bytes are complete.
		if d := time.Since(start); d < best {
			best = d
		}
		size = int64(out.n)
	}
	return best, size
}

// decodeTime is the minimum over rounds of iterating the SST through the
// ordinary reader and decoding every key.
func decodeTime(t *testing.T, path string, decode func(k []byte) error) time.Duration {
	t.Helper()
	best := time.Duration(1 << 62)
	for round := 0; round < 3; round++ {
		f, err := os.Open(path)
		if err != nil {
			t.Fatal(err)
		}
		rd, _ := sstable.NewSimpleReadable(f)
		r, err := sstable.NewReader(rd, sstable.ReaderOptions{})
		if err != nil {
			t.Fatal(err)
		}
		it, _ := r.NewIter(nil, nil)
		start := time.Now()
		for ik, lv := it.First(); ik != nil; ik, lv = it.Next() {
			if err := decode(ik.UserKey); err != nil {
				t.Fatal(err)
			}
			_, _, _ = lv.Value(nil)
		}
		if d := time.Since(start); d < best {
			best = d
		}
		_ = it.Close()
		_ = r.Close()
	}
	return best
}

// seekTime opens the file with a warm block cache and times SeekGE of each
// probe key (the per-part block index walk of a render).
func seekTime(t *testing.T, path string, probes [][]byte) (time.Duration, int) {
	t.Helper()
	c := pebble.NewCache(1 << 30)
	defer c.Unref()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	rd, _ := sstable.NewSimpleReadable(f)
	r, err := sstable.NewReader(rd, sstable.ReaderOptions{Cache: c})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	it, _ := r.NewIter(nil, nil)
	defer it.Close()
	hits := 0
	for _, p := range probes { // warm
		if ik, _ := it.SeekGE(p, 0); ik != nil {
			hits++
		}
	}
	best := time.Duration(1 << 62)
	for round := 0; round < 3; round++ {
		start := time.Now()
		for _, p := range probes {
			it.SeekGE(p, 0)
		}
		if d := time.Since(start); d < best {
			best = d
		}
	}
	return best, hits
}

func indexBytes(t *testing.T, path string) (int64, int) {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	rd, _ := sstable.NewSimpleReadable(f)
	r, err := sstable.NewReader(rd, sstable.ReaderOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	l, err := r.Layout()
	if err != nil {
		t.Fatal(err)
	}
	var n int64
	for _, h := range l.Index {
		n += int64(h.Length)
	}
	if l.TopIndex.Length != 0 {
		n += int64(l.TopIndex.Length)
	}
	return n, len(l.Data)
}

// dictStudy front-codes each vshard's names in ID order in blocks of 16 and
// measures the dictionary bytes (raw and Zstd 3) and name -> ID lookups: the
// fingerprint table (target read path) and a seek of the sorted dictionary.
func dictStudy(t *testing.T, d idDict, probes []string) map[string]any {
	t.Helper()
	var raw []byte
	var names int
	for _, list := range d.names {
		sorted := slices.Clone(list)
		sort.Strings(sorted)
		var prev string
		for i, n := range sorted {
			shared := 0
			if i%16 != 0 {
				shared = commonPrefix([]byte(prev), []byte(n))
			}
			raw = binary.AppendUvarint(raw, uint64(shared))
			raw = binary.AppendUvarint(raw, uint64(len(n)-shared))
			raw = append(raw, n[shared:]...)
			raw = binary.AppendUvarint(raw, uint64(d.id[n]))
			prev = n
		}
		names += len(list)
	}
	compressed, err := zstd.CompressLevel(nil, raw, 3)
	if err != nil {
		t.Fatal(err)
	}
	var table namefp.Table
	for n, id := range d.id {
		table.Put(namefp.Name(n), id)
	}
	start := time.Now()
	var miss int
	for _, p := range probes {
		if _, ok := table.Get(namefp.Name(p)); !ok {
			miss++
		}
	}
	fp := time.Since(start)
	// Sorted dictionary seek: binary search in a vshard's sorted names (the
	// mmapped front-coded level reads the same O(log n) heads plus one block).
	sorted := map[uint16][]string{}
	for vs, list := range d.names {
		s := slices.Clone(list)
		sort.Strings(s)
		sorted[vs] = s
	}
	start = time.Now()
	for _, p := range probes {
		s := sorted[Route(ID(p))]
		i := sort.SearchStrings(s, p)
		if i == len(s) || s[i] != p {
			miss++
		} else {
			_ = d.id[s[i]]
		}
	}
	seek := time.Since(start)
	if miss != 0 {
		t.Fatalf("dictionary misses %d", miss)
	}
	return map[string]any{
		"names": names, "raw_bytes": len(raw), "zstd_bytes": len(compressed),
		"raw_B_per_name": float64(len(raw)) / float64(names), "zstd_B_per_name": float64(len(compressed)) / float64(names),
		"fp_ns_per_lookup":   float64(fp.Nanoseconds()) / float64(len(probes)),
		"seek_ns_per_lookup": float64(seek.Nanoseconds()) / float64(len(probes)),
	}
}

func studyIDPart(t *testing.T, path, out string, level int) map[string]any {
	recs := readIDPart(t, path)
	if len(recs) == 0 {
		return map[string]any{"path": path, "records": 0}
	}
	st, _ := os.Stat(path)
	base := strings.TrimSuffix(filepath.Base(path), ".sst")
	res := map[string]any{"path": path, "records": len(recs), "file_bytes": st.Size(), "level": level}
	// Baseline: the same writer and level over the original keys must
	// reproduce the original file (harness fidelity).
	oldKeys := make([][]byte, len(recs))
	values := make([][]byte, len(recs))
	for i := range recs {
		oldKeys[i], values[i] = recs[i].raw, recs[i].value
	}
	oldPath := filepath.Join(out, base+".old.sst")
	oldSize, _ := writeIDSST(t, oldPath, oldKeys, values, level)
	oldStudy := partStudy(t, oldPath)
	points := oldStudy["points"].(int64)
	res["points"] = points
	res["old"] = map[string]any{"bytes": oldSize, "B_per_pt": float64(oldSize) / float64(points), "identical_to_input": oldSize == st.Size() && sameFile(t, oldPath, path), "study": oldStudy}
	idx, blocks := indexBytes(t, oldPath)
	res["old"].(map[string]any)["index_bytes"], res["old"].(map[string]any)["data_blocks"] = idx, blocks
	order := make([]int, len(recs))
	for i := range order {
		order[i] = i
	}
	oldEnc, _ := encodeTime(t, recs, order, values, level, func(r *idRec, dst []byte) []byte {
		k, err := Encode(r.key)
		if err != nil {
			t.Fatal(err)
		}
		return append(dst, k...)
	})
	oldSet, _ := encodeTime(t, recs, order, values, level, func(r *idRec, dst []byte) []byte { return append(dst, r.raw...) })
	oldDec := decodeTime(t, oldPath, func(k []byte) error { _, _, err := DecodeName(k, ""); return err })
	res["old"].(map[string]any)["encode_ns_per_rec"] = float64(oldEnc.Nanoseconds()) / float64(len(recs))
	res["old"].(map[string]any)["write_only_ns_per_rec"] = float64(oldSet.Nanoseconds()) / float64(len(recs))
	res["old"].(map[string]any)["decode_ns_per_rec"] = float64(oldDec.Nanoseconds()) / float64(len(recs))

	// Probes: up to 20,000 random names of the part, seek to their first key.
	rng := rand.New(rand.NewSource(1))
	var probeNames []string
	{
		var all []string
		seen := map[string]bool{}
		for _, r := range recs {
			if !seen[string(r.key.Metric)] {
				seen[string(r.key.Metric)] = true
				all = append(all, string(r.key.Metric))
			}
		}
		rng.Shuffle(len(all), func(i, j int) { all[i], all[j] = all[j], all[i] })
		probeNames = all[:min(len(all), 20000)]
		res["names"] = len(all)
	}
	first := map[string]Key{}
	for _, r := range recs {
		if _, ok := first[string(r.key.Metric)]; !ok {
			first[string(r.key.Metric)] = r.key
		}
	}
	oldProbes := make([][]byte, len(probeNames))
	for i, n := range probeNames {
		k := first[n]
		oldProbes[i], _ = Encode(k)
	}
	oldSeek, oldHits := seekTime(t, oldPath, oldProbes)
	res["old"].(map[string]any)["seek_ns"] = float64(oldSeek.Nanoseconds()) / float64(len(oldProbes))

	for _, admission := range []bool{false, true} {
		d := buildIDDict(recs, admission)
		for _, compact := range []bool{false, true} {
			name := "id_full"
			if compact {
				name = "id_compact"
			}
			if admission {
				name += "_admission"
			} else {
				name += "_nameorder"
			}
			kvs, err := encodeIDStream(recs, d, compact)
			if err != nil {
				if !compact {
					t.Fatalf("%s: %v", name, err)
				}
				res[name] = map[string]any{"unfit": err.Error()}
				continue
			}
			keys := make([][]byte, len(kvs))
			vals := make([][]byte, len(kvs))
			ord := make([]int, len(kvs))
			for i, kv := range kvs {
				keys[i], vals[i], ord[i] = kv.key, recs[kv.rec].value, kv.rec
			}
			if err := verifyIDStream(recs, kvs, d, compact, func(i int) ([]byte, []byte) { return keys[i], vals[i] }); err != nil {
				t.Fatalf("%s: %v", name, err)
			}
			p := filepath.Join(out, base+"."+name+".sst")
			size, _ := writeIDSST(t, p, keys, vals, level)
			// Read back through the ordinary SST reader and verify again.
			var rk, rv [][]byte
			f, _ := os.Open(p)
			rd, _ := sstable.NewSimpleReadable(f)
			rr, err := sstable.NewReader(rd, sstable.ReaderOptions{})
			if err != nil {
				t.Fatal(err)
			}
			it, _ := rr.NewIter(nil, nil)
			for ik, lv := it.First(); ik != nil; ik, lv = it.Next() {
				v, _, _ := lv.Value(nil)
				rk, rv = append(rk, bytes.Clone(ik.UserKey)), append(rv, bytes.Clone(v))
			}
			_ = it.Close()
			_ = rr.Close()
			if err := verifyIDStream(recs, kvs, d, compact, func(i int) ([]byte, []byte) { return rk[i], rv[i] }); err != nil {
				t.Fatalf("%s read back: %v", name, err)
			}
			// Mutation: swap two names' IDs in one vshard; the round trip must fail.
			caught := "n/a"
			for vs, list := range d.names {
				if len(list) < 2 {
					continue
				}
				m := idDict{id: d.id, names: map[uint16][]string{}}
				for k, v := range d.names {
					m.names[k] = v
				}
				sw := slices.Clone(list)
				sw[0], sw[1] = sw[1], sw[0]
				m.names[vs] = sw
				caught = fmt.Sprint(verifyIDStream(recs, kvs, m, compact, func(i int) ([]byte, []byte) { return keys[i], vals[i] }) != nil)
				break
			}
			if caught == "false" {
				t.Fatalf("%s: dictionary mutation not caught", name)
			}
			// Mutation: resolve IDs through the neighbouring vshard's dictionary
			// (IDs are only unique within a vshard); the round trip must fail.
			shifted := idDict{id: d.id, names: map[uint16][]string{}}
			for vs, list := range d.names {
				shifted.names[(vs+1)%VirtualShards] = list
			}
			if verifyIDStream(recs, kvs, shifted, compact, func(i int) ([]byte, []byte) { return keys[i], vals[i] }) == nil {
				t.Fatalf("%s: vshard-scope mutation not caught", name)
			}
			study := partStudy(t, p)
			idx, blocks := indexBytes(t, p)
			enc, _ := encodeTime(t, recs, ord, vals, level, func(r *idRec, dst []byte) []byte {
				k, err := appendIDKey(dst, r.key, d.id[string(r.key.Metric)], compact)
				if err != nil {
					t.Fatal(err)
				}
				return k
			})
			dec := decodeTime(t, p, func(k []byte) error { _, _, err := decodeIDKey(k, compact); return err })
			probes := make([][]byte, len(probeNames))
			for i, n := range probeNames {
				probes[i], _ = appendIDKey(nil, first[n], d.id[n], compact)
			}
			seek, hits := seekTime(t, p, probes)
			if hits != oldHits {
				t.Fatalf("%s: seek hits %d != %d", name, hits, oldHits)
			}
			res[name] = map[string]any{
				"bytes": size, "B_per_pt": float64(size) / float64(points), "saving_pct": 100 * (1 - float64(size)/float64(oldSize)),
				"index_bytes": idx, "data_blocks": blocks, "mutation_caught": caught,
				"encode_ns_per_rec": float64(enc.Nanoseconds()) / float64(len(recs)),
				"decode_ns_per_rec": float64(dec.Nanoseconds()) / float64(len(recs)),
				"seek_ns":           float64(seek.Nanoseconds()) / float64(len(probes)),
				"study":             study,
			}
		}
		if !admission {
			res["dict"] = dictStudy(t, d, probeNames)
		}
	}
	return res
}

func sameFile(t *testing.T, a, b string) bool {
	t.Helper()
	x, err := os.ReadFile(a)
	if err != nil {
		t.Fatal(err)
	}
	y, err := os.ReadFile(b)
	if err != nil {
		t.Fatal(err)
	}
	return bytes.Equal(x, y)
}
