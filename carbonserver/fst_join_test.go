package carbonserver

import (
	"bytes"
	"errors"
	"fmt"
	"math/rand"
	"slices"
	"testing"

	"github.com/blevesearch/vellum"
)

func buildTestFST(t testing.TB, keys [][]byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	b, err := vellum.New(&buf, nil)
	if err != nil {
		t.Fatal(err)
	}
	for i, k := range keys {
		if err := b.Insert(k, uint64(i)); err != nil {
			t.Fatal(err)
		}
	}
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func buildTestFSTValues(t testing.TB, keys [][]byte, values []uint64) []byte {
	t.Helper()
	var buf bytes.Buffer
	b, err := vellum.New(&buf, nil)
	if err != nil {
		t.Fatal(err)
	}
	for i, k := range keys {
		if err := b.Insert(k, values[i]); err != nil {
			t.Fatal(err)
		}
	}
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func joinTestFST(t testing.TB, keys [][]byte, cuts []int) []byte {
	return joinTestFSTValues(t, keys, nil, cuts)
}

// joinTestFSTValues joins shards of keys; with values, each shard keeps them
// instead of numbering rows.
func joinTestFSTValues(t testing.TB, keys [][]byte, values []uint64, cuts []int) []byte {
	t.Helper()
	var out bytes.Buffer
	j, err := newFSTJoinerMode(&out, values != nil)
	if err != nil {
		t.Fatal(err)
	}
	start := 0
	for _, end := range append(cuts, len(keys)) {
		if end == start {
			continue
		}
		part := keys[start:end]
		// The tightest bounds a scan can pass: the common prefix with the
		// neighbouring shards' keys.
		left, right := 0, 0
		if start > 0 {
			left = scanCommonPrefix(part[0], keys[start-1])
		}
		if end < len(keys) {
			right = scanCommonPrefix(part[len(part)-1], keys[end])
		}
		data := buildTestFST(t, part)
		if values != nil {
			data = buildTestFSTValues(t, part, values[start:end])
		}
		shard, err := newFSTShard(data, part[0], part[len(part)-1], uint64(len(part)), left, right)
		if err != nil {
			t.Fatal(err)
		}
		if err := j.add(shard, bytes.NewReader(fstShardBody(data))); err != nil {
			t.Fatal(err)
		}
		start = end
	}
	if err := j.finish(); err != nil {
		t.Fatal(err)
	}
	return out.Bytes()
}

func assertSameFST(t *testing.T, keys [][]byte, data []byte, probes [][]byte) {
	t.Helper()
	fst, err := vellum.Load(data)
	if err != nil {
		t.Fatal(err)
	}
	if fst.Len() != len(keys) {
		t.Fatalf("len %d, want %d", fst.Len(), len(keys))
	}
	it, err := fst.Iterator(nil, nil)
	for i := 0; i < len(keys); i++ {
		if err != nil {
			t.Fatalf("iterator stopped at %d/%d: %v", i, len(keys), err)
		}
		k, v := it.Current()
		if !bytes.Equal(k, keys[i]) || v != uint64(i) {
			t.Fatalf("entry %d: got %q=%d, want %q=%d", i, k, v, keys[i], i)
		}
		err = it.Next()
	}
	if !errors.Is(err, vellum.ErrIteratorDone) {
		t.Fatalf("iterator has extra entries: %v", err)
	}
	for i, k := range keys {
		if v, ok, err := fst.Get(k); err != nil || !ok || v != uint64(i) {
			t.Fatalf("get %q: %d %t %v, want %d", k, v, ok, err, i)
		}
	}
	for _, k := range probes {
		_, want := slices.BinarySearchFunc(keys, k, bytes.Compare)
		if _, ok, err := fst.Get(k); err != nil || ok != want {
			t.Fatalf("probe %q: found=%t err=%v, want %t", k, ok, err, want)
		}
	}
	// Bounded iteration seeks through the merged nodes.
	if len(keys) > 2 {
		lo, hi := len(keys)/3, 2*len(keys)/3
		it, err := fst.Iterator(keys[lo], keys[hi])
		for i := lo; i < hi; i++ {
			if err != nil {
				t.Fatalf("range iterator stopped at %d: %v", i, err)
			}
			if k, v := it.Current(); !bytes.Equal(k, keys[i]) || v != uint64(i) {
				t.Fatalf("range entry %d: got %q=%d", i, k, v)
			}
			err = it.Next()
		}
		if !errors.Is(err, vellum.ErrIteratorDone) {
			t.Fatal("range iterator passed its end")
		}
	}
}

func randomFSTKeys(rng *rand.Rand, n int) [][]byte {
	alphabet := []byte("\x00abc.wsp\xff")
	seen := map[string]bool{}
	var keys [][]byte
	for len(keys) < n {
		var k []byte
		// Shared prefixes, prefix keys and long runs exercise deep shared spines.
		if len(keys) > 0 && rng.Intn(3) > 0 {
			prev := keys[rng.Intn(len(keys))]
			k = append(k, prev[:rng.Intn(len(prev)+1)]...)
		}
		for i := rng.Intn(6); i >= 0; i-- {
			k = append(k, alphabet[rng.Intn(len(alphabet))])
		}
		if !seen[string(k)] {
			seen[string(k)] = true
			keys = append(keys, k)
		}
	}
	slices.SortFunc(keys, bytes.Compare)
	return keys
}

func TestFSTJoinMatchesSerialBuild(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	for iter := 0; iter < 300; iter++ {
		keys := randomFSTKeys(rng, 1+rng.Intn(400))
		var cuts []int
		for i := rng.Intn(min(len(keys), 40)); i > 0; i-- {
			cuts = append(cuts, rng.Intn(len(keys)+1))
		}
		slices.Sort(cuts)
		cuts = slices.Compact(cuts)
		probes := randomFSTKeys(rng, 50)
		for _, k := range keys[:min(len(keys), 20)] {
			probes = append(probes, k[:len(k)/2], append(bytes.Clone(k), 0))
		}
		t.Run(fmt.Sprint(iter), func(t *testing.T) {
			assertSameFST(t, keys, joinTestFST(t, keys, cuts), probes)
		})
	}
}

func TestFSTJoinEdgeCases(t *testing.T) {
	prefix := "\x00aggregations\x00secondly\x00"
	var paths [][]byte
	for ns := 0; ns < 30; ns++ {
		for m := 0; m < 40; m++ {
			paths = append(paths, fmt.Appendf(nil, "%sns%02d\x00host%03d.wsp", prefix, ns, m))
		}
	}
	slices.SortFunc(paths, bytes.Compare)
	every := make([]int, 0, len(paths))
	for i := 1; i < len(paths); i++ {
		every = append(every, i)
	}
	wide := make([][]byte, 0, 256)
	for c := 0; c < 256; c++ {
		wide = append(wide, []byte{'w', byte(c), 'x'})
	}
	for name, tc := range map[string]struct {
		keys [][]byte
		cuts []int
	}{
		"single shard":         {paths, nil},
		"one key per shard":    {paths, every},
		"cut inside namespace": {paths, []int{17, 18, 400, 401, 1199}},
		"prefix keys":          {[][]byte{[]byte("a"), []byte("ab"), []byte("abc"), []byte("abd"), []byte("b")}, []int{1, 2, 3, 4}},
		"256 transitions":      {wide, []int{1, 100, 255}},
		"single key":           {[][]byte{[]byte("only")}, nil},
	} {
		t.Run(name, func(t *testing.T) {
			assertSameFST(t, tc.keys, joinTestFST(t, tc.keys, tc.cuts), nil)
		})
	}
	t.Run("empty", func(t *testing.T) {
		assertSameFST(t, nil, joinTestFST(t, nil, nil), [][]byte{[]byte("x")})
	})
}

func TestFSTJoinRejectsUnorderedShards(t *testing.T) {
	j, err := newFSTJoiner(&bytes.Buffer{})
	if err != nil {
		t.Fatal(err)
	}
	for _, keys := range [][][]byte{{[]byte("b")}, {[]byte("a")}} {
		data := buildTestFST(t, keys)
		shard, err := newFSTShard(data, keys[0], keys[0], 1, 1, 1)
		if err != nil {
			t.Fatal(err)
		}
		if err = j.add(shard, bytes.NewReader(fstShardBody(data))); err != nil {
			return
		}
	}
	t.Fatal("out-of-order shard accepted")
}

func TestFSTJoinKeepsValues(t *testing.T) {
	rng := rand.New(rand.NewSource(5))
	for iter := 0; iter < 200; iter++ {
		keys := randomFSTKeys(rng, 1+rng.Intn(300))
		values := make([]uint64, len(keys))
		for i := range values {
			// Small codes, large counts and zeros, in no particular order.
			switch rng.Intn(3) {
			case 0:
				values[i] = uint64(rng.Intn(4))
			case 1:
				values[i] = uint64(rng.Int63n(1 << 40))
			}
		}
		var cuts []int
		for i := rng.Intn(min(len(keys), 20)); i > 0; i-- {
			cuts = append(cuts, rng.Intn(len(keys)+1))
		}
		slices.Sort(cuts)
		cuts = slices.Compact(cuts)
		for _, data := range [][]byte{buildTestFSTValues(t, keys, values), joinTestFSTValues(t, keys, values, cuts)} {
			fst, err := vellum.Load(data)
			if err != nil {
				t.Fatal(err)
			}
			c, err := newFSTCursor(data)
			if err != nil {
				t.Fatal(err)
			}
			for i, k := range keys {
				if v, ok, err := fst.Get(k); err != nil || !ok || v != values[i] {
					t.Fatalf("%d: get %q = %d %t %v, want %d", iter, k, v, ok, err, values[i])
				}
				if final, v := c.final(c.acceptBytes(c.start(), k)); !final || v != values[i] {
					t.Fatalf("%d: cursor %q = %d %t, want %d", iter, k, v, final, values[i])
				}
			}
		}
	}
}
