package v2dict

import (
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"syscall"
	"testing"
	"time"

	"github.com/zeebo/xxh3"
)

// TestFlushKindCost measures what writing a flush as a compact L2 level
// (compressed, persisted Bloom filter) costs against the L1 flush (raw
// blocks, Bloom filter kept in memory), per default-size (8 MiB) flush of
// real names (V2DICT_SCALE_NAMES), and the negative lookup cost over k
// waiting flushed levels: L1 as written, L1 reopened (no filter) and L2.
// It measures; it asserts nothing.
func TestFlushKindCost(t *testing.T) {
	src := os.Getenv("V2DICT_SCALE_NAMES")
	if src == "" || os.Getenv("V2DICT_FLUSH_COST") == "" {
		t.Skip("set V2DICT_FLUSH_COST=1 and V2DICT_SCALE_NAMES")
	}
	names := readNames(t, src)
	vs := func(s string) uint16 { return uint16(xxh3.HashString(s) % 1024) }
	// One real flush gives the names per default-size flush.
	d, err := Open(t.TempDir(), Options{NoMerge: true})
	if err != nil {
		t.Fatal(err)
	}
	next := 0
	for len(d.snap.Load().levels) == 0 {
		s := names[next]
		next++
		if _, _, err := d.Append("g", vs(s), []byte(s), 0); err != nil {
			t.Fatal(err)
		}
		d.flushWG.Wait()
	}
	perFlush := d.snap.Load().levels[0].n
	d.Close()
	const k = 64
	if len(names) < (k+1)*perFlush {
		t.Fatalf("%d names, need %d", len(names), (k+1)*perFlush)
	}
	type kv struct{ k, v []byte }
	flushKeys := func(f int) []kv {
		out := make([]kv, 0, perFlush)
		for _, s := range names[f*perFlush : (f+1)*perFlush] {
			out = append(out, kv{append(scopeKey(nil, "g", vs(s)), s...), (Entry{ID: uint32(len(out))}).put(nil)})
		}
		slices.SortFunc(out, func(a, b kv) int { return slices.Compare(a.k, b.k) })
		return out
	}
	cpu := func() time.Duration {
		var ru syscall.Rusage
		_ = syscall.Getrusage(syscall.RUSAGE_SELF, &ru)
		return time.Duration(ru.Utime.Nano() + ru.Stime.Nano())
	}
	dir := t.TempDir()
	levels := map[string][]*level{}
	for _, kind := range []string{"L1", "L2"} {
		var wall, used time.Duration
		bytes := 0
		for f := range k {
			keys := flushKeys(f)
			path := levelPath(filepath.Join(dir, kind), uint64(f+1), uint64(f+1))
			os.MkdirAll(filepath.Dir(path), 0o755)
			t0, c0 := time.Now(), cpu()
			l, err := writeLevelKind(path, uint64(f+1), uint64(f+1), kind == "L2", func(w *levelWriter) {
				w.reserve(len(keys))
				for _, e := range keys {
					w.add(e.k, e.v)
				}
			})
			if err != nil {
				t.Fatal(err)
			}
			wall += time.Since(t0)
			used += cpu() - c0
			bytes += l.fileBytes()
			levels[kind] = append(levels[kind], l)
		}
		fmt.Printf("FLUSHCOST kind=%s names/flush=%d wall_ms/flush=%.2f cpu_ms/flush=%.2f file_B/name=%.1f\n",
			kind, perFlush, float64(wall.Microseconds())/1e3/k, float64(used.Microseconds())/1e3/k, float64(bytes)/float64(k*perFlush))
	}
	for _, l := range levels["L1"] {
		r, err := openLevel(l.path, l.minSeq, l.maxSeq)
		if err != nil {
			t.Fatal(err)
		}
		levels["L1-reopened"] = append(levels["L1-reopened"], r)
	}
	// Negative lookups probe every waiting level, newest first, as
	// snapshot.get does for a name no level holds.
	const probes = 20000
	miss := make([][]byte, probes)
	for j := range miss {
		s := names[k*perFlush+j%perFlush] + fmt.Sprint(j)
		miss[j] = append(scopeKey(nil, "g", vs(s)), s...)
	}
	for _, kind := range []string{"L1", "L1-reopened", "L2"} {
		for _, n := range []int{1, 8, 16, 64} {
			ls := levels[kind][:n]
			t0 := time.Now()
			for _, key := range miss {
				h := xxh3.Hash(key)
				for i := len(ls) - 1; i >= 0; i-- {
					if _, ok := ls[i].getHashed(key, h); ok {
						t.Fatal("hit")
					}
				}
			}
			fmt.Printf("FLUSHCOST lookup kind=%s k=%d neg_ns=%d\n", kind, n, time.Since(t0).Nanoseconds()/probes)
		}
	}
}
