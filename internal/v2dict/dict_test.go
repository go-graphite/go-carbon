package v2dict

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"hash/crc32"
	"math/rand/v2"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func synthName(i int) []byte {
	return fmt.Appendf(nil, "svc%02d.host-%05d.cpu.core%02d.metric%d", i%37, (i/37)%20000, i%16, i)
}

// model is the expected content: per scope, names in append order.
type model map[string][][]byte

func scopeOf(i int) (string, uint16) { return []string{"a", "ab", "b"}[i%3], uint16(i % 5) }

func fill(t *testing.T, d *Dict, from, to int, m model) {
	t.Helper()
	for i := from; i < to; i++ {
		g, vs := scopeOf(i)
		e, created, err := d.Append(g, vs, synthName(i), uint16(i%7))
		if err != nil {
			t.Fatal(err)
		}
		k := g + "/" + strconv.Itoa(int(vs))
		if !created || int(e.ID) != len(m[k]) {
			t.Fatalf("append %d: %+v created=%v, want ID %d", i, e, created, len(m[k]))
		}
		m[k] = append(m[k], synthName(i))
	}
}

func verify(t *testing.T, d *Dict, m model) {
	t.Helper()
	for k, names := range m {
		var g string
		var vs int
		fmt.Sscanf(k[bytes.IndexByte([]byte(k), '/')+1:], "%d", &vs)
		g = k[:bytes.IndexByte([]byte(k), '/')]
		for id, name := range names {
			e, ok := d.Lookup(g, uint16(vs), name)
			if !ok || e.ID != uint32(id) || e.Deleted() {
				t.Fatalf("Lookup(%s, %s) = %+v %v, want ID %d", k, name, e, ok, id)
			}
			got, e2, ok := d.LookupID(g, uint16(vs), uint32(id), nil)
			if !ok || !bytes.Equal(got, name) || e2 != e {
				t.Fatalf("LookupID(%s, %d) = %q %+v %v, want %q", k, id, got, e2, ok, name)
			}
		}
		if _, _, ok := d.LookupID(g, uint16(vs), uint32(len(names)), nil); ok {
			t.Fatalf("LookupID(%s, %d) found an unassigned ID", k, len(names))
		}
		if _, ok := d.Lookup(g, uint16(vs), []byte("missing.name")); ok {
			t.Fatal("Lookup found a missing name")
		}
		want := slices.Clone(names)
		slices.SortFunc(want, bytes.Compare)
		var got [][]byte
		d.Scan(g, uint16(vs), nil, func(name []byte, e Entry) bool { got = append(got, slices.Clone(name)); return true })
		if !slices.EqualFunc(got, want, bytes.Equal) {
			t.Fatalf("Scan(%s): %d names, want %d (first got %q want %q)", k, len(got), len(want), first(got), first(want))
		}
		if n := d.NextID(g, uint16(vs)); n != uint32(len(names)) {
			t.Fatalf("NextID(%s) = %d, want %d", k, n, len(names))
		}
	}
}

func first(b [][]byte) []byte {
	if len(b) == 0 {
		return nil
	}
	return b[0]
}

func TestAppendLookupReopen(t *testing.T) {
	dir := t.TempDir()
	d, err := Open(dir, Options{BufferBytes: 16 << 10})
	if err != nil {
		t.Fatal(err)
	}
	m := model{}
	fill(t, d, 0, 5000, m)
	d.Merge()
	verify(t, d, m)
	if st := d.Stats(); st.Levels < 1 || st.Entries < 4000 {
		t.Fatalf("want flushed levels, got %+v", st)
	}
	// Existing names return their entry; a second append is not new.
	if e, created, _ := d.Append("a", 0, synthName(0), 9); created || e.ID != 0 || e.Policy != 0 {
		t.Fatalf("re-append = %+v %v", e, created)
	}
	// Prefix seek.
	var n int
	d.Scan("a", 0, []byte("svc00."), func(name []byte, _ Entry) bool {
		if !bytes.HasPrefix(name, []byte("svc00.")) {
			t.Fatalf("prefix scan returned %q", name)
		}
		n++
		return true
	})
	if n == 0 {
		t.Fatal("prefix scan found nothing")
	}
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	d, err = Open(dir, Options{BufferBytes: 16 << 10})
	if err != nil {
		t.Fatal(err)
	}
	verify(t, d, m)
	fill(t, d, 5000, 6000, m)
	if err := d.MergeAll(); err != nil {
		t.Fatal(err)
	}
	if st := d.Stats(); st.Levels != 1 {
		t.Fatalf("after MergeAll: %+v", st)
	}
	verify(t, d, m)
	d.Close()
}

// TestDeleteRevive checks that a deleted name keeps its ID, that appending
// it again assigns a new ID, and that the old ID becomes unknown, wherever
// the entries live (append level, level files, after merges).
func TestDeleteRevive(t *testing.T) {
	for _, flushAt := range []string{"none", "after-delete", "after-revive", "both"} {
		t.Run(flushAt, func(t *testing.T) {
			d, err := Open(t.TempDir(), Options{NoMerge: true})
			if err != nil {
				t.Fatal(err)
			}
			defer d.Close()
			name := []byte("a.b.c")
			d.Append("g", 1, []byte("a.a"), 1)
			e0, _, _ := d.Append("g", 1, name, 2)
			d.Flush()
			if ok, _ := d.Delete("g", 1, name); !ok {
				t.Fatal("delete failed")
			}
			if ok, _ := d.Delete("g", 1, name); ok {
				t.Fatal("second delete succeeded")
			}
			if flushAt == "after-delete" || flushAt == "both" {
				d.Flush()
			}
			if e, ok := d.Lookup("g", 1, name); !ok || !e.Deleted() || e.ID != e0.ID {
				t.Fatalf("after delete: %+v %v", e, ok)
			}
			if _, e, ok := d.LookupID("g", 1, e0.ID, nil); !ok || !e.Deleted() {
				t.Fatalf("LookupID after delete: %+v %v", e, ok)
			}
			e1, created, _ := d.Append("g", 1, name, 2)
			if !created || e1.ID != 2 || e1.Deleted() {
				t.Fatalf("revival = %+v %v", e1, created)
			}
			if flushAt == "after-revive" || flushAt == "both" {
				d.Flush()
			}
			check := func(stage string) {
				if e, ok := d.Lookup("g", 1, name); !ok || e != e1 {
					t.Fatalf("%s: Lookup = %+v %v, want %+v", stage, e, ok, e1)
				}
				if got, _, ok := d.LookupID("g", 1, e0.ID, nil); ok {
					t.Fatalf("%s: old ID still resolves to %q", stage, got)
				}
				if got, e, ok := d.LookupID("g", 1, e1.ID, nil); !ok || string(got) != "a.b.c" || e != e1 {
					t.Fatalf("%s: LookupID(new) = %q %+v %v", stage, got, e, ok)
				}
				var seen []Entry
				d.Scan("g", 1, []byte("a.b"), func(_ []byte, e Entry) bool { seen = append(seen, e); return true })
				if len(seen) != 1 || seen[0] != e1 {
					t.Fatalf("%s: Scan = %+v", stage, seen)
				}
			}
			check("before merge")
			d.Flush()
			if err := d.MergeAll(); err != nil {
				t.Fatal(err)
			}
			check("after merge")
		})
	}
}

// TestCrashPoints kills (abandons) the dictionary at each crash point of a
// flush and a merge, then checks that a reopen serves every synced entry.
func TestCrashPoints(t *testing.T) {
	for _, point := range []string{"level-written", "flush-installed", "merge-installed"} {
		t.Run(point, func(t *testing.T) {
			dir := t.TempDir()
			// Flushes run in the background: explicit ones (a buffer no fill
			// reaches) leave a level, the append level the crashing flush
			// writes or the second level a merge needs, and no flush still
			// reading testHook.
			d, err := Open(dir, Options{BufferBytes: 1 << 20, NoMerge: true})
			if err != nil {
				t.Fatal(err)
			}
			m := model{}
			fill(t, d, 0, 1000, m)
			if err := d.Flush(); err != nil {
				t.Fatal(err)
			}
			fill(t, d, 1000, 2000, m)
			if point == "merge-installed" {
				if err := d.Flush(); err != nil {
					t.Fatal(err)
				}
			}
			if err := d.Sync(); err != nil {
				t.Fatal(err)
			}
			fired := false
			testHook = func(p string) error {
				if p == point && !fired {
					fired = true
					return errors.New("killed")
				}
				return nil
			}
			defer func() { testHook = nil }()
			if point == "merge-installed" {
				d.MergeAll()
			} else {
				d.Flush()
			}
			testHook = nil
			if !fired {
				t.Fatalf("crash point %s never reached", point)
			}
			// Abandon d without Close, as a kill would.
			d2, err := Open(dir, Options{BufferBytes: 8 << 10})
			if err != nil {
				t.Fatal(err)
			}
			verify(t, d2, m)
			fill(t, d2, 2000, 2500, m)
			d2.Sync()
			d2.Merge()
			verify(t, d2, m)
			d2.Close()
			ents, _ := os.ReadDir(dir)
			for _, e := range ents {
				if filepath.Ext(e.Name()) == ".tmp" {
					t.Fatalf("leftover %s", e.Name())
				}
			}
		})
	}
}

func TestTornLogTail(t *testing.T) {
	dir := t.TempDir()
	d, _ := Open(dir, Options{})
	m := model{}
	fill(t, d, 0, 300, m)
	d.Sync()
	logs, _ := filepath.Glob(filepath.Join(dir, "*.log"))
	f, _ := os.OpenFile(logs[0], os.O_WRONLY|os.O_APPEND, 0)
	f.Write([]byte{40, 0, 0, 0, 1, 2, 3, 4, 5}) // a torn record
	f.Close()
	d2, err := Open(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	verify(t, d2, m)
	fill(t, d2, 300, 400, m)
	d2.Close()
	d3, _ := Open(dir, Options{})
	verify(t, d3, m)
	d3.Close()
}

// TestKill9 kills a child process that appends and syncs, at a random
// moment, and checks that every name it reported synced is present with
// the ID it was given.
func TestKill9(t *testing.T) {
	if dir := os.Getenv("V2DICT_KILL_CHILD"); dir != "" {
		d, err := Open(dir, Options{BufferBytes: 32 << 10})
		if err != nil {
			fmt.Println("ERR", err)
			os.Exit(1)
		}
		for i := int(d.NextID("g", 7)); ; i++ {
			if _, _, err := d.Append("g", 7, synthName(i), 1); err != nil {
				fmt.Println("ERR", err)
				os.Exit(1)
			}
			if i%50 == 49 {
				if err := d.Sync(); err != nil {
					fmt.Println("ERR", err)
					os.Exit(1)
				}
				fmt.Println("synced", i+1)
			}
		}
	}
	dir := t.TempDir()
	synced := 0
	for round := range 3 {
		cmd := exec.Command(os.Args[0], "-test.run=^TestKill9$")
		cmd.Env = append(os.Environ(), "V2DICT_KILL_CHILD="+dir)
		out, err := cmd.StdoutPipe()
		if err != nil {
			t.Fatal(err)
		}
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}
		sc := bufio.NewScanner(out)
		target := synced + 1000 + rand.IntN(3000)
		for sc.Scan() {
			if n, err := strconv.Atoi(string(bytes.TrimPrefix(sc.Bytes(), []byte("synced ")))); err == nil {
				synced = n
			} else {
				t.Fatalf("child: %s", sc.Text())
			}
			if synced >= target {
				break
			}
		}
		cmd.Process.Kill()
		cmd.Wait()
		d, err := Open(dir, Options{})
		if err != nil {
			t.Fatal(err)
		}
		if n := int(d.NextID("g", 7)); n < synced {
			t.Fatalf("round %d: recovered %d names, synced %d", round, n, synced)
		}
		for i := range synced {
			if e, ok := d.Lookup("g", 7, synthName(i)); !ok || e.ID != uint32(i) {
				t.Fatalf("round %d: name %d after kill: %+v %v (synced %d)", round, i, e, ok, synced)
			}
		}
		n := d.NextID("g", 7)
		for id := range n {
			if name, _, ok := d.LookupID("g", 7, id, nil); !ok || !bytes.Equal(name, synthName(int(id))) {
				t.Fatalf("round %d: ID %d -> %q %v", round, id, name, ok)
			}
		}
		t.Logf("round %d: synced %d, recovered %d, %+v", round, synced, n, d.Stats())
		d.Close()
	}
}

// TestMergeDuringLookup runs lookups, ID lookups and scans while a writer
// appends with small flushes, so merges install under the readers.
func TestMergeDuringLookup(t *testing.T) {
	d, err := Open(t.TempDir(), Options{BufferBytes: 8 << 10, Fanout: 2})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	const total = 20000
	var done atomic.Int64
	var wg sync.WaitGroup
	stop := make(chan struct{})
	for r := range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rng := rand.New(rand.NewPCG(uint64(r), 1))
			for {
				select {
				case <-stop:
					return
				default:
				}
				n := int(done.Load())
				if n == 0 {
					continue
				}
				i := rng.IntN(n)
				if e, ok := d.Lookup("g", 3, synthName(i)); !ok || e.ID != uint32(i) {
					t.Errorf("Lookup(%d) = %+v %v", i, e, ok)
					return
				}
				if name, _, ok := d.LookupID("g", 3, uint32(i), nil); !ok || !bytes.Equal(name, synthName(i)) {
					t.Errorf("LookupID(%d) = %q %v", i, name, ok)
					return
				}
				if rng.IntN(64) == 0 {
					c := 0
					d.Scan("g", 3, nil, func([]byte, Entry) bool { c++; return true })
					if c < n {
						t.Errorf("Scan saw %d names, want >= %d", c, n)
						return
					}
				}
			}
		}()
	}
	for i := range total {
		if _, _, err := d.Append("g", 3, synthName(i), 0); err != nil {
			t.Fatal(err)
		}
		done.Store(int64(i + 1))
	}
	time.Sleep(50 * time.Millisecond)
	close(stop)
	wg.Wait()
	d.Merge()
	t.Logf("%+v", d.Stats())
}

func TestImportRekey(t *testing.T) {
	src, _ := Open(t.TempDir(), Options{BufferBytes: 8 << 10})
	defer src.Close()
	for i := range 1000 {
		src.Append("g", 9, synthName(i), uint16(i%3))
	}
	src.Delete("g", 9, synthName(5))
	// Move: the target does not own the vshard; IDs are kept.
	dst, _ := Open(t.TempDir(), Options{BufferBytes: 8 << 10})
	defer dst.Close()
	if err := dst.Import("g", 9, src.Entries("g", 9)); err != nil {
		t.Fatal(err)
	}
	src.Scan("g", 9, nil, func(name []byte, e Entry) bool {
		if got, ok := dst.Lookup("g", 9, name); !ok || got != e {
			t.Fatalf("imported %q = %+v, want %+v", name, got, e)
		}
		return true
	})
	if err := dst.Import("g", 9, src.Entries("g", 9)); err == nil {
		t.Fatal("import into a non-empty vshard succeeded")
	}
	// Gap repair into a live owner with its own IDs: re-key.
	live, _ := Open(t.TempDir(), Options{BufferBytes: 8 << 10})
	defer live.Close()
	for i := 999; i >= 500; i-- { // other admission order, other IDs
		live.Append("g", 9, synthName(i), uint16(i%3))
	}
	m, err := live.Rekey("g", 9, src.Entries("g", 9))
	if err != nil {
		t.Fatal(err)
	}
	if m[5] != NoID {
		t.Fatalf("deleted donor ID maps to %d", m[5])
	}
	for i := range 1000 {
		if i == 5 {
			continue
		}
		e, ok := live.Lookup("g", 9, synthName(i))
		if !ok || m[i] != e.ID {
			t.Fatalf("rekey %d -> %d, live has %+v %v", i, m[i], e, ok)
		}
	}
	if m[600] != uint32(999-600) {
		t.Fatalf("existing live name re-keyed to %d, want %d", m[600], 999-600)
	}
}

func scaleName(dst []byte, i int) []byte {
	return fmt.Appendf(dst, "apps.svc%03d.dc%d.host-%05d.http.requests.status.m%03d.count", i%300, (i/300)%4, (i/1200)%2500, i/3000000)
}

func liveHeap() uint64 {
	runtime.GC()
	runtime.GC()
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	return ms.HeapAlloc
}

// TestDictScale measures the dictionary at V2DICT_SCALE synthetic names over
// 4096 vshards: heap and page cache per name, append throughput, merge time,
// lookup by name (hit, miss) and by ID, scan and reopen.
func TestDictScale(t *testing.T) {
	n, _ := strconv.Atoi(os.Getenv("V2DICT_SCALE"))
	if n == 0 {
		t.Skip("set V2DICT_SCALE")
	}
	const vshards = 4096
	vs := func(name []byte) uint16 { return uint16(crc32.ChecksumIEEE(name) % vshards) }
	dir := t.TempDir()
	base := liveHeap()
	d, err := Open(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	var nb []byte
	t0 := time.Now()
	for i := range n {
		nb = scaleName(nb[:0], i)
		if _, _, err := d.Append("g", vs(nb), nb, uint16(i%8)); err != nil {
			t.Fatal(err)
		}
		if i%100000 == 99999 {
			d.Sync()
		}
	}
	d.Sync()
	appendDur := time.Since(t0)
	d.Merge()
	settled := time.Since(t0)
	st1 := d.Stats()
	heapLevels := liveHeap() - base
	t1 := time.Now()
	if err := d.MergeAll(); err != nil {
		t.Fatal(err)
	}
	mergeAll := time.Since(t1)
	st := d.Stats()
	heapOne := liveHeap() - base
	// Lookups over a sample of names prepared outside the timed loops.
	const sample = 200000
	rng := rand.New(rand.NewPCG(1, 2))
	names := make([][]byte, sample)
	shards := make([]uint16, sample)
	ids := make([]uint32, sample)
	for j := range names {
		names[j] = scaleName(nil, rng.IntN(n))
		shards[j] = vs(names[j])
		e, ok := d.Lookup("g", shards[j], names[j])
		if !ok {
			t.Fatalf("missing %q", names[j])
		}
		ids[j] = e.ID
	}
	timeIt := func(f func(j int)) float64 {
		t := time.Now()
		for j := range sample {
			f(j)
		}
		return float64(time.Since(t).Nanoseconds()) / sample
	}
	hit := timeIt(func(j int) { d.Lookup("g", shards[j], names[j]) })
	miss := timeIt(func(j int) { d.Lookup("g", shards[j], names[j][:len(names[j])-1]) })
	var buf []byte
	byID := timeIt(func(j int) { buf, _, _ = d.LookupID("g", shards[j], ids[j], buf[:0]) })
	t2 := time.Now()
	scanned := 0
	for v := range uint16(64) {
		d.Scan("g", v, nil, func([]byte, Entry) bool { scanned++; return true })
	}
	scanNs := float64(time.Since(t2).Nanoseconds()) / float64(scanned)
	heapSample := liveHeap() - base
	d.Close()
	t3 := time.Now()
	d, err = Open(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	reopen := time.Since(t3)
	heapReopen := liveHeap() - base - (heapSample - heapOne) // without the lookup sample
	if e, ok := d.Lookup("g", shards[0], names[0]); !ok || e.ID != ids[0] {
		t.Fatal("lookup after reopen failed")
	}
	d.Close()
	t.Logf("SCALE names=%d vshards=%d append=%.0f names/s (%.2fs, settled with merges %.2fs) levels=%d->%d", n, vshards, float64(n)/appendDur.Seconds(), appendDur.Seconds(), settled.Seconds(), st1.Levels, st.Levels)
	t.Logf("SCALE heap B/name: levels as settled %.2f, one level %.2f, after reopen %.2f; page cache B/name %.2f (%d MB) with %d levels, %.2f after MergeAll",
		float64(heapLevels)/float64(n), float64(heapOne)/float64(n), float64(heapReopen)/float64(n), float64(st1.FileBytes)/float64(n), st1.FileBytes>>20, st1.Levels, float64(st.FileBytes)/float64(n))
	t.Logf("SCALE MergeAll=%.2fs lookup hit=%.0f ns miss=%.0f ns byID=%.0f ns scan=%.0f ns/name reopen=%.1f ms", mergeAll.Seconds(), hit, miss, byID, scanNs, float64(reopen.Microseconds())/1000)
}

// TestGeneration checks that the generation moves only at Sync, flush and
// open, grows on every durable change, and never goes back across a reopen.
func TestGeneration(t *testing.T) {
	dir := t.TempDir()
	d, err := Open(dir, Options{BufferBytes: 16 << 10, NoMerge: true})
	if err != nil {
		t.Fatal(err)
	}
	g0 := d.Generation()
	if _, _, err := d.Append("a", 1, []byte("x.one"), 0); err != nil {
		t.Fatal(err)
	}
	if g := d.Generation(); g != g0 {
		t.Fatalf("generation moved before Sync: %d -> %d", g0, g)
	}
	if err := d.Sync(); err != nil {
		t.Fatal(err)
	}
	g1 := d.Generation()
	if g1 <= g0 {
		t.Fatalf("Sync did not grow the generation: %d -> %d", g0, g1)
	}
	if _, _, err := d.Append("a", 1, []byte("x.one"), 0); err != nil || d.Sync() != nil || d.Generation() != g1 {
		t.Fatalf("an existing name changed the generation: %d -> %d (%v)", g1, d.Generation(), err)
	}
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	if d, err = Open(dir, Options{BufferBytes: 16 << 10, NoMerge: true}); err != nil {
		t.Fatal(err)
	}
	if g := d.Generation(); g != g1 {
		t.Fatalf("reopen changed the generation: %d -> %d", g1, g)
	}
	last := g1
	for i := 0; i < 2000; i++ { // several flushes
		if _, _, err := d.Append("a", uint16(i%8), synthName(i), 0); err != nil {
			t.Fatal(err)
		}
		if i%97 == 0 {
			if err := d.Sync(); err != nil {
				t.Fatal(err)
			}
		}
		if g := d.Generation(); g < last {
			t.Fatalf("generation went back: %d -> %d", last, g)
		} else {
			last = g
		}
	}
	// A background flush can still hold the first frozen level while the
	// remaining appends accumulate. Flush waits and publishes that remainder.
	if err := d.Flush(); err != nil {
		t.Fatal(err)
	}
	if g := d.Generation(); g < last {
		t.Fatalf("flush went back: %d -> %d", last, g)
	} else {
		last = g
	}
	if st := d.Stats(); st.Levels < 2 {
		t.Fatalf("want flushes, got %+v", st)
	}
	d.Close()
	if d, err = Open(dir, Options{NoMerge: true}); err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	if g := d.Generation(); g < last {
		t.Fatalf("reopen went back: %d -> %d", last, g)
	}
}

// TestAppendDuringFlush: the level file of a full append level is written in
// the background, so appends below the next table's bound, syncs and lookups
// go on while that write is stalled. The frozen level stays readable until
// its file replaces it.
func TestAppendDuringFlush(t *testing.T) {
	dir := t.TempDir()
	release, blocked := make(chan struct{}), make(chan struct{}, 1)
	testHook = func(p string) error {
		if p == "level-written" {
			select {
			case blocked <- struct{}{}:
			default:
			}
			<-release
		}
		return nil
	}
	defer func() { testHook = nil }()
	d, err := Open(dir, Options{BufferBytes: 4 << 10, NoMerge: true})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := d.Append("g", 3, synthName(0), 0); err != nil {
		t.Fatal(err)
	}
	if err := d.freeze(true); err != nil {
		t.Fatal(err)
	}
	<-blocked
	n := 1
	total := n + 8 // leave room below the 4 KiB active-table bound
	done := make(chan error, 1)
	go func() {
		for i := n; i < total; i++ {
			if _, _, err := d.Append("g", 3, synthName(i), 0); err != nil {
				done <- err
				return
			}
		}
		done <- d.Sync()
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(10 * time.Second):
		close(release)
		t.Fatal("appends waited on a stalled level file write")
	}
	check := func(d *Dict) {
		t.Helper()
		for i := range total {
			if e, ok := d.Lookup("g", 3, synthName(i)); !ok || e.ID != uint32(i) {
				t.Fatalf("Lookup(%d) = %+v %v", i, e, ok)
			}
			if name, _, ok := d.LookupID("g", 3, uint32(i), nil); !ok || !bytes.Equal(name, synthName(i)) {
				t.Fatalf("LookupID(%d) = %q %v", i, name, ok)
			}
		}
		c := 0
		d.Scan("g", 3, nil, func([]byte, Entry) bool { c++; return true })
		if c != total {
			t.Fatalf("Scan saw %d names, want %d", c, total)
		}
	}
	check(d)
	if st := d.Stats(); st.Levels != 0 {
		t.Fatalf("a level was installed while its write was stalled: %+v", st)
	}
	close(release)
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	if d, err = Open(dir, Options{BufferBytes: 4 << 10, NoMerge: true}); err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	check(d)
}
