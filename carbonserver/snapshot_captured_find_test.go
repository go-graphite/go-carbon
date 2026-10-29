package carbonserver

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"math/rand"
	"os"
	"sort"
	"strings"
	"testing"
)

// Opt-in proof on an immutable captured index. The ordinary suite needs no
// external corpus. Snapshot files are opened read-only and checksum validated.
type capturedFindInput struct {
	FileListCache, Root string
	Queries             []string
}

func capturedFindIndex(tb testing.TB) (*indexSnapshot, capturedFindInput) {
	tb.Helper()
	path := os.Getenv("GO_CARBON_FIND_CAPTURE")
	if path == "" {
		tb.Skip("GO_CARBON_FIND_CAPTURE not set")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		tb.Fatal(err)
	}
	var input capturedFindInput
	if err := json.Unmarshal(data, &input); err != nil {
		tb.Fatal(err)
	}
	s, err := openIndexSnapshot(input.FileListCache, input.Root)
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() { _ = s.close() })
	return s, input
}

func capturedFindQueries(s *indexSnapshot) []string {
	type directory struct {
		name  string
		state fstState
		depth int
	}
	type candidate struct {
		expr  string
		count int
	}
	rng := rand.New(rand.NewSource(241024))
	queue := []directory{{state: s.keys.accept(s.keys.start(), 0)}}
	var candidates []candidate
	for i := 0; i < len(queue) && i < 256; i++ {
		current := queue[i]
		var sample []directory
		var scratch []byte
		count, children := 0, 0
		s.keys.names(current.state, &scratch, func(name []byte, final bool, _ uint64, child fstState) {
			if final && strings.HasSuffix(string(name), ".wsp") {
				count++
			}
			if !child.ok {
				return
			}
			count++
			children++
			if current.depth >= 5 {
				return
			}
			at := len(sample)
			if at >= 8 {
				at = rng.Intn(children)
				if at >= 8 {
					return
				}
			}
			next := directory{name: string(name), state: child, depth: current.depth + 1}
			if current.name != "" {
				next.name = current.name + "/" + next.name
			}
			if at == len(sample) {
				sample = append(sample, next)
			} else {
				sample[at] = next
			}
		})
		query := "*"
		if current.name != "" {
			query = current.name + "/*"
		}
		// Bound old-path memory and execution time on a serving machine.
		if count > 0 && count <= 150000 {
			candidates = append(candidates, candidate{query, count})
		}
		queue = append(queue, sample...)
	}
	sort.Slice(candidates, func(i, j int) bool { return candidates[i].count > candidates[j].count })
	queries := []string{"*"}
	for _, candidate := range candidates {
		if candidate.expr != "*" {
			queries = append(queries, candidate.expr)
		}
		if len(queries) >= 9 {
			break
		}
	}
	return queries
}

func capturedFindTrie(s *indexSnapshot) *trieIndex {
	ti := newTrie(".wsp", 0, nil)
	ti.snapshot = &indexSnapshot{index: s.index, keys: s.keys, metadata: s.metadata, openedAt: s.openedAt}
	return ti
}

func capturedFindDigest(names []string, leaves []bool, nodes []*trieNode, lookups uint32) string {
	h := sha256.New()
	var scratch [32]byte
	for i, name := range names {
		binary.LittleEndian.PutUint64(scratch[:8], uint64(len(name)))
		h.Write(scratch[:8])
		h.Write([]byte(name))
		if leaves[i] {
			h.Write([]byte{1})
			meta := nodes[i].meta.Load().(*fileMeta)
			for j, value := range []int64{meta.logicalSize, meta.physicalSize, meta.dataPoints, meta.firstSeenAt} {
				binary.LittleEndian.PutUint64(scratch[j*8:], uint64(value))
			}
			h.Write(scratch[:])
		} else {
			h.Write([]byte{0})
		}
	}
	binary.LittleEndian.PutUint64(scratch[:8], uint64(lookups))
	h.Write(scratch[:8])
	return hex.EncodeToString(h.Sum(nil))
}

func TestSnapshotCapturedFindOracle(t *testing.T) {
	s, input := capturedFindIndex(t)
	if len(input.Queries) == 0 {
		input.Queries = capturedFindQueries(s)
	}
	type result struct {
		Query, SHA256 string
		Limit, Rows   int
		Lookups       uint32
	}
	var results []result
	for _, query := range input.Queries {
		for _, limit := range []int{0, 1, 2, 17, 500, 1 << 30} {
			old, next := capturedFindTrie(s), capturedFindTrie(s)
			wn, wl, wd, wi, we := oldTrieQuery(old, query, limit, nil)
			gn, gl, gd, gi, ge := next.query(query, limit, nil)
			if we != nil || ge != nil {
				t.Fatalf("%q/%d: %v/%v", query, limit, we, ge)
			}
			want, got := capturedFindDigest(wn, wl, wd, wi), capturedFindDigest(gn, gl, gd, gi)
			if want != got {
				t.Fatalf("%q/%d: %d/%d rows, %s/%s", query, limit, len(wn), len(gn), want, got)
			}
			results = append(results, result{query, got, limit, len(gn), gi})
		}
	}
	data, err := json.MarshalIndent(struct {
		Input   capturedFindInput
		Results []result
	}{input, results}, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if output := os.Getenv("GO_CARBON_FIND_CAPTURE_OUT"); output != "" {
		file, err := os.OpenFile(output, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
		if err != nil {
			t.Fatal(err)
		}
		_, err = file.Write(data)
		closeErr := file.Close()
		if err != nil || closeErr != nil {
			t.Fatalf("write result: %v/%v", err, closeErr)
		}
	}
	t.Logf("%d exact captured-index comparisons, %d records", len(results), s.manifest.Records)
}

func BenchmarkSnapshotCapturedFind(b *testing.B) {
	s, input := capturedFindIndex(b)
	for _, query := range input.Queries {
		b.Run(query, func(b *testing.B) {
			for _, next := range []bool{false, true} {
				name, fn := "old", oldTrieQuery
				if next {
					name, fn = "new", (*trieIndex).query
				}
				b.Run(name, func(b *testing.B) {
					ti := capturedFindTrie(s)
					if _, _, _, _, err := fn(ti, query, 1<<30, nil); err != nil {
						b.Fatal(err)
					}
					b.ReportAllocs()
					for b.Loop() {
						if _, _, _, _, err := fn(ti, query, 1<<30, nil); err != nil {
							b.Fatal(err)
						}
					}
				})
			}
		})
	}
}
