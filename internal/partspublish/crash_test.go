//go:build sealrollup_integration

package partspublish

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/gob"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"syscall"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/objstorage/objstorageprovider"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/vfs"
	pf "github.com/go-graphite/go-carbon/internal/partsformat"
	pr "github.com/go-graphite/go-carbon/internal/partsread"
)

var crashEnvelopeKey = []byte{'p'}

func commitCrashSST(t *testing.T, db *pebble.DB, root string, plan *Plan) {
	t.Helper()
	var frontier [8]byte
	binary.LittleEndian.PutUint64(frontier[:], plan.envelope.Generation)
	paths := []string{}
	// Three disjoint files exercise atomic raw/archive-envelope/frontier ingest.
	for i, kv := range []struct{ k, v []byte }{{[]byte{'d'}, frontier[:]}, {[]byte{'f'}, frontier[:]}, {crashEnvelopeKey, plan.Value}} {
		path := filepath.Join(root, fmt.Sprintf("stage-%d.sst", i))
		file, err := vfs.Default.Create(path)
		if err != nil {
			t.Fatal(err)
		}
		writer := sstable.NewWriter(objstorageprovider.NewFileWritable(file), sstable.WriterOptions{})
		if err = writer.Set(kv.k, kv.v); err != nil {
			t.Fatal(err)
		}
		if err = writer.Close(); err != nil {
			t.Fatal(err)
		}
		paths = append(paths, path)
	}
	if err := db.Ingest(paths); err != nil {
		t.Fatal(err)
	}
	plan.Commit()
}
func loadCrashValue(t *testing.T, db *pebble.DB) []byte {
	t.Helper()
	v, c, err := db.Get(crashEnvelopeKey)
	if err != nil {
		t.Fatal(err)
	}
	copy := append([]byte(nil), v...)
	if err = c.Close(); err != nil {
		t.Fatal(err)
	}
	return copy
}
func crashDB(t *testing.T, root string) *pebble.DB {
	t.Helper()
	db, err := pebble.Open(filepath.Join(root, "db"), &pebble.Options{MaxOpenFiles: 64, MemTableSize: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	return db
}
func checkCrashOracle(t *testing.T, p *Publisher, fixture []oracleMetric) {
	t.Helper()
	count := 0
	for _, m := range fixture {
		h, err := p.Pin(pr.NewBudget(1<<30), 128<<20)
		if err != nil {
			t.Fatal(err)
		}
		for _, q := range m.Queries {
			s, err := h.Render(context.Background(), m.Metric.Name, q.From, q.Until, q.Now)
			if err != nil {
				t.Fatal(err)
			}
			if seriesDigest(s) != q.Digest {
				t.Fatalf("oracle mismatch metric%s range%d-%d", m.Metric.Name, q.From, q.Until)
			}
			count++
		}
		h.Close()
	}
	if count != 1680 {
		t.Fatalf("oracle query count%d", count)
	}
}
func TestPublisherCrashChild(t *testing.T) {
	root := os.Getenv("PARTS_PUBLISH_CHILD_ROOT")
	if root == "" {
		t.Skip("subprocess helper")
	}
	file, err := os.Open(filepath.Join(root, "fixture.gob"))
	if err != nil {
		t.Fatal(err)
	}
	var fixture []oracleMetric
	if err = gob.NewDecoder(file).Decode(&fixture); err != nil {
		t.Fatal(err)
	}
	_ = file.Close()
	db := crashDB(t, root)
	defer db.Close()
	p := testPublisher(t, filepath.Join(root, "parts"), 12345, nil)
	if err = p.Recover(loadCrashValue(t, db)); err != nil {
		t.Fatal(err)
	}
	target := os.Getenv("PARTS_PUBLISH_CRASH_STEP")
	p.options.Step = func(step string) {
		if step == target {
			_ = syscall.Kill(os.Getpid(), syscall.SIGKILL)
			select {}
		}
	}
	e := cloneEnvelope(p.current)
	e.Generation++
	e.Frontier = []uint64{e.Generation}
	index, _ := strconv.Atoi(os.Getenv("PARTS_PUBLISH_CRASH_INDEX"))
	index %= len(fixture)
	m := fixture[index]
	var id pf.PartID
	encodedID, err := hex.DecodeString(os.Getenv("PARTS_PUBLISH_NEW_ID"))
	if err != nil || len(encodedID) != len(id) {
		t.Fatal("invalid planned part UUID", err)
	}
	copy(id[:], encodedID)
	if id == (pf.PartID{}) {
		t.Fatal(err)
	}
	blocks := make([]pr.BlockData, len(m.Blocks))
	copy(blocks, m.Blocks)
	for i := range blocks {
		k, err := pf.Decode(blocks[i].Key)
		if err != nil {
			t.Fatal(err)
		}
		k.Part = id
		blocks[i].Key, err = pf.Encode(k)
		if err != nil {
			t.Fatal(err)
		}
	}
	ref := e.Parts[index]
	ref.Name = fmt.Sprintf("part-%x", id)
	ref.Generation = e.Generation
	e.Parts[index] = ref
	plan, err := p.PlanSeal(Request{Envelope: e, Added: map[string][]pr.BlockData{ref.Name: blocks}})
	if err != nil {
		t.Fatal(err)
	}
	commitCrashSST(t, db, root, plan)
	if err = p.Collect(); err != nil {
		t.Fatal(err)
	}
	t.Fatal("fault injection step not reached", target)
}
func runOracleCrashMatrix(t *testing.T, fixture []oracleMetric) {
	t.Helper()
	// Preserve every version while using the writer's normal bounded block
	// policy. One 24-byte filesystem Write per sample is an unrelated fixture
	// construction bottleneck on instrumented local filesystems.
	for i := range fixture {
		var blocks []pr.BlockData
		last := map[pr.Archive]int{}
		for _, b := range fixture[i].Blocks {
			index, ok := last[b.Archive]
			if !ok || len(blocks[index].Samples)+len(b.Samples) > pr.ArchiveBlockBytes(b.Archive)/24 {
				index = len(blocks)
				last[b.Archive] = index
				blocks = append(blocks, pr.BlockData{SlotBlock: pr.SlotBlock{ID: b.ID, Archive: b.Archive, Slots: pr.SlotRange{From: 0, Until: b.Archive.Points}, Key: slices.Clone(b.Key)}})
			}
			blocks[index].Samples = append(blocks[index].Samples, b.Samples...)
		}
		fixture[i].Blocks = blocks
	}
	count, err := strconv.Atoi(os.Getenv("PARTS_PUBLISH_CRASHES"))
	if err != nil || count < 200 {
		t.Fatal("PARTS_PUBLISH_CRASHES must be >=200")
	}
	root := t.TempDir()
	f, err := os.Create(filepath.Join(root, "fixture.gob"))
	if err != nil {
		t.Fatal(err)
	}
	if err = gob.NewEncoder(f).Encode(fixture); err != nil {
		t.Fatal(err)
	}
	if err = f.Close(); err != nil {
		t.Fatal(err)
	}
	var catalog []testMetric
	added := map[string][]pr.BlockData{}
	e := Envelope{Version: 1, Generation: 1, Frontier: []uint64{1}}
	for i, m := range fixture {
		name := fmt.Sprintf("base-%d", i)
		scope, _ := pf.PartitionPrefixForGroup("test", pf.Route(pf.ID(m.Metric.Name)), 0)
		e.Parts = append(e.Parts, Ref{Name: name, Generation: 1, Version: 1, Codec: CodecV1, Scope: scope})
		catalog = append(catalog, m.Metric)
		added[name] = m.Blocks
	}
	e.Catalog, err = json.Marshal(catalog)
	if err != nil {
		t.Fatal(err)
	}
	staged := 0
	p := testPublisher(t, filepath.Join(root, "parts"), 1, func(step string) {
		if step == "staged" {
			staged++
			if staged%50 == 0 {
				t.Logf("fixture staged parts=%d/%d", staged, len(fixture))
			}
		}
	})
	plan, err := p.PlanSeal(Request{Envelope: e, Added: added})
	if err != nil {
		t.Fatal(err)
	}
	db := crashDB(t, root)
	commitCrashSST(t, db, root, plan)
	p.Close()
	if err = db.Close(); err != nil {
		t.Fatal(err)
	}
	stats := map[string]int{}
	steps := []string{"staged", "synced", "committed", "published", "retiring"}
	start := time.Now()
	for i := 0; i < count; i++ {
		step := steps[i%len(steps)]
		var intendedID pf.PartID
		if _, err = rand.Read(intendedID[:]); err != nil {
			t.Fatal(err)
		}
		intended := cloneEnvelope(e)
		if step != "staged" && step != "synced" {
			intended.Generation++
			intended.Frontier = []uint64{intended.Generation}
			index := i % len(fixture)
			intended.Parts[index].Name = fmt.Sprintf("part-%x", intendedID)
			intended.Parts[index].Generation = intended.Generation
		}
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestPublisherCrashChild$", "-test.timeout=25s")
		cmd.Env = append(os.Environ(), "PARTS_PUBLISH_CHILD_ROOT="+root, "PARTS_PUBLISH_CRASH_STEP="+step, "PARTS_PUBLISH_CRASH_INDEX="+strconv.Itoa(i), "PARTS_PUBLISH_NEW_ID="+hex.EncodeToString(intendedID[:]))
		output, err := cmd.CombinedOutput()
		timedOut := ctx.Err() != nil
		cancel()
		if timedOut {
			t.Fatalf("child timed out instead of reaching requested cut %s: %s", step, output)
		}
		if err == nil {
			t.Fatal("child survived", step)
		}
		exit, ok := err.(*exec.ExitError)
		if !ok {
			t.Fatalf("child error %v %s", err, output)
		}
		status, ok := exit.Sys().(syscall.WaitStatus)
		if !ok || !status.Signaled() || status.Signal() != syscall.SIGKILL {
			t.Fatalf("not killed at%s:%v %s", step, err, output)
		}
		expected := e.Generation
		if step != "staged" && step != "synced" {
			expected++
		}
		db = crashDB(t, root)
		value := loadCrashValue(t, db)
		var recovered Envelope
		if err = json.Unmarshal(value, &recovered); err != nil {
			t.Fatal(err)
		}
		if recovered.Generation != expected || len(recovered.Parts) != len(e.Parts) {
			t.Fatalf("lost/duplicate refs generation%d expected%d parts%d", recovered.Generation, expected, len(recovered.Parts))
		}
		if !bytes.Equal(recovered.Catalog, intended.Catalog) || !slices.Equal(recovered.Frontier, intended.Frontier) {
			t.Fatal("catalog/frontier mismatch")
		}
		for j, ref := range recovered.Parts {
			if !equalRef(ref, intended.Parts[j]) {
				t.Fatalf("lost/duplicated/replaced committed ref %d got%s want%s", j, ref, intended.Parts[j])
			}
		}
		seen := map[string]bool{}
		for _, r := range recovered.Parts {
			if seen[r.Name] {
				t.Fatal("duplicate part", r.Name)
			}
			seen[r.Name] = true
		}
		for _, key := range [][]byte{{'d'}, {'f'}} {
			v, c, er := db.Get(key)
			if er != nil {
				t.Fatal(er)
			}
			got := binary.LittleEndian.Uint64(v)
			_ = c.Close()
			if got != expected {
				t.Fatal("non-atomic raw/frontier", got, expected)
			}
		}
		e = recovered
		next := testPublisher(t, filepath.Join(root, "parts"), uint64(10000+i*1000), nil)
		if err = next.Recover(value); err != nil {
			t.Fatal(err)
		}
		checkCrashOracle(t, next, fixture)
		next.Close()
		if err = db.Close(); err != nil {
			t.Fatal(err)
		}
		stats[step]++
		if (i+1)%10 == 0 {
			t.Logf("crashes=%d queries=%d elapsed=%s", i+1, (i+1)*1680, time.Since(start))
		}
	}
	for _, step := range steps {
		t.Logf("step=%s crashes=%d oracle=1680-each lost=0 duplicated=0", step, stats[step])
	}
}
