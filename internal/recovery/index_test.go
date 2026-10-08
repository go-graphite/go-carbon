package recovery

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"math/rand"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/cespare/xxhash/v2"
	"github.com/go-graphite/go-carbon/points"
)

func fixture(t *testing.T) ([]byte, [2][]byte, map[string][]points.Point) {
	t.Helper()
	b := NewBuilder(nil)
	var source [2][]byte
	want := make(map[string][]points.Point)
	random := rand.New(rand.NewSource(99))
	// Deliberately record WAL before cache for some metrics. Replay still consumes
	// all cache records before any WAL record, matching RestoreFromDir.
	var input [2][]*points.Points
	for i := 0; i < 3000; i++ {
		file := random.Intn(2)
		name := fmt.Sprintf("namespace.metric%d", random.Intn(100))
		p := &points.Points{Metric: name, Data: make([]points.Point, random.Intn(4)+1)}
		for j := range p.Data {
			p.Data[j] = points.Point{Value: math.Float64frombits(random.Uint64()), Timestamp: int64(random.Uint64())}
		}
		raw := p.AppendBinary(nil)
		source[file] = append(source[file], raw...)
		if err := b.Add(file, p, len(raw)); err != nil {
			t.Fatal(err)
		}
		input[file] = append(input[file], p)
	}
	for _, batches := range input {
		for _, p := range batches {
			want[p.Metric] = append(want[p.Metric], p.Data...)
		}
	}
	var encoded bytes.Buffer
	if err := b.Write(&encoded); err != nil {
		t.Fatal(err)
	}
	return encoded.Bytes(), source, want
}

func TestRecoveryIndexMatchesLegacyOrder(t *testing.T) {
	data, source, want := fixture(t)
	index, err := Open(data, source[0], source[1])
	if err != nil {
		t.Fatal(err)
	}
	var count uint64
	for name, expected := range want {
		slot, ok, err := index.Find(name)
		if err != nil || !ok {
			t.Fatal("lookup", name, ok, err)
		}
		got, err := index.Read(slot)
		if err != nil || len(got.Data) != len(expected) {
			t.Fatal("read", name, err)
		}
		for i, p := range expected {
			if math.Float64bits(p.Value) != math.Float64bits(got.Data[i].Value) || p.Timestamp != got.Data[i].Timestamp {
				t.Fatal("point differs", name, i)
			}
		}
		count += uint64(len(expected))
	}
	if index.Metrics() != uint64(len(want)) || index.Points() != count {
		t.Fatal("aggregate differs")
	}
	if _, ok, err := index.Find("absent"); err != nil || ok {
		t.Fatal("absent lookup", ok, err)
	}
	visited := map[string]bool{}
	for slot := uint64(0); slot < index.Slots(); slot++ {
		name, ok, err := index.Name(slot)
		if err != nil {
			t.Fatal(err)
		}
		if ok {
			visited[name] = true
		}
	}
	if len(visited) != len(want) {
		t.Fatal("enumeration differs")
	}
	legacy := map[string][]points.Point{}
	for _, file := range source {
		if err := points.ReadBinary(bytes.NewReader(file), func(p *points.Points) { legacy[p.Metric] = append(legacy[p.Metric], p.Data...) }); err != nil {
			t.Fatal(err)
		}
	}
	// Values may include NaN: compare the exact encoded float bits above and the
	// complete re-encoding here, rather than floating-point equality.
	for name, expected := range want {
		a := (&points.Points{Metric: name, Data: expected}).AppendBinary(nil)
		b := (&points.Points{Metric: name, Data: legacy[name]}).AppendBinary(nil)
		if !bytes.Equal(a, b) {
			t.Fatal("stream oracle differs", name)
		}
	}
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for name := range want {
				slot, ok, err := index.Find(name)
				if err != nil || !ok {
					t.Error(err)
					return
				}
				if _, err = index.Read(slot); err != nil {
					t.Error(err)
					return
				}
			}
		}()
	}
	wg.Wait()
}

func TestRecoveryHashCollisionChecksMetric(t *testing.T) {
	b := NewBuilder(nil)
	names := []string{"target"}
	// Two names sharing their initial slot are sufficient to exercise probing.
	for i := 0; ; i++ {
		name := fmt.Sprintf("other%d", i)
		if xxhash.Sum64String(name)&3 == xxhash.Sum64String(names[0])&3 {
			names = append(names, name)
			break
		}
	}
	var cache []byte
	for i, name := range names {
		p := points.OnePoint(name, float64(i), 1)
		raw := p.AppendBinary(nil)
		cache = append(cache, raw...)
		if err := b.Add(0, p, len(raw)); err != nil {
			t.Fatal(err)
		}
	}
	var out bytes.Buffer
	if err := b.Write(&out); err != nil {
		t.Fatal(err)
	}
	index, err := Open(out.Bytes(), cache, nil)
	if err != nil {
		t.Fatal(err)
	}
	// Force equal stored hashes while retaining two different full metric names.
	// Find must never return a different name just because its hash matches.
	slot := xxhash.Sum64String(names[0]) & (index.slots - 1)
	first, _, _ := index.Name(slot)
	other := names[0]
	if first == other {
		other = names[1]
	}
	binary.LittleEndian.PutUint64(out.Bytes()[headerSize+slot*slotSize:], xxhash.Sum64String(other))
	found, ok, err := index.Find(other)
	if err != nil || !ok {
		t.Fatal(err, ok)
	}
	got, err := index.Read(found)
	if err != nil || got.Metric != other {
		t.Fatal("hash collision merged metrics", got, err)
	}
}

type shortWriter struct{}

func (shortWriter) Write(p []byte) (int, error) { return len(p) - 1, nil }

func TestRecoveryIndexRejectsInvalidExtents(t *testing.T) {
	data, source, _ := fixture(t)
	for n := 0; n < len(data); n += max(1, len(data)/200) {
		if _, err := Open(data[:n], source[0], source[1]); err == nil {
			t.Fatal("accepted truncation", n)
		}
	}
	for _, field := range []int{8, 16, 24, 32, 40, 48, 56, 64} {
		bad := bytes.Clone(data)
		binary.LittleEndian.PutUint64(bad[field:], math.MaxUint64)
		if _, err := Open(bad, source[0], source[1]); err == nil {
			t.Fatal("accepted bad header", field)
		}
	}
	if _, err := Open(data, source[0][:len(source[0])-1], source[1]); err == nil {
		t.Fatal("accepted incomplete source")
	}
	index, err := Open(data, source[0], source[1])
	if err != nil {
		t.Fatal(err)
	}
	bad := bytes.Clone(data)
	binary.LittleEndian.PutUint64(bad[index.recordOffsets[0]+16:], 1)
	if _, err := Open(bad, source[0], source[1]); err == nil {
		t.Fatal("accepted cyclic chain")
	}
	if err := NewBuilder(nil).Write(shortWriter{}); !errors.Is(err, io.ErrShortWrite) {
		t.Fatal("short write", err)
	}
}

func TestRecoveryBuilderConcurrentSources(t *testing.T) {
	b := NewBuilder(nil)
	var source [2][]byte
	var wg sync.WaitGroup
	for file := range source {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 1000; i++ {
				p := points.OnePoint("metric", float64(file), int64(i))
				raw := p.AppendBinary(nil)
				source[file] = append(source[file], raw...)
				if err := b.Add(file, p, len(raw)); err != nil {
					t.Error(err)
				}
			}
		}()
	}
	wg.Wait()
	var data bytes.Buffer
	if err := b.Write(&data); err != nil {
		t.Fatal(err)
	}
	index, err := Open(data.Bytes(), source[0], source[1])
	if err != nil {
		t.Fatal(err)
	}
	slot, _, _ := index.Find("metric")
	got, err := index.Read(slot)
	if err != nil {
		t.Fatal(err)
	}
	want := make([]points.Point, 0, 2000)
	for file := 0; file < 2; file++ {
		for i := 0; i < 1000; i++ {
			want = append(want, points.Point{Value: float64(file), Timestamp: int64(i)})
		}
	}
	if !reflect.DeepEqual(got.Data, want) {
		t.Fatal("concurrent writers changed replay order")
	}
}

func FuzzRecoveryIndex(f *testing.F) {
	var b bytes.Buffer
	_ = NewBuilder(nil).Write(&b)
	f.Add(b.Bytes(), []byte{}, []byte{})
	f.Fuzz(func(t *testing.T, data, cache, wal []byte) {
		index, err := Open(data, cache, wal)
		if err != nil {
			return
		}
		for slot := uint64(0); slot < index.Slots(); slot++ {
			name, ok, err := index.Name(slot)
			if err != nil {
				continue
			}
			if ok {
				_, _, _ = index.Find(name)
				_, _ = index.Read(slot)
			}
		}
	})
}

func TestRecoveryNewNames(t *testing.T) {
	b := NewBuilder(func(name string) bool { return name == "known" })
	var source []byte
	for _, name := range []string{"known", "new", "known", "other", "new"} {
		p := points.OnePoint(name, 1, 1)
		raw := p.AppendBinary(nil)
		source = append(source, raw...)
		if err := b.Add(0, p, len(raw)); err != nil {
			t.Fatal(err)
		}
	}
	var data bytes.Buffer
	if err := b.Write(&data); err != nil {
		t.Fatal(err)
	}
	index, err := Open(data.Bytes(), source, nil)
	if err != nil {
		t.Fatal(err)
	}
	names := map[string]bool{}
	if err = index.NewNames(func(name string) error { names[name] = true; return nil }); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(names, map[string]bool{"new": true, "other": true}) {
		t.Fatal("new metric catalog", names)
	}
}

func TestConcurrentBuilderMatchesSerial(t *testing.T) {
	var known sync.Map
	var lookups atomic.Int32
	newKnown := func() func(string) bool {
		lookups.Add(1)
		var calls int // a data race here would mean a lookup was shared
		return func(name string) bool {
			calls++
			_, ok := known.Load(name)
			return ok
		}
	}
	serial := NewBuilder(func(name string) bool { _, ok := known.Load(name); return ok })
	concurrent := NewConcurrentBuilder(newKnown, 7)
	var source []byte
	for i := 0; i < 50000; i++ {
		name := fmt.Sprintf("m.%d", i%20011)
		if i%3 == 0 {
			known.Store(name, true)
		}
		p := points.OnePoint(name, float64(i), int64(i))
		raw := p.AppendBinary(nil)
		source = append(source, raw...)
		for _, b := range []*Builder{serial, concurrent} {
			if err := b.Add(0, p, len(raw)); err != nil {
				t.Fatal(err)
			}
		}
	}
	collect := func(b *Builder) map[string]bool {
		var data bytes.Buffer
		if err := b.Write(&data); err != nil {
			t.Fatal(err)
		}
		index, err := Open(data.Bytes(), source, nil)
		if err != nil {
			t.Fatal(err)
		}
		names := map[string]bool{}
		if err = index.NewNames(func(name string) error { names[name] = true; return nil }); err != nil {
			t.Fatal(err)
		}
		return names
	}
	want, got := collect(serial), collect(concurrent)
	if len(want) == 0 || !reflect.DeepEqual(want, got) {
		t.Fatal("concurrent new metric catalogue differs", len(want), len(got))
	}
	if n := lookups.Load(); n < 2 || n > 7 {
		t.Fatal("unexpected lookup count", n)
	}
}

func TestRecoveryRejectsAliasesRequiringLegacyPersistence(t *testing.T) {
	for _, name := range []string{"", ".a", "a.", "a..b", "a/b", "a\x00b"} {
		t.Run(fmt.Sprintf("%q", name), func(t *testing.T) {
			p := points.OnePoint(name, 42, 1)
			raw := p.AppendBinary(nil)
			builder := NewBuilder(nil)
			if err := builder.Add(0, p, len(raw)); err != nil {
				t.Fatal(err)
			}
			var encoded bytes.Buffer
			if err := builder.Write(&encoded); err != nil {
				t.Fatal(err)
			}
			if _, err := Open(encoded.Bytes(), raw, nil); err == nil {
				t.Fatal("alias checkpoint accepted for early reads")
			}
			// Its legacy source remains intact for the existing persistence fallback.
			var restored []*points.Points
			if err := points.ReadBinary(bytes.NewReader(raw), func(p *points.Points) { restored = append(restored, p) }); err != nil {
				t.Fatal(err)
			}
			if len(restored) != 1 || !restored[0].Eq(p) {
				t.Fatal("fallback source changed")
			}
		})
	}
}
