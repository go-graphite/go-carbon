package partspublish

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"math"
	"os"
	"path/filepath"
	"sync"
	"testing"

	pf "github.com/go-graphite/go-carbon/internal/partsformat"
	pr "github.com/go-graphite/go-carbon/internal/partsread"
)

type testMetric struct {
	Name     string
	Group    pf.Group
	Archives []pr.Archive
}

func testDecoder(base uint64) DecodeCatalog {
	return func(raw json.RawMessage) (pr.Catalog, func(Ref, []byte) (uint64, error), int64, error) {
		var entries []testMetric
		if err := json.Unmarshal(raw, &entries); err != nil {
			return nil, nil, 0, err
		}
		cat := pr.CatalogFuncs{LookupFunc: func(n string) (pr.Metric, bool) {
			for i, m := range entries {
				if m.Name == n {
					return pr.Metric{ID: base + uint64(i), Archives: m.Archives}, true
				}
			}
			return pr.Metric{}, false
		}, FindFunc: func(ctx context.Context, n string, y func(string) error) error {
			for _, m := range entries {
				if m.Name == n {
					if err := y(n); err != nil {
						return err
					}
				}
			}
			return ctx.Err()
		}}
		resolve := func(r Ref, b []byte) (uint64, error) {
			k, err := pf.Decode(b)
			if err != nil {
				return 0, err
			}
			if !bytes.HasPrefix(b, r.Scope) {
				return 0, errors.New("scope mismatch")
			}
			for i, m := range entries {
				if string(k.Metric) == m.Name && k.Group == m.Group && int(k.Archive) < len(m.Archives) {
					return base + uint64(i), nil
				}
			}
			return 0, errors.New("unknown portable metric")
		}
		return cat, resolve, int64(len(raw) * 2), nil
	}
}
func testRequest(t *testing.T, generation uint64, name string, bits uint64) Request {
	t.Helper()
	a := pr.Archive{Step: 1, Points: 60}
	metric := pf.ID("test.metric")
	scope, err := pf.PartitionPrefixForGroup("test", pf.Route(metric), 0)
	if err != nil {
		t.Fatal(err)
	}
	key, err := pf.Encode(pf.Key{Group: "test", VShard: pf.Route(metric), Metric: metric, Part: pf.PartID{byte(generation)}, Archive: 0})
	if err != nil {
		t.Fatal(err)
	}
	cat, err := json.Marshal([]testMetric{{string(metric), "test", []pr.Archive{a}}})
	if err != nil {
		t.Fatal(err)
	}
	return Request{Envelope: Envelope{Version: 1, Generation: generation, Catalog: cat, Frontier: []uint64{generation}, Parts: []Ref{{Name: name, Generation: generation, Version: 1, Codec: CodecV1, Scope: scope}}}, Added: map[string][]pr.BlockData{name: {{SlotBlock: pr.SlotBlock{ID: 777, Archive: a, Slots: pr.SlotRange{From: 0, Until: 60}, Key: key}, Samples: []pr.Sample{{Time: 100, Bits: bits, Sequence: generation}}}}}}
}
func testPublisher(t *testing.T, root string, base uint64, step func(string)) *Publisher {
	t.Helper()
	p, err := New(Options{Root: root, DecodeCatalog: testDecoder(base), Step: step})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(p.Close)
	return p
}
func renderBits(t *testing.T, h *pr.Handle) uint64 {
	t.Helper()
	s, err := h.Render(context.Background(), "test.metric", 99, 101, 110)
	if err != nil {
		t.Fatal(err)
	}
	if s == nil || len(s.Values) != 2 {
		t.Fatalf("bad render %+v", s)
	}
	return math.Float64bits(s.Values[0])
}
func pin(t *testing.T, p *Publisher) *pr.Handle {
	t.Helper()
	h, err := p.Pin(pr.NewBudget(1<<30), 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(h.Close)
	return h
}
func commitRequest(t *testing.T, p *Publisher, r Request) *Plan {
	t.Helper()
	plan, err := p.PlanSeal(r)
	if err != nil {
		t.Fatal(err)
	}
	plan.Commit()
	return plan
}

func TestPublishRecoverPortableRemap(t *testing.T) {
	root := t.TempDir()
	p := testPublisher(t, root, 1, nil)
	want := uint64(0x8000000000000000)
	plan := commitRequest(t, p, testRequest(t, 1, "a", want))
	if got := renderBits(t, pin(t, p)); got != want {
		t.Fatalf("got %x", got)
	}
	// A higher complete orphan cannot be promoted on restart.
	r := testRequest(t, 999, "orphan", 0x7ff8000000000042)
	if _, err := pr.PublishBlockPart(root, "orphan", 999, r.Added["orphan"]); err != nil {
		t.Fatal(err)
	}
	p.Close()
	next := testPublisher(t, root, 987, nil)
	if err := next.Recover(plan.Value); err != nil {
		t.Fatal(err)
	}
	if got := renderBits(t, pin(t, next)); got != want {
		t.Fatalf("remap %x", got)
	}
	if next.current.Generation != 1 {
		t.Fatal("promoted orphan")
	}
}
func TestSharedPartSurvivesMultipleViewsAndRetiresAfterPins(t *testing.T) {
	root := t.TempDir()
	p := testPublisher(t, root, 1, nil)
	r := testRequest(t, 1, "a", 1)
	commitRequest(t, p, r)
	old := pin(t, p)
	// Same part and same catalog in the next view must not be closed with old.
	r.Envelope.Generation = 2
	r.Envelope.Frontier = []uint64{2}
	r.Added = nil
	commitRequest(t, p, r)
	newer := pin(t, p)
	old.Close()
	if got := renderBits(t, newer); got != 1 {
		t.Fatal(got)
	}
	commitRequest(t, p, testRequest(t, 3, "b", 2))
	if err := p.Collect(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(root, "a")); err != nil {
		t.Fatal("deleted pinned part", err)
	}
	newer.Close()
	if err := p.Collect(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(root, "a")); !os.IsNotExist(err) {
		t.Fatal("retired part remains", err)
	}
}
func TestRecoveryRejectsMissingCodecScopeAndGeneration(t *testing.T) {
	root := t.TempDir()
	p := testPublisher(t, root, 1, nil)
	plan := commitRequest(t, p, testRequest(t, 1, "a", 1))
	p.Close()
	var base Envelope
	if err := json.Unmarshal(plan.Value, &base); err != nil {
		t.Fatal(err)
	}
	for _, mode := range []string{"missing", "codec", "scope", "generation", "duplicate"} {
		t.Run(mode, func(t *testing.T) {
			e := cloneEnvelope(base)
			switch mode {
			case "missing":
				e.Parts[0].Name = "missing"
			case "codec":
				e.Parts[0].Codec = "compact-unknown"
			case "scope":
				e.Parts[0].Scope = []byte("other")
			case "generation":
				e.Generation = 2
				e.Parts[0].Generation = 2
			case "duplicate":
				e.Parts = append(e.Parts, e.Parts[0])
			}
			value, _ := json.Marshal(e)
			p := testPublisher(t, root, 9, nil)
			if err := p.Recover(value); err == nil {
				t.Fatal("accepted invalid committed refs")
			}
		})
	}
}
func TestAbortDoesNotPublishOrPermitFurtherSeals(t *testing.T) {
	p := testPublisher(t, t.TempDir(), 1, nil)
	commitRequest(t, p, testRequest(t, 1, "a", 1))
	plan, err := p.PlanSeal(testRequest(t, 2, "b", 2))
	if err != nil {
		t.Fatal(err)
	}
	plan.Abort()
	if got := renderBits(t, pin(t, p)); got != 1 {
		t.Fatal(got)
	}
	if _, err := p.PlanSeal(testRequest(t, 3, "c", 3)); err == nil {
		t.Fatal("continued after uncertain failure")
	}
}

func TestEnvelopeCopyPinDelaysRetirement(t *testing.T) {
	root := t.TempDir()
	p := testPublisher(t, root, 1, nil)
	commitRequest(t, p, testRequest(t, 1, "a", 1))
	snapshot, err := p.PinEnvelope()
	if err != nil {
		t.Fatal(err)
	}
	commitRequest(t, p, testRequest(t, 2, "b", 2))
	if err = p.Collect(); err != nil {
		t.Fatal(err)
	}
	if _, err = os.Stat(filepath.Join(root, "a")); err != nil {
		t.Fatal("copy pin lost part", err)
	}
	snapshot.Close()
	snapshot.Close()
	if err = p.Collect(); err != nil {
		t.Fatal(err)
	}
	if _, err = os.Stat(filepath.Join(root, "a")); !os.IsNotExist(err) {
		t.Fatal("copy part remains", err)
	}
}
func TestRecoverRejectsCorruptPayloadBeforePublication(t *testing.T) {
	root := t.TempDir()
	p := testPublisher(t, root, 1, nil)
	plan := commitRequest(t, p, testRequest(t, 1, "a", 1))
	p.Close()
	f, err := os.OpenFile(filepath.Join(root, "a", "blocks"), os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = f.WriteAt([]byte{0xff}, 8); err != nil {
		t.Fatal(err)
	}
	if err = f.Close(); err != nil {
		t.Fatal(err)
	}
	next := testPublisher(t, root, 1, nil)
	if err = next.Recover(plan.Value); err == nil {
		t.Fatal("accepted corrupt payload")
	}
	if h, err := next.Pin(pr.NewBudget(1<<30), 0); err == nil {
		h.Close()
		t.Fatal("published corrupt source")
	}
}
func TestFrozenCatalogSurvivesCallerMutationAndChangedRemap(t *testing.T) {
	root := t.TempDir()
	p := testPublisher(t, root, 1, nil)
	r := testRequest(t, 1, "a", 1)
	commitRequest(t, p, r)
	old := pin(t, p)
	r.Envelope.Generation = 2
	r.Envelope.Frontier = []uint64{2}
	r.Envelope.Catalog = append([]byte(" "), r.Envelope.Catalog...)
	r.Added = nil
	plan, err := p.PlanSeal(r)
	if err != nil {
		t.Fatal(err)
	}
	r.Envelope.Catalog[0] = 'X'
	r.Envelope.Parts[0].Scope[0] = 'X'
	r.Envelope.Frontier[0] = 999
	plan.Commit()
	if got := renderBits(t, pin(t, p)); got != 1 {
		t.Fatal(got)
	}
	if got := renderBits(t, old); got != 1 {
		t.Fatal(got)
	}
}

func TestConcurrentPinnedReadersAcrossSharedGenerations(t *testing.T) {
	p := testPublisher(t, t.TempDir(), 1, nil)
	r := testRequest(t, 1, "a", 0x8000000000000000)
	commitRequest(t, p, r)
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				h, err := p.Pin(pr.NewBudget(1<<30), 1<<20)
				if err != nil {
					t.Error(err)
					return
				}
				series, err := h.Render(context.Background(), "test.metric", 99, 101, 110)
				if err != nil || series == nil || math.Float64bits(series.Values[0]) != 0x8000000000000000 {
					t.Errorf("render changed: %v %v", series, err)
					h.Close()
					return
				}
				h.Close()
			}
		}()
	}
	for g := uint64(2); g <= 100; g++ {
		r.Envelope.Generation = g
		r.Envelope.Frontier = []uint64{g}
		r.Added = nil
		commitRequest(t, p, r)
	}
	close(stop)
	wg.Wait()
}
