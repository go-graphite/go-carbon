package chunkstore

import (
	"bytes"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	whisper "github.com/go-graphite/go-whisper"
)

type continuationFetchImage struct {
	age, from, until, step int
	values                 []float64
}

type continuationImage struct {
	header  []byte
	fetches []continuationFetchImage
}

type continuationFixtureKey struct {
	retentions             string
	seedCount, blockPoints int
	integers               bool
	method                 whisper.AggregationMethod
	xff                    float32
}

type continuationFixture struct {
	once          sync.Once
	ready         chan struct{}
	err           error
	main, sidecar []byte
	images        []continuationImage
}

var continuationFixtures sync.Map

// The physical, external-byte and external-typed transports use the same
// small reference trace. Build its physical files and exact expected images
// once; each transport still gets its own mutable resumed file and clock.
func prepareContinuationFixture(t *testing.T, key continuationFixtureKey) *continuationFixture {
	t.Helper()
	entry, _ := continuationFixtures.LoadOrStore(key, &continuationFixture{})
	fixture := entry.(*continuationFixture)
	fixture.once.Do(func() {
		root := t.TempDir()
		fixture.ready = make(chan struct{})
		// Build while the child waits for its parallel slot. Other readers
		// of this immutable trace wait on ready, without rebuilding it.
		go func() {
			defer close(fixture.ready)
			fixture.err = buildContinuationFixture(fixture, key, root)
		}()
	})
	return fixture
}

func exerciseSharedCWhisperContinuation(t *testing.T, external, typed bool, seedCount int, integers bool, fixture *continuationFixture) {
	t.Helper()
	<-fixture.ready
	if fixture.err != nil {
		t.Fatal(fixture.err)
	}
	if len(fixture.images) != 140 || len(fixture.main) == 0 {
		t.Fatal("incomplete continuation reference fixture")
	}
	const base = int64(1792432631)
	now := time.Unix(base+2, 0)
	clock := func() time.Time { return now }
	resumed := filepath.Join(t.TempDir(), "resumed.wsp")
	if err := os.WriteFile(resumed, fixture.main, 0600); err != nil {
		t.Fatal(err)
	}
	if fixture.sidecar != nil {
		if err := os.WriteFile(whisper.OutOfOrderSidecarPath(resumed), fixture.sidecar, 0600); err != nil {
			t.Fatal(err)
		}
	}
	var fs whisper.StorageFileSystem
	if external {
		fs = externalContinuationFiles(t, resumed, typed)
	}
	for seq := 3; seq < len(fixture.images); seq++ {
		stamp := base + int64(3*(seq+1))
		now = time.Unix(stamp+2, 0)
		if seq > 3 {
			if err := updateContinuationWhisper(resumed, clock, fs, seq, stamp, seedCount, integers); err != nil {
				t.Fatal(err)
			}
		}
		now = time.Unix(stamp+3, 0)
		compareContinuationImage(t, fixture.images[seq], resumed, int(now.Unix()), seq, clock, fs)
		if external {
			if _, err := os.Stat(resumed); !os.IsNotExist(err) {
				t.Fatal("external state machine recreated a physical metric file")
			}
			if _, err := os.Stat(whisper.OutOfOrderSidecarPath(resumed)); !os.IsNotExist(err) {
				t.Fatal("external state machine recreated a physical sidecar")
			}
		}
	}
}

func buildContinuationFixture(fixture *continuationFixture, key continuationFixtureKey, root string) error {
	const base = int64(1792432631)
	now := time.Unix(base+2, 0)
	clock := func() time.Time { return now }
	source, resumed := filepath.Join(root, "source.wsp"), filepath.Join(root, "resumed.wsp")
	if err := createContinuationSource(source, clock, key.retentions, key.seedCount, key.blockPoints, key.integers, key.method, key.xff, base); err != nil {
		return err
	}
	images := make([]continuationImage, 140)
	for seq := range images {
		stamp := base + int64(3*(seq+1))
		now = time.Unix(stamp+2, 0)
		if err := updateContinuationWhisper(source, clock, nil, seq, stamp, key.seedCount, key.integers); err != nil {
			return err
		}
		if seq == 3 {
			if err := transcodeContinuationForTest(source, resumed, clock); err != nil {
				return err
			}
			var err error
			fixture.main, err = os.ReadFile(resumed)
			if err != nil {
				return err
			}
			fixture.sidecar, err = os.ReadFile(whisper.OutOfOrderSidecarPath(resumed))
			if err != nil && !os.IsNotExist(err) {
				return err
			}
		}
		if seq >= 3 {
			now = time.Unix(stamp+3, 0)
			w, err := whisper.OpenWithOptions(source, &whisper.Options{OutOfOrder: true, FileSystem: continuationPhysicalReadFS, Now: clock})
			if err != nil {
				return err
			}
			images[seq], err = captureContinuationImage(w, int(now.Unix()))
			if err != nil {
				_ = w.Close()
				return err
			}
			if err := w.Close(); err != nil {
				return err
			}
		}
	}
	fixture.images = images
	return nil
}

func captureContinuationImage(w *whisper.Whisper, now int) (continuationImage, error) {
	layout, err := w.ContinuationLayout()
	if err != nil {
		return continuationImage{}, err
	}
	image := continuationImage{header: bytes.Clone(layout.Header)}
	for _, age := range continuationFetchAges(w) {
		want, err := w.Fetch(now-age, now-1)
		if err != nil {
			return continuationImage{}, err
		}
		if want == nil {
			return continuationImage{}, fmt.Errorf("nil reference fetch at age%d", age)
		}
		image.fetches = append(image.fetches, continuationFetchImage{age, want.FromTime(), want.UntilTime(), want.Step(), append([]float64(nil), want.Values()...)})
	}
	return image, nil
}

func compareContinuationImage(t *testing.T, image continuationImage, resumed string, now, seq int, clock func() time.Time, fs whisper.StorageFileSystem) {
	t.Helper()
	right := openContinuationWhisperForRead(t, resumed, clock, fs)
	defer right.Close()
	layout, err := right.ContinuationLayout()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(image.header, layout.Header) {
		t.Fatalf("writer continuation metadata changed at round%d", seq)
	}
	for _, want := range image.fetches {
		got, err := right.Fetch(now-want.age, now-1)
		if err != nil {
			t.Fatal(err)
		}
		compareContinuationFetch(t, want, got, seq)
	}
}

func continuationFetchAges(w *whisper.Whisper) []int {
	ages := []int{8, 20, 100, 240, 600}
	if retentions := w.Retentions(); retentions[0].MaxRetention() > 600 {
		for _, r := range retentions[:len(retentions)-1] {
			ages = append(ages, r.MaxRetention()+60)
		}
	}
	return ages
}

func compareContinuationFetch(t *testing.T, want continuationFetchImage, got *whisper.TimeSeries, seq int) {
	t.Helper()
	if got == nil || want.from != got.FromTime() || want.until != got.UntilTime() || want.step != got.Step() || len(want.values) != len(got.Values()) {
		t.Fatalf("continuation fetch shape changed at round%d age%d", seq, want.age)
	}
	for i, value := range want.values {
		actual := got.Values()[i]
		if math.IsNaN(value) && math.IsNaN(actual) {
			continue
		}
		if math.Float64bits(value) != math.Float64bits(actual) {
			t.Fatalf("continuation changed at round%d age%d timestamp%d: %g != %g", seq, want.age, want.from+i*want.step, value, actual)
		}
	}
}
