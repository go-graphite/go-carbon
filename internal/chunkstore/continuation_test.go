package chunkstore

import (
	"bytes"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"testing"
	"time"

	whisper "github.com/go-graphite/go-whisper"
)

// This is a continuation-format proof, not a production backend or a performance
// qualification. Preserve source buffers/watermarks while transcoding physical
// block points through the existing shared exact integer/float codec. The resumed
// writer then runs the unchanged CWhisper state machine, with originals absent.
func TestExactPhysicalBlocksPreserveCWhisperContinuation(t *testing.T) {
	t.Parallel()
	exerciseCWhisperContinuation(t, false, false)
}

func TestExternalFilesPreserveCWhisperContinuation(t *testing.T) {
	t.Parallel()
	exerciseCWhisperContinuation(t, true, false)
}

func TestTypedExactBlocksPreserveCWhisperContinuation(t *testing.T) {
	t.Parallel()
	exerciseCWhisperContinuation(t, true, true)
}

func TestTypedExactLargeCWhisperContinuation(t *testing.T) {
	testTypedExactLargeCWhisperContinuation(t, 0)
}

func TestTypedExactLargeCWhisperContinuationGroup1(t *testing.T) {
	testTypedExactLargeCWhisperContinuation(t, 1)
}

func TestTypedExactLargeCWhisperContinuationGroup2(t *testing.T) {
	testTypedExactLargeCWhisperContinuation(t, 2)
}

func testTypedExactLargeCWhisperContinuation(t *testing.T, group int) {
	t.Parallel()
	methods := []whisper.AggregationMethod{whisper.Average, whisper.Sum, whisper.First, whisper.Last, whisper.Min, whisper.Max}
	for _, integers := range []bool{false, true} {
		t.Run(fmt.Sprintf("integers=%t", integers), func(t *testing.T) {
			t.Parallel()
			exerciseCWhisperContinuationMethods(t, true, true, "1s:2d,60s:28d,3600s:2y", 1000, 7200, integers, methods[group*2:group*2+2])
		})
	}
}

func exerciseCWhisperContinuation(t *testing.T, external, typed bool) {
	exerciseCWhisperContinuationConfig(t, external, typed, "1s:20s,5s:2m,30s:10m", 18, 16, false)
}

func exerciseCWhisperContinuationConfig(t *testing.T, external, typed bool, retentionDefs string, seedCount, blockPoints int, integers bool) {
	exerciseCWhisperContinuationMethods(t, external, typed, retentionDefs, seedCount, blockPoints, integers,
		[]whisper.AggregationMethod{whisper.Average, whisper.Sum, whisper.First, whisper.Last, whisper.Min, whisper.Max})
}

func exerciseCWhisperContinuationMethods(t *testing.T, external, typed bool, retentionDefs string, seedCount, blockPoints int, integers bool, methods []whisper.AggregationMethod) {
	for _, method := range methods {
		for _, xff := range []float32{0, .5, 1} {
			t.Run(fmt.Sprintf("%s/xff%g", method, xff), func(t *testing.T) {
				var fixture *continuationFixture
				if seedCount <= 128 {
					fixture = prepareContinuationFixture(t, continuationFixtureKey{retentionDefs, seedCount, blockPoints, integers, method, xff})
				}
				t.Parallel()
				if fixture != nil {
					exerciseSharedCWhisperContinuation(t, external, typed, seedCount, integers, fixture)
					return
				}
				const base = int64(1792432631)
				now := time.Unix(base+2, 0)
				clock := func() time.Time { return now }
				root := t.TempDir()
				source, resumed := filepath.Join(root, "source.wsp"), filepath.Join(root, "resumed.wsp")
				if err := createContinuationSource(source, clock, retentionDefs, seedCount, blockPoints, integers, method, xff, base); err != nil {
					t.Fatal(err)
				}
				var paths = []string{source}
				var externalFS whisper.StorageFileSystem
				for seq := 0; seq < 140; seq++ {
					stamp := base + int64(3*(seq+1))
					now = time.Unix(stamp+2, 0)
					for _, path := range paths {
						var fs whisper.StorageFileSystem
						if path == resumed {
							fs = externalFS
						}
						if err := updateContinuationWhisper(path, clock, fs, seq, stamp, seedCount, integers); err != nil {
							t.Fatal(err)
						}
					}
					// Capture after OOO already exists; this must preserve its raw
					// precedence and pending buffers rather than merging a snapshot.
					if seq == 3 {
						if err := transcodeContinuationForTest(source, resumed, clock); err != nil {
							t.Fatal(err)
						}
						if external {
							externalFS = externalContinuationFiles(t, resumed, typed)
						}
						paths = append(paths, resumed)
					}
					if len(paths) == 2 {
						now = time.Unix(stamp+3, 0)
						compareContinuationForTest(t, source, resumed, int(now.Unix()), seq, clock, externalFS)
						if external {
							if _, e := os.Stat(resumed); !os.IsNotExist(e) {
								t.Fatal("external state machine recreated a physical metric file")
							}
							if _, e := os.Stat(whisper.OutOfOrderSidecarPath(resumed)); !os.IsNotExist(e) {
								t.Fatal("external state machine recreated a physical sidecar")
							}
						}
					}
				}
			})
		}
	}
}

func openContinuationWhisper(t *testing.T, path string, clock func() time.Time, filesystems ...whisper.StorageFileSystem) *whisper.Whisper {
	t.Helper()
	var fs whisper.StorageFileSystem
	if len(filesystems) != 0 {
		fs = filesystems[0]
	}
	w, err := whisper.OpenWithOptions(path, &whisper.Options{OutOfOrder: true, FileSystem: fs, Now: clock})
	if err != nil {
		t.Fatal(err)
	}
	return w
}

func transcodeContinuationForTest(source, destination string, clock func() time.Time) error {
	w, err := whisper.OpenWithOptions(source, &whisper.Options{OutOfOrder: true, FileSystem: continuationPhysicalReadFS, Now: clock})
	if err != nil {
		return err
	}
	defer w.Close()
	layout, err := w.ContinuationLayout()
	if err != nil {
		return err
	}
	out, err := os.OpenFile(destination, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0600)
	if err != nil {
		return err
	}
	defer out.Close()
	if err := out.Truncate(int64(layout.Size)); err != nil {
		return err
	}
	if _, err := out.WriteAt(layout.Header, 0); err != nil {
		return err
	}
	for _, block := range layout.Blocks {
		points, err := w.ContinuationBlockPoints(block)
		if err != nil {
			return err
		}
		var decoded []whisper.TimeSeriesPoint
		for first := 0; first < len(points); first += chunkSlots {
			last := min(first+chunkSlots, len(points))
			var input chunk
			for i, p := range points[first:last] {
				input.set(i, Point{Timestamp: int64(p.Time), Value: p.Value})
			}
			encoded := encodeExactChunk(&input)
			var actual chunk
			if err := decodeChunk(encoded, &actual); err != nil {
				return err
			}
			for i := range last - first {
				if !actual.has(i) || actual.Points[i].Timestamp != input.Points[i].Timestamp ||
					math.Float64bits(actual.Points[i].Value) != math.Float64bits(input.Points[i].Value) {
					return fmt.Errorf("exact physical point changed")
				}
				p := actual.Points[i]
				decoded = append(decoded, whisper.TimeSeriesPoint{Time: int(p.Timestamp), Value: p.Value})
			}
		}
		data, err := whisper.EncodeContinuationBlock(block, decoded)
		if err != nil {
			return err
		}
		if _, err := out.WriteAt(data, int64(block.Offset)); err != nil {
			return err
		}
	}
	if sidecar := w.OutOfOrderPath(); sidecar != "" {
		// Only a small test fixture: the future backend must transcode these
		// sparse classic slots as well, rather than retain a raw sidecar copy.
		data, err := os.ReadFile(sidecar)
		if err != nil {
			return err
		}
		if err := os.WriteFile(whisper.OutOfOrderSidecarPath(destination), data, 0600); err != nil {
			return err
		}
	}
	return nil
}

func compareContinuationForTest(t *testing.T, source, resumed string, now, seq int, clock func() time.Time, fs whisper.StorageFileSystem) {
	t.Helper()
	left, right := openContinuationWhisperForRead(t, source, clock, nil), openContinuationWhisperForRead(t, resumed, clock, fs)
	defer left.Close()
	defer right.Close()
	ll, err := left.ContinuationLayout()
	if err != nil {
		t.Fatal(err)
	}
	rl, err := right.ContinuationLayout()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(ll.Header, rl.Header) {
		t.Fatalf("writer continuation metadata changed at round%d", seq)
	}
	for _, age := range continuationFetchAges(left) {
		want, err := left.Fetch(now-age, now-1)
		if err != nil {
			t.Fatal(err)
		}
		got, err := right.Fetch(now-age, now-1)
		if err != nil {
			t.Fatal(err)
		}
		if want == nil {
			t.Fatalf("nil reference fetch at age%d", age)
		}
		compareContinuationFetch(t, continuationFetchImage{age, want.FromTime(), want.UntilTime(), want.Step(), want.Values()}, got, seq)
	}
}

func createContinuationSource(source string, clock func() time.Time, retentionDefs string, seedCount, blockPoints int, integers bool, method whisper.AggregationMethod, xff float32, base int64) error {
	w, err := whisper.CreateWithOptions(source, whisper.MustParseRetentionDefs(retentionDefs), method, xff,
		&whisper.Options{Compressed: true, OutOfOrder: true, PointsPerBlock: blockPoints, Now: clock})
	if err != nil {
		return err
	}
	var seed []*whisper.TimeSeriesPoint
	for age := seedCount; age > 0; age-- {
		value := float64(age) + .1
		if integers {
			value = float64(age)
		}
		seed = append(seed, &whisper.TimeSeriesPoint{Time: int(base) - age, Value: value})
	}
	if err := w.UpdateMany(seed); err != nil {
		return err
	}
	if err := w.Close(); err != nil {
		return err
	}
	return nil
}

func updateContinuationWhisper(path string, clock func() time.Time, fs whisper.StorageFileSystem, seq int, stamp int64, seedCount int, integers bool) error {
	w, err := whisper.OpenWithOptions(path, &whisper.Options{OutOfOrder: true, FileSystem: fs, Now: clock})
	if err != nil {
		return err
	}
	var input []*whisper.TimeSeriesPoint
	for _, age := range []int64{3, 2, 1} {
		value := float64(seq) + float64(age)/4 + .1
		if integers {
			value = float64(seq)*4 + float64(age)
		}
		input = append(input, &whisper.TimeSeriesPoint{Time: int(stamp - age), Value: value})
	}
	if seq >= 3 {
		lateAge := int64(17)
		if seedCount > 128 {
			lateAge = int64(seedCount / 2)
		}
		a, b := 111.25+float64(seq), 222.5+float64(seq)
		if integers {
			a, b = 111+float64(seq), 222+float64(seq)
		}
		input = append(input, &whisper.TimeSeriesPoint{Time: int(stamp - lateAge), Value: a}, &whisper.TimeSeriesPoint{Time: int(stamp - lateAge), Value: b})
	}
	if err := w.UpdateMany(input); err != nil {
		return err
	}
	if seq%19 == 18 {
		if err := w.MergeOutOfOrder(); err != nil {
			return err
		}
	}
	if err := w.Close(); err != nil {
		return err
	}
	return nil
}

func externalContinuationFiles(t *testing.T, resumed string, typed bool) whisper.StorageFileSystem {
	t.Helper()
	virtual := &continuationMemoryFiles{files: make(map[string]*continuationMemoryData), typed: typed}
	for _, name := range []string{resumed, whisper.OutOfOrderSidecarPath(resumed)} {
		data, e := os.ReadFile(name)
		if os.IsNotExist(e) {
			continue
		}
		if e != nil {
			t.Fatal(e)
		}
		virtual.files[name] = continuationMemoryImage(data)
		if e := os.Remove(name); e != nil {
			t.Fatal(e)
		}
	}
	return virtual
}

func openContinuationWhisperForRead(t *testing.T, path string, clock func() time.Time, fs whisper.StorageFileSystem) *whisper.Whisper {
	t.Helper()
	if fs == nil {
		fs = continuationPhysicalReadFS
	}
	return openContinuationWhisper(t, path, clock, fs)
}
