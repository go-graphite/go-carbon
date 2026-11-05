//go:build unix

package tcp

import (
	"fmt"
	"testing"

	"github.com/go-graphite/go-carbon/receiver/stream/codec"
)

// Two cycles: definitions, then reusable steady frames. Every occurrence has
// identical value/time, so after the definition cycle the predictors are stable.
func liveStreamLoadFrames(t testing.TB, worker, metrics int, name func(int) string) [][]byte {
	t.Helper()
	encoder, err := codec.NewEncoder(codec.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer encoder.Close()
	var frames [][]byte
	for range 2 {
		for batch := range 128 {
			for j := range 128 {
				i := int((uint64(worker*16384+batch*128+j) * 6364136223846793005 >> 32) % uint64(metrics))
				if err := encoder.Add([]byte(name(i)), 1, 1791000000); err != nil {
					t.Fatal(err)
				}
			}
			wire, err := encoder.Flush(nil)
			if err != nil {
				t.Fatal(err)
			}
			frames = append(frames, wire)
		}
	}
	return frames
}

func TestLiveStreamLoadFramesRepeatExactly(t *testing.T) {
	name := func(i int) string { return fmt.Sprintf("stream.fixture.%04d", i) }
	frames := liveStreamLoadFrames(t, 0, 1024, name)
	decoder, err := codec.NewDecoder(codec.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer decoder.Close()
	first, repeated := map[string]int{}, map[string]int{}
	for cycle := range 3 {
		for batch := range 128 {
			frame := batch
			if cycle > 0 {
				frame += 128
			}
			if _, err := decoder.Decode(frames[frame], func(_ uint32, metric []byte, value float64, ts int64, _ *any) {
				if value != 1 || ts != 1791000000 {
					t.Errorf("predictors changed across repeated frame: %s %g %d", metric, value, ts)
				}
				if cycle == 0 {
					first[string(metric)]++
				} else {
					repeated[string(metric)]++
				}
			}); err != nil {
				t.Fatal(err)
			}
		}
	}
	if len(first) != len(repeated) {
		t.Fatal("stream frame name sets differ")
	}
	for metric, count := range first {
		if repeated[metric] != 2*count {
			t.Fatal("stream frame point conservation", metric, count, repeated[metric])
		}
	}
}
