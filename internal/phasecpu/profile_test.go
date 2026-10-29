package phasecpu

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"runtime/pprof"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/protobuf/encoding/protowire"
)

func msg(n protowire.Number, b []byte) []byte {
	return protowire.AppendBytes(protowire.AppendTag(nil, n, protowire.BytesType), b)
}
func num(n protowire.Number, v uint64) []byte {
	return protowire.AppendVarint(protowire.AppendTag(nil, n, protowire.VarintType), v)
}
func zipped(b []byte) []byte {
	var buf bytes.Buffer
	w := gzip.NewWriter(&buf)
	_, _ = w.Write(b)
	_ = w.Close()
	return buf.Bytes()
}
func fixture(labels ...[2]string) []byte {
	table := []string{"", "samples", "count", "cpu", "nanoseconds"}
	label := func(key, value string) []byte {
		k, v := len(table), len(table)+1
		table = append(table, key, value)
		return msg(3, append(num(1, uint64(k)), num(2, uint64(v))...))
	}
	var sample []byte
	sample = append(sample, msg(2, append(protowire.AppendVarint(nil, 7), protowire.AppendVarint(nil, 70000000)...))...)
	for _, l := range labels {
		sample = append(sample, label(l[0], l[1])...)
	}
	var raw []byte
	raw = append(raw, msg(1, append(num(1, 1), num(2, 2)...))...)
	raw = append(raw, msg(1, append(num(1, 3), num(2, 4)...))...)
	raw = append(raw, msg(2, sample)...)
	for _, s := range table {
		raw = append(raw, msg(6, []byte(s))...)
	}
	return zipped(raw)
}
func TestDecodePhasePrecedenceAndUnknown(t *testing.T) {
	for _, tt := range []struct {
		labels [][2]string
		want   Phase
	}{
		{nil, Other},
		{[][2]string{{"go_carbon_phase", "cache_insert"}}, CacheInsert},
		{[][2]string{{"go_carbon_phase", "dynamic_metric_name"}}, Other},
		{[][2]string{{"go_carbon_phase", "receiver_tcp"}, {"persister_stage", "route"}}, Persister},
		{[][2]string{{"go_carbon_phase", "persister"}, {"persister_stage", "commit"}, {"pebble", "flush"}}, PebbleFlush},
		{[][2]string{{"pebble", "compact"}}, PebbleCompaction},
		{[][2]string{{"pebble", "wal-sync"}}, PebbleWAL},
	} {
		got, err := decode(fixture(tt.labels...))
		if err != nil {
			t.Fatal(err)
		}
		for p, v := range got {
			want := uint64(0)
			if Phase(p) == tt.want {
				want = 70000000
			}
			if v != want {
				t.Fatalf("labels %v phase %v: got %d want %d", tt.labels, Phase(p), v, want)
			}
		}
	}
}
func TestDecodeRejectsMalformed(t *testing.T) {
	for _, b := range [][]byte{nil, []byte("invalid"), zipped([]byte{0x12, 0xff}), zipped(nil)} {
		if _, err := decode(b); err == nil {
			t.Fatal("accepted malformed profile")
		}
	}
}
func TestLabelsInherited(t *testing.T) {
	Set(ReceiverTCP)
	defer Set(Other)
	ready, done := make(chan struct{}), make(chan struct{})
	go func() { close(ready); <-done }()
	<-ready
	var buf bytes.Buffer
	_ = pprof.Lookup("goroutine").WriteTo(&buf, 1)
	close(done)
	if !strings.Contains(buf.String(), `"go_carbon_phase":"receiver_tcp"`) {
		t.Fatal("goroutine label missing")
	}
}

func TestDutyBudget(t *testing.T) {
	for _, tt := range []struct {
		interval, window time.Duration
		valid            bool
	}{
		{time.Minute, time.Second, true}, {50 * time.Second, time.Second, false},
		{time.Minute, 1200 * time.Millisecond, false}, {2 * time.Minute, 2 * time.Second, true},
		{30 * time.Second, 500 * time.Millisecond, true}, {time.Minute, 0, false}, {0, time.Second, false},
	} {
		if err := Validate(tt.interval, tt.window); (err == nil) != tt.valid {
			t.Fatalf("interval %s window %s: %v", tt.interval, tt.window, err)
		}
	}
}

func TestProfilerLifecycleAndConflict(t *testing.T) {
	if _, err := Start(time.Second); err == nil {
		t.Fatal("accepted short interval")
	}
	p, err := Start(MinInterval)
	if err != nil {
		t.Fatal(err)
	}
	reg := prometheus.NewPedanticRegistry()
	reg.MustRegister(p)
	if _, err = reg.Gather(); err != nil {
		t.Fatal(err)
	}
	if _, err = Start(MinInterval); err == nil {
		t.Fatal("accepted duplicate profiler")
	}
	p.Stop()
	p.Stop()
	next, err := Start(MinInterval)
	if err != nil {
		t.Fatal(err)
	}
	next.Stop()
	var buf bytes.Buffer
	if err = pprof.StartCPUProfile(&buf); err != nil {
		t.Fatal(err)
	}
	p.capture(time.Second, time.Millisecond)
	if p.failures.Load() != 1 {
		t.Fatal("conflicting profile not counted")
	}
	pprof.StopCPUProfile()
	// The conflict did not stop the owner's CPU profiler.
	if _, err = decode(buf.Bytes()); err != nil {
		t.Fatal(err)
	}
}
func TestCaptureRealLabels(t *testing.T) {
	p := &Profiler{stop: make(chan struct{})}
	var wg sync.WaitGroup
	for _, phase := range []Phase{ReceiverTCP, CacheInsert} {
		wg.Go(func() {
			pprof.Do(context.Background(), pprof.Labels("go_carbon_phase", phase.String()), func(context.Context) {
				b := make([]byte, 256<<10)
				for range 2048 {
					sum := sha256.Sum256(b)
					copy(b, sum[:])
				}
			})
		})
	}
	p.capture(time.Second, time.Second)
	wg.Wait()
	if p.windows.Load() != 1 {
		t.Fatalf("capture failed: %d", p.failures.Load())
	}
	for _, phase := range []Phase{ReceiverTCP, CacheInsert} {
		if p.sampled[phase].Load() == 0 {
			t.Fatalf("no samples for %s", phase)
		}
	}
}
func TestDecodeMetadataCacheFrames(t *testing.T) {
	for _, tt := range []struct {
		frame string
		want  Phase
	}{
		{"github.com/go-graphite/go-carbon/internal/chunkstore.(*stateCache).locked", MetadataCache},
		{"github.com/go-graphite/go-carbon/internal/chunkstore.decodeStateRecord", MetadataCache},
		{"github.com/go-graphite/go-carbon/cache.(*Cache).AddPoint", CacheInsert},
		{"github.com/go-graphite/go-carbon/cache.(*Cache).add", CacheInsert},
		{"github.com/go-graphite/go-carbon/cache.(*WriteoutQueue).get", WriteoutQueue},
		{"github.com/go-graphite/go-carbon/cache.(*Cache).makeQueueAt", WriteoutQueue},
		{"unrelated.decodeStateRecord", Persister},
	} {
		frame := tt.frame
		var raw []byte
		raw = append(raw, msg(1, append(num(1, 1), num(2, 2)...))...)
		sample := append(num(1, 1), num(2, 20000000)...)
		sample = append(sample, msg(3, append(num(1, 4), num(2, 5)...))...)
		raw = append(raw, msg(2, sample)...)
		raw = append(raw, msg(5, append(num(1, 1), num(2, 3)...))...)
		raw = append(raw, msg(4, append(num(1, 1), msg(4, num(1, 1))...))...)
		for _, s := range []string{"", "cpu", "nanoseconds", frame, "go_carbon_phase", "persister"} {
			raw = append(raw, msg(6, []byte(s))...)
		}
		got, err := decode(zipped(raw))
		if err != nil {
			t.Fatal(err)
		}
		want := tt.want
		if got[want] != 20000000 {
			t.Fatalf("frame %s: %v", frame, got)
		}
	}
}
