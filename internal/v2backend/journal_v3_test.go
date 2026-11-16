package v2backend

import (
	"context"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/points"
)

func TestJournalCodecRejectsBeforeMutation(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "must-not-exist")
	opts := testOptions(dir, simplePolicy())
	opts.JournalCodec = "pdc3-dod"
	if _, err := Open(opts); err == nil {
		t.Fatal("unknown codec accepted")
	}
	if _, err := os.Stat(dir); !os.IsNotExist(err) {
		t.Fatal("invalid codec mutated store", err)
	}
}
func TestPredictorBackendSIGKILL(t *testing.T) {
	const clock = int64(1794700800)
	if dir := os.Getenv("V2_PDC3_CRASH_DIR"); dir != "" {
		opts := testOptions(dir, simplePolicy())
		opts.JournalCodec = "pdc3"
		s, e := openAt(opts, func() time.Time { return time.Unix(clock, 0) })
		if e != nil {
			panic(e)
		}
		for i := 0; i < 200; i++ {
			value := math.Float64frombits(0x3ff0000000000000 + uint64(i))
			if i == 0 {
				value = math.Copysign(0, -1)
			}
			if i == 1 {
				value = math.Float64frombits(0x7ff8000000000042)
			}
			if e = s.Append(&points.Points{Metric: "crash.\xff.metric", Data: []points.Point{{Timestamp: clock - 300 + int64(i), Value: value}}}); e != nil {
				panic(e)
			}
			if i == 99 {
				if e = s.Flush(); e != nil {
					panic(e)
				}
			}
		}
		if e = s.log.Sync(); e != nil {
			panic(e)
		}
		if e = os.WriteFile(filepath.Join(dir, "ready"), nil, 0600); e != nil {
			panic(e)
		}
		select {}
	}
	dir := t.TempDir()
	cmd := exec.Command(os.Args[0], "-test.run=^TestPredictorBackendSIGKILL$")
	cmd.Env = append(os.Environ(), "V2_PDC3_CRASH_DIR="+dir)
	if e := cmd.Start(); e != nil {
		t.Fatal(e)
	}
	defer func() { _ = cmd.Process.Kill(); _ = cmd.Wait() }()
	deadline := time.Now().Add(15 * time.Second)
	for {
		if _, e := os.Stat(filepath.Join(dir, "ready")); e == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("helper not ready")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if e := cmd.Process.Kill(); e != nil {
		t.Fatal(e)
	}
	_ = cmd.Wait()
	// Reopen with the legacy writer; decoding dispatches by each receipt's magic.
	opts := testOptions(dir, simplePolicy())
	for epoch := 0; epoch < 2; epoch++ {
		s, e := openAt(opts, func() time.Time { return time.Unix(clock, 0) })
		if e != nil {
			t.Fatal(e)
		}
		if s.Stats().IngestedPoints != uint64(200+epoch) {
			t.Fatal("replay count")
		}
		got, e := s.Fetch(context.Background(), "crash.\xff.metric", int(clock-301), int(clock))
		if e != nil {
			t.Fatal(e)
		}
		for i := 0; i < 200; i++ {
			want := 0x3ff0000000000000 + uint64(i)
			if i == 0 {
				want = 0x8000000000000000
			}
			if i == 1 {
				want = 0x7ff8000000000042
			}
			if math.Float64bits(got.Values[i]) != want {
				t.Fatalf("epoch%d slot%d", epoch, i)
			}
		}
		got.Release()
		if epoch == 0 {
			if e = s.Append(&points.Points{Metric: "second.metric", Data: []points.Point{{Timestamp: clock - 1, Value: 9}}}); e != nil {
				t.Fatal(e)
			}
		}
		requireClose(t, s)
	}
}
