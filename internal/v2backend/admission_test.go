package v2backend

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/internal/daemonstore"
	"github.com/go-graphite/go-carbon/points"
	whisper "github.com/go-graphite/go-whisper"
)

func TestAdmissionBeforeRecovery(t *testing.T) { testAdmissionBeforeRecovery(t, "standalone", "pdx1") }
func TestCoreCatalogAdmissionBeforeRecovery(t *testing.T) {
	testAdmissionBeforeRecovery(t, "core", "pdc4")
}
func testAdmissionBeforeRecovery(t *testing.T, engine, codec string) {
	dir := t.TempDir()
	opts := testOptions(dir, simplePolicy())
	opts.WriteEngine, opts.JournalCodec = engine, codec
	if engine == "core" {
		opts.Owners, opts.Shards = 4, 2
	}
	gate := make(chan struct{})
	entered := make(chan struct{}, 1)
	// An existing metric forces policy validation during historical recovery.
	base, e := openAt(opts, time.Now)
	if e != nil {
		t.Fatal(e)
	}
	stamp := time.Now().Unix() - 10
	if e = base.Append(&points.Points{Metric: "old.value", Data: []points.Point{{Timestamp: stamp, Value: 1}}}); e != nil {
		t.Fatal(e)
	}
	requireClose(t, base)
	opts.Policy = func(name string) (daemonstore.Metric, error) {
		if name == "old.value" {
			select {
			case entered <- struct{}{}:
			default:
			}
			<-gate
		}
		return simplePolicy(), nil
	}
	s, e := openAdmissionAt(opts, time.Now, true)
	if e != nil {
		t.Fatal(e)
	}
	<-entered
	if _, e = s.Fetch(context.Background(), "old.value", int(stamp-1), int(stamp+1)); !errors.Is(e, ErrRecovering) {
		t.Fatalf("recovery read: %v", e)
	}
	// Only recovery is blocked; resolve a new policy through the original closure.
	// Release policy calls independently to prove admission is journal-backed.
	done := make(chan error, 1)
	go func() {
		done <- s.Append(&points.Points{Metric: "live.value", Data: []points.Point{{Timestamp: stamp, Value: math.Copysign(0, -1)}}})
	}()
	select {
	case e := <-done:
		if e != nil {
			t.Fatal(e)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("append waited for recovery")
	}
	if s.FirstAcceptedUnixNano() == 0 {
		t.Fatal("first acceptance not synced")
	}
	close(gate)
	if e = s.WaitReady(context.Background()); e != nil {
		t.Fatal(e)
	}
	if s.Stats().IngestedPoints != 2 {
		t.Fatalf("lost/dup points: %d", s.Stats().IngestedPoints)
	}
	requireClose(t, s)
	opts.Policy = func(string) (daemonstore.Metric, error) { return simplePolicy(), nil }
	s, e = openAt(opts, time.Now)
	if e != nil {
		t.Fatal(e)
	}
	if s.Stats().IngestedPoints != 2 {
		t.Fatalf("restart points: %d", s.Stats().IngestedPoints)
	}
	requireClose(t, s)
}

// Each child is killed while replay is in progress, after durable admission.
// A distinct point per trial plus checkpoint counters detects reapply/omission;
// final rendered value bits are compared with an uninterrupted oracle.
func TestCoreCatalogAdmissionCrashRecovery(t *testing.T) {
	t.Setenv("V2_ADMISSION_CRASH_ENGINE", "core")
	t.Setenv("V2_ADMISSION_CRASH_TRIALS", "16")
	TestAdmissionCrashRecovery100(t)
}
func admissionCrashOptions(dir string) daemonstore.Options {
	opts := testOptions(dir, simplePolicy())
	if os.Getenv("V2_ADMISSION_CRASH_ENGINE") == "core" {
		opts.WriteEngine, opts.JournalCodec, opts.Owners, opts.Shards = "core", "pdc4", 4, 2
	}
	return opts
}
func TestAdmissionCrashRecovery100(t *testing.T) {
	trials := 100
	if n := os.Getenv("V2_ADMISSION_CRASH_TRIALS"); n != "" {
		var e error
		trials, e = strconv.Atoi(n)
		if e != nil || trials < 1 || trials > 100 {
			t.Fatal("invalid crash trial count")
		}
	}

	if dir := os.Getenv("V2_ADMISSION_CRASH_CHILD"); dir != "" {
		trial, _ := strconv.Atoi(os.Getenv("V2_ADMISSION_TRIAL"))
		opts := admissionCrashOptions(dir)
		opts.Policy = func(name string) (daemonstore.Metric, error) {
			if name != "crash.value" {
				time.Sleep(2 * time.Millisecond)
			}
			return simplePolicy(), nil
		}
		s, e := openAdmissionAt(opts, func() time.Time { return time.Unix(1794783000, 0) }, true)
		if e != nil {
			t.Fatal(e)
		}
		p := points.Point{Timestamp: 1794783000 - 300 + int64(trial), Value: math.Float64frombits(uint64(trial) + 0x8000000000000000)}
		if e = s.Append(&points.Points{Metric: "crash.value", Data: []points.Point{p}}); e != nil {
			t.Fatal(e)
		}
		if e = s.log.Sync(); e != nil {
			t.Fatal(e)
		}
		if s.readyOK.Load() {
			t.Fatal("child recovered before kill window")
		}
		fmt.Println("ADMITTED")
		select {}
	}
	dir := t.TempDir()
	clock := func() time.Time { return time.Unix(1794783000, 0) }
	seed, e := openAt(admissionCrashOptions(dir), clock)
	if e != nil {
		t.Fatal(e)
	}
	for i := 0; i < 128; i++ {
		if e = seed.Append(&points.Points{Metric: fmt.Sprintf("history.%d", i), Data: []points.Point{{Timestamp: 1794782990, Value: float64(i)}}}); e != nil {
			t.Fatal(e)
		}
	}
	requireClose(t, seed)
	for trial := 0; trial < trials; trial++ {
		c := exec.Command(os.Args[0], "-test.run=^TestAdmissionCrashRecovery100$", "-test.v")
		c.Env = append(os.Environ(), "V2_ADMISSION_CRASH_CHILD="+dir, "V2_ADMISSION_TRIAL="+strconv.Itoa(trial))
		out, e := c.StdoutPipe()
		if e != nil {
			t.Fatal(e)
		}
		c.Stderr = os.Stderr
		if e = c.Start(); e != nil {
			t.Fatal(e)
		}
		// Byte reader avoids scanner prefetch hiding a child exit.
		line := ""
		b := make([]byte, 1)
		for !containsAdmission(line) {
			n, e := out.Read(b)
			if e != nil {
				_ = c.Process.Kill()
				_ = c.Wait()
				t.Fatalf("trial%d: %s %v", trial, line, e)
			}
			line += string(b[:n])
		}
		time.Sleep(time.Duration((trial*7919)%200000) * time.Microsecond)
		_ = c.Process.Kill()
		_ = c.Wait()
		_ = out.Close()
	}
	s, e := openAt(admissionCrashOptions(dir), clock)
	if e != nil {
		t.Fatal(e)
	}
	defer requireClose(t, s)
	if n := s.Stats().IngestedPoints; n != uint64(128+trials) {
		t.Fatalf("lost/dup: got%d want%d", n, 128+trials)
	}
	// Every sample remains in the physical one-second ring; verify exact bits.
	found := false
	for _, o := range s.owners {
		found = found || o.metrics["crash.value"] != nil
	}
	if !found {
		t.Fatal("missing metric")
	}
	got, e := s.Fetch(context.Background(), "crash.value", 1794782640, 1794783000)
	if e != nil {
		t.Fatal(e)
	}
	defer got.Release()
	seen := map[uint64]int{}
	for _, v := range got.Values {
		if !math.IsNaN(v) {
			seen[math.Float64bits(v)]++
		}
	}
	for trial := 0; trial < trials; trial++ {
		bits := uint64(trial) + 0x8000000000000000
		if seen[bits] != 1 {
			t.Fatalf("bits%x count%d", bits, seen[bits])
		}
	}
	oracle, e := whisper.CreateWithOptions(filepath.Join(t.TempDir(), "oracle.wsp"), whisper.MustParseRetentionDefs("1s:1h"), whisper.Average, .5, &whisper.Options{Sparse: true, Now: clock})
	if e != nil {
		t.Fatal(e)
	}
	defer oracle.Close()
	for trial := 0; trial < trials; trial++ {
		if e = oracle.UpdateMany([]*whisper.TimeSeriesPoint{{Time: 1794782700 + trial, Value: math.Float64frombits(uint64(trial) + 0x8000000000000000)}}); e != nil {
			t.Fatal(e)
		}
	}
	want, e := oracle.Fetch(1794782640, 1794783000)
	if e != nil {
		t.Fatal(e)
	}
	if len(got.Values) != len(want.Values()) {
		t.Fatal("oracle grid")
	}
	for i, v := range want.Values() {
		if math.Float64bits(v) != math.Float64bits(got.Values[i]) {
			t.Fatalf("oracle slot%d got%x want%x", i, math.Float64bits(got.Values[i]), math.Float64bits(v))
		}
	}
	t.Logf("%d SIGKILL recoveries: lost=0 dup=0 classic-exact=true root=%s", trials, filepath.Base(dir))
}
func containsAdmission(s string) bool {
	for i := 0; i+9 <= len(s); i++ {
		if s[i:i+9] == "ADMITTED\n" {
			return true
		}
	}
	return false
}

// One producer may publish live data while another producer sharing its physical
// database still consumes history. Both use the same durable journal boundary.
func TestCoreCatalogAdmissionPartialActivation(t *testing.T) {
	opts := testOptions(t.TempDir(), simplePolicy())
	opts.WriteEngine, opts.JournalCodec, opts.Owners, opts.Shards = "core", "pdc4", 4, 2
	clock := func() time.Time { return time.Unix(1794783000, 0) }
	names := make([]string, opts.Owners)
	for i := 0; ; i++ {
		name := fmt.Sprintf("activation.%d", i)
		id := (&Store{opts: opts}).route(name)
		names[id] = name
		full := true
		for _, n := range names {
			full = full && n != ""
		}
		if full {
			break
		}
	}
	seed, err := openAt(opts, clock)
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range names {
		if err = seed.Append(&points.Points{Metric: name, Data: []points.Point{{Timestamp: 1794782990, Value: 1}}}); err != nil {
			t.Fatal(err)
		}
	}
	requireClose(t, seed)
	entered, release := make(chan struct{}), make(chan struct{})
	s, err := openAdmissionWithHook(opts, clock, true, func(id int) {
		if id == 2 {
			close(entered)
			<-release
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	defer requireClose(t, s)
	<-entered // owner0 is live; owner2 (same physical DB) is still recovering.
	for i := 0; i < 6; i++ {
		if err = s.Append(&points.Points{Metric: names[0], Data: []points.Point{{Timestamp: 1794782991 + int64(i), Value: float64(i + 2)}}}); err != nil {
			t.Fatal(err)
		}
	}
	// Force an owner-local durability barrier without waiting for all recovery.
	ack := make(chan answer, 1)
	s.owners[0].requests <- command{op: 's', answer: ack}
	if err = (<-ack).err; err != nil {
		t.Fatal(err)
	}
	close(release)
	if err = s.WaitReady(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err = s.Flush(); err != nil {
		t.Fatal(err)
	}
	if got := s.Stats().IngestedPoints; got != 10 {
		t.Fatalf("count %d want10", got)
	}
	for i := 0; i < 6; i++ {
		got, e := s.Fetch(context.Background(), names[0], 1794782990+i, 1794782991+i)
		if e != nil {
			t.Fatal(e)
		}
		if len(got.Values) != 1 || math.Float64bits(got.Values[0]) != math.Float64bits(float64(i+2)) {
			t.Fatalf("slot%d values%v", i, got.Values)
		}
		got.Release()
	}
}
