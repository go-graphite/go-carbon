package carbon

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/persister"
	"github.com/go-graphite/go-carbon/receiver/tcp"
	whisper "github.com/go-graphite/go-whisper"
	protov3 "github.com/go-graphite/protocol/carbonapi_v3_pb"
	"github.com/lomik/zapwriter"
	"google.golang.org/protobuf/proto"
)

func TestV2PortableDaemonTCPHTTP(t *testing.T) {
	if isolatedTestProcess(t) {
		return
	}
	defer zapwriter.Test()()
	app := v2Fixture(t, &oracleBackend{})
	app.v2Open = nil
	app.Config.V2.Owners = 1
	app.Config.V2.Shards = 1
	app.Config.V2.PartInterval.Duration = time.Hour
	if e := app.Start(); e != nil {
		t.Fatal(e)
	}
	name := "routing.value"
	schema, ok := app.Config.Whisper.Schemas.Match(name)
	if !ok {
		t.Fatal("no schema")
	}
	aggr := app.Config.Whisper.Aggregation.Match(name)
	w, e := whisper.CreateWithOptions(filepath.Join(t.TempDir(), "classic.wsp"), schema.Retentions, aggr.AggregationMethod(), float32(aggr.XFilesFactor()), &whisper.Options{Sparse: true})
	if e != nil {
		t.Fatal(e)
	}
	defer w.Close()
	now := time.Now().Unix()
	step := int64(schema.Retentions[0].SecondsPerPoint())
	from, until := now-10*step, now-step
	values := []float64{1.2345678901234567, math.Copysign(0, -1), math.SmallestNonzeroFloat64, math.MaxFloat64, -7.25}
	ps := make([]*whisper.TimeSeriesPoint, len(values))
	for i, v := range values {
		ps[i] = &whisper.TimeSeriesPoint{Time: int(from) + (i+1)*int(step), Value: v}
	}
	if e = w.UpdateMany(ps); e != nil {
		t.Fatal(e)
	}
	conn, e := net.Dial("tcp", app.Receivers[0].Receiver.(*tcp.TCP).Addr().String())
	if e != nil {
		t.Fatal(e)
	}
	for i, v := range values {
		if _, e = fmt.Fprintf(conn, "%s %.17g %d\n", name, v, from+(int64(i)+1)*step); e != nil {
			t.Fatal(e)
		}
	}
	_ = conn.Close()
	deadline := time.Now().Add(5 * time.Second)
	for app.v2.Backend.Stats().IngestedPoints < uint64(len(values)) {
		if time.Now().After(deadline) {
			t.Fatal("TCP admission timeout")
		}
		time.Sleep(time.Millisecond)
	}
	request := &protov3.MultiFetchRequest{Metrics: []*protov3.FetchRequest{{Name: name, PathExpression: name, StartTime: from, StopTime: until}}}
	encoded, e := proto.Marshal(request)
	if e != nil {
		t.Fatal(e)
	}
	address := "http://" + app.Carbonserver.HTTPListener().Addr().String()
	res, e := http.Post(address+"/render/?format=carbonapi_v3_pb", "application/x-carbonapi-v3-pb", bytes.NewReader(encoded))
	if e != nil {
		t.Fatal(e)
	}
	body, e := io.ReadAll(res.Body)
	_ = res.Body.Close()
	if e != nil {
		t.Fatal(e)
	}
	if res.StatusCode != 200 {
		t.Fatalf("render %d %s", res.StatusCode, body)
	}
	var got protov3.MultiFetchResponse
	if e = proto.Unmarshal(body, &got); e != nil {
		t.Fatal(e)
	}
	if len(got.Metrics) != 1 {
		t.Fatalf("metrics=%d", len(got.Metrics))
	}
	want, e := w.Fetch(int(from), int(until))
	if e != nil {
		t.Fatal(e)
	}
	g := got.Metrics[0]
	if int(g.StartTime) != want.FromTime() || int(g.StopTime) != want.UntilTime() || int(g.StepTime) != want.Step() || len(g.Values) != len(want.Values()) {
		t.Fatal("render grid differs")
	}
	for i, v := range want.Values() {
		if math.Float64bits(g.Values[i]) != math.Float64bits(v) {
			t.Fatalf("slot%d bits%x != %x", i, math.Float64bits(g.Values[i]), math.Float64bits(v))
		}
	}
	if app.v2.Backend.Stats().SnapshotPins != 0 {
		t.Fatal("HTTP response reservation leaked")
	}
	if e = app.DumpStop(); e != nil {
		t.Fatal(e)
	}
}

// This opt-in integration measurement drives the real receiver and storage
// boundary. The shell supplies CPU affinity and the scratch-device IO cap.
func TestV2DaemonThroughput120(t *testing.T) {
	if os.Getenv("V2_BACKEND_THROUGHPUT") != "1" {
		t.Skip("explicit native benchmark only")
	}
	// The explicitly selected native benchmark owns this process. Wrapping it
	// in isolatedTestProcess would hide successful receipts behind t.Log when
	// the outer runner omits -test.v and would distort the exec-start clock.
	defer zapwriter.Test()()
	app := v2Fixture(t, &oracleBackend{})
	app.v2Open = nil
	app.Config.V2.WriteEngine = os.Getenv("V2_BACKEND_WRITE_ENGINE")
	if codec := os.Getenv("V2_JOURNAL_CODEC"); codec != "" {
		app.Config.V2.JournalCodec = codec
	}
	app.Config.Common.MaxCPU = 32
	// The ordinary fixture has a minute/hour policy. Use the single-archive
	// intake shape explicitly here; repeated timestamps within a minute would
	// otherwise conceal the cost of advancing fine-resolution data.
	schema := filepath.Join(t.TempDir(), "throughput-schemas.conf")
	if err := os.WriteFile(schema, []byte("[all]\npattern = .*\nretentions = 1s:1d\n"), 0600); err != nil {
		t.Fatal(err)
	}
	var err error
	app.Config.Whisper.Schemas, err = persister.ReadWhisperSchemas(schema)
	if err != nil {
		t.Fatal(err)
	}
	app.Config.V2.Owners = 16
	app.Config.V2.Shards = 16
	app.Config.V2.PartInterval.Duration = time.Second
	for _, item := range []struct {
		name string
		dst  *int
	}{{"V2_BACKEND_CPUS", &app.Config.Common.MaxCPU}, {"V2_BACKEND_OWNERS", &app.Config.V2.Owners}, {"V2_BACKEND_DBS", &app.Config.V2.Shards}} {
		if v := os.Getenv(item.name); v != "" {
			n, e := strconv.Atoi(v)
			if e != nil || n < 1 {
				t.Fatal("invalid " + item.name)
			}
			*item.dst = n
		}
	}
	if err := app.Start(); err != nil {
		t.Fatal(err)
	}
	fmt.Printf("V2_DAEMON_CONFIG gomaxprocs=%d owners=%d dbs=%d journal_codec=%s\n", runtime.GOMAXPROCS(0), app.Config.V2.Owners, app.Config.V2.Shards, app.Config.V2.JournalCodec)
	// Measure an actual TCP append, not just listener construction. The runner
	// supplies its pre-exec wall clock; absent it this is not an exec-start gate.
	address := app.Receivers[0].Receiver.(*tcp.TCP).Addr().String()
	probe, e := net.Dial("tcp", address)
	if e != nil {
		t.Fatal(e)
	}
	_, e = fmt.Fprintf(probe, "startup.probe 1 %d\n", time.Now().Unix()-1)
	if e != nil {
		t.Fatal(e)
	}
	_ = probe.Close()
	probeDeadline := time.Now().Add(5 * time.Second)
	for app.v2.Backend.Stats().IngestedPoints < 1 {
		if time.Now().After(probeDeadline) {
			t.Fatal("startup probe not accepted")
		}
		time.Sleep(time.Millisecond)
	}
	startupMS := -1.0
	if stamp := os.Getenv("V2_BACKEND_EXEC_NS"); stamp != "" {
		n, e := strconv.ParseInt(stamp, 10, 64)
		if e != nil {
			t.Fatal(e)
		}
		startupMS = float64(time.Now().UnixNano()-n) / 1e6
	}
	fmt.Printf("V2_STARTUP first_tcp_append_ms=%.3f exec_stamp=%t\n", startupMS, startupMS >= 0)
	var cpuFile *os.File
	if dir := os.Getenv("V2_BACKEND_PROFILES"); dir != "" {
		runtime.SetMutexProfileFraction(10)
		runtime.SetBlockProfileRate(1000000)
		var err error
		cpuFile, err = os.Create(filepath.Join(dir, "cpu.pprof"))
		if err != nil {
			t.Fatal(err)
		}
		if err = pprof.StartCPUProfile(cpuFile); err != nil {
			t.Fatal(err)
		}
	}
	addr := app.Receivers[0].Receiver.(*tcp.TCP).Addr().String()
	var stop atomic.Bool
	var wg sync.WaitGroup
	var usage0, usage1 syscall.Rusage
	_ = syscall.Getrusage(syscall.RUSAGE_SELF, &usage0)
	io0 := nativeWriteBytes()
	start := time.Now()
	points0 := app.v2.Backend.Stats().IngestedPoints
	journal0 := journalDiskBytes(app.Config.V2.DataDir)
	var sender *exec.Cmd
	stopFile := filepath.Join(t.TempDir(), "sender-stop")
	senderResult := filepath.Join(t.TempDir(), "sender-count")
	if cpus := os.Getenv("V2_BACKEND_SENDER_CPUS"); cpus != "" {
		sender = exec.Command("taskset", "-c", cpus, os.Args[0], "-test.run=^TestV2DaemonExternalSender$", "-test.v=true", "-test.timeout=210s")
		sender.Env = append(os.Environ(), "V2_BACKEND_SENDER_ADDR="+address, "V2_BACKEND_SENDER_STOP="+stopFile, "V2_BACKEND_SENDER_RESULT="+senderResult)
		sender.Stdout = os.Stdout
		sender.Stderr = os.Stderr
		if e := sender.Start(); e != nil {
			t.Fatal(e)
		}
		defer func() {
			if sender.Process != nil && sender.ProcessState == nil {
				_ = sender.Process.Kill()
				_ = sender.Wait()
			}
		}()
	}
	for producer := 0; sender == nil && producer < 32; producer++ {
		wg.Add(1)
		go func(p int) {
			defer wg.Done()
			conn, err := net.Dial("tcp", addr)
			if err != nil {
				t.Error(err)
				return
			}
			defer conn.Close()
			_ = conn.(*net.TCPConn).SetWriteBuffer(32 << 10)
			var packet bytes.Buffer
			stamp := time.Now().Unix() - 30
			for n := range 1024 {
				fmt.Fprintf(&packet, "bench.namespace%d.cluster1.host%04d.component.requests.value %.17g %d\n", p%4, p*128+n%128, float64(n)+0.125, stamp)
			}
			data := packet.Bytes()
			next := time.Now()
			for !stop.Load() {
				_ = conn.SetWriteDeadline(time.Now().Add(time.Second))
				if _, err = conn.Write(data); err != nil {
					return
				}
				if os.Getenv("V2_BACKEND_OFFER_1M") == "1" {
					next = next.Add(32768 * time.Microsecond)
					time.Sleep(time.Until(next))
				}
			}
		}(producer)
	}
	duration := 120 * time.Second
	if v := os.Getenv("V2_BACKEND_DURATION"); v != "" {
		var err error
		duration, err = time.ParseDuration(v)
		if err != nil {
			t.Fatal(err)
		}
	}
	// Receipts survive an external health guard stopping this process. The final
	// receipt uses the same counters and interval as these cumulative samples.
	receipt := func(stage string) {
		var u syscall.Rusage
		_ = syscall.Getrusage(syscall.RUSAGE_SELF, &u)
		seconds := func(x syscall.Timeval) float64 { return float64(x.Sec) + float64(x.Usec)/1e6 }
		cpu := seconds(u.Utime) + seconds(u.Stime) - seconds(usage0.Utime) - seconds(usage0.Stime)
		accepted := app.v2.Backend.Stats().IngestedPoints - points0
		elapsed := time.Since(start).Seconds()
		journal := journalDiskBytes(app.Config.V2.DataDir) - journal0
		fmt.Printf("V2_DAEMON_RECEIPT stage=%s seconds=%.6f accepted=%d points_per_second=%.3f journal_bytes=%d journal_bytes_per_point=%.6f cpu_seconds=%.6f cpu_ns_per_point=%.3f rss_bytes=%d maxrss_native_units=%d stats=%+v\n", stage, elapsed, accepted, float64(accepted)/elapsed, journal, float64(journal)/float64(accepted), cpu, cpu*1e9/float64(accepted), nativeRSSBytes(), u.Maxrss, app.v2.Backend.Stats())
	}
	tick := time.NewTicker(5 * time.Second)
	timer := time.NewTimer(duration)
measure:
	for {
		select {
		case <-tick.C:
			receipt("periodic")
		case <-timer.C:
			break measure
		}
	}
	tick.Stop()
	receipt("final")
	elapsed := time.Since(start).Seconds()
	stats := app.v2.Backend.Stats()
	_ = syscall.Getrusage(syscall.RUSAGE_SELF, &usage1)
	io1 := nativeWriteBytes()
	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)
	seconds := func(x syscall.Timeval) float64 { return float64(x.Sec) + float64(x.Usec)/1e6 }
	cpu := seconds(usage1.Utime) + seconds(usage1.Stime) - seconds(usage0.Utime) - seconds(usage0.Stime)
	// This explicit opt-in benchmark must emit its receipt even without -test.v.
	accepted := stats.IngestedPoints - points0
	journalBytes := journalDiskBytes(app.Config.V2.DataDir) - journal0
	fmt.Printf("V2_DAEMON_THROUGHPUT seconds=%.3f accepted=%d points_per_second=%.1f journal_bytes=%d journal_bytes_per_point=%.6f cpu_cores=%.3f cpu_ns_per_point=%.3f maxrss_native_units=%d heap_bytes=%d device_write_bytes=%d device_MB_per_second=%.3f startup_ms=%.3f external_senders=%t stats=%+v\n", elapsed, accepted, float64(accepted)/elapsed, journalBytes, float64(journalBytes)/float64(accepted), cpu/elapsed, cpu*1e9/float64(accepted), usage1.Maxrss, mem.HeapAlloc, io1-io0, float64(io1-io0)/elapsed/1e6, startupMS, sender != nil, stats)

	stop.Store(true)
	if sender != nil {
		if e := os.WriteFile(stopFile, []byte("stop"), 0600); e != nil {
			t.Fatal(e)
		}
		if e := sender.Wait(); e != nil {
			t.Fatal(e)
		}
	}
	wg.Wait()
	settleStart := time.Now()
	var written uint64
	if sender != nil {
		data, err := os.ReadFile(senderResult)
		if err != nil {
			t.Fatal(err)
		}
		written, err = strconv.ParseUint(string(data), 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		// Closing senders only proves socket delivery. Wait for the receiver's
		// accepted frontier before freezing heads, and fail on loss or duplicates.
		deadline := time.Now().Add(45 * time.Second)
		for app.v2.Backend.Stats().IngestedPoints-points0 < written {
			if time.Now().After(deadline) {
				t.Fatalf("receiver drain incomplete: wrote=%d accepted=%d", written, app.v2.Backend.Stats().IngestedPoints-points0)
			}
			time.Sleep(10 * time.Millisecond)
		}
		if got := app.v2.Backend.Stats().IngestedPoints - points0; got != written {
			t.Fatalf("receiver count differs: wrote=%d accepted=%d", written, got)
		}
	}
	if e := app.v2.Backend.Flush(); e != nil {
		t.Fatal(e)
	}
	fmt.Printf("V2_DAEMON_DRAIN final_accepted=%d flush_seconds=%.6f stats=%+v\n", app.v2.Backend.Stats().IngestedPoints, time.Since(settleStart).Seconds(), app.v2.Backend.Stats())
	completedBackend := app.v2.Backend
	if err := app.DumpStop(); err != nil {
		t.Fatal(err)
	}
	if cpuFile != nil {
		pprof.StopCPUProfile()
		_ = cpuFile.Close()
		for _, kind := range []string{"mutex", "block"} {
			f, err := os.Create(filepath.Join(os.Getenv("V2_BACKEND_PROFILES"), kind+".pprof"))
			if err != nil {
				t.Fatal(err)
			}
			_ = pprof.Lookup(kind).WriteTo(f, 0)
			_ = f.Close()
		}
	}
	_ = syscall.Getrusage(syscall.RUSAGE_SELF, &usage1)
	cpu = seconds(usage1.Utime) + seconds(usage1.Stime) - seconds(usage0.Utime) - seconds(usage0.Stime)
	elapsed = time.Since(start).Seconds()
	final := completedBackend.Stats().IngestedPoints - points0
	fmt.Printf("V2_DAEMON_COMPLETE seconds=%.6f points=%d sender_points=%d points_per_second=%.3f cpu_ns_per_point=%.3f journal_bytes_per_point=%.6f maxrss_native_units=%d drain_and_close_seconds=%.6f\n", elapsed, final, written, float64(final)/elapsed, cpu*1e9/float64(final), float64(journalDiskBytes(app.Config.V2.DataDir)-journal0)/float64(final), usage1.Maxrss, time.Since(settleStart).Seconds())
}

func journalDiskBytes(dir string) int64 {
	var n int64
	// Durable catalog definitions are part of the replay input, including WALs.
	roots, _ := filepath.Glob(filepath.Join(dir, "owner-*", "name-catalog"))
	roots = append(roots, filepath.Join(dir, "journal"))
	for _, root := range roots {
		_ = filepath.WalkDir(root, func(_ string, d os.DirEntry, e error) error {
			if e != nil {
				return e
			}
			if !d.IsDir() {
				if i, e := d.Info(); e == nil {
					n += i.Size()
				}
			}
			return nil
		})
	}
	return n
}

// Separate process: its CPU and memory are excluded from daemon RUSAGE_SELF.
func TestV2DaemonExternalSender(t *testing.T) {
	addr := os.Getenv("V2_BACKEND_SENDER_ADDR")
	if addr == "" {
		t.Skip("external sender only")
	}
	// TestMain deliberately caps ordinary test suites at four CPUs. This
	// separate, explicitly requested load process has an independent budget.
	cpus := v2BenchPositiveInt(t, "V2_BACKEND_SENDER_MAXPROCS", 96)
	connections := v2BenchPositiveInt(t, "V2_BACKEND_CONNECTIONS", 64)
	cardinality := v2BenchPositiveInt(t, "V2_BACKEND_CARDINALITY", 1048576)
	if connections > cardinality {
		t.Fatal("connections exceed cardinality")
	}
	runtime.GOMAXPROCS(cpus)
	fmt.Printf("V2_TCP_SENDER_CONFIG gomaxprocs=%d connections=%d cardinality=%d advancing_timestamps=true\n", runtime.GOMAXPROCS(0), connections, cardinality)
	start := time.Now()
	var u0, u1 syscall.Rusage
	_ = syscall.Getrusage(syscall.RUSAGE_SELF, &u0)
	var stop atomic.Bool
	go func() {
		for !stop.Load() {
			if _, e := os.Stat(os.Getenv("V2_BACKEND_SENDER_STOP")); e == nil {
				stop.Store(true)
				return
			}
			time.Sleep(20 * time.Millisecond)
		}
	}()
	var wg sync.WaitGroup
	var sent atomic.Uint64
	for producer := range connections {
		wg.Add(1)
		go func(p int) {
			defer wg.Done()
			conn, e := net.Dial("tcp", addr)
			if e != nil {
				t.Error(e)
				return
			}
			defer conn.Close()
			// Names and values are prepared once, independently of storage CPU.
			// Each connection walks disjoint names, and timestamps advance with
			// wall time instead of replaying one frozen packet for two minutes.
			lines := make([][]byte, (cardinality+connections-1-p)/connections)
			for i := range lines {
				id := i*connections + p
				lines[i] = []byte(fmt.Sprintf("bench.namespace%d.cluster1.host%07d.component.requests.value %d.125 ", id%4, id, id%100003))
			}
			data := make([]byte, 0, 128<<10)
			nextName := 0
			next := time.Now()
			for !stop.Load() {
				data = data[:0]
				stamp := time.Now().Unix() - 1
				for range 1024 {
					data = append(data, lines[nextName]...)
					data = strconv.AppendInt(data, stamp, 10)
					data = append(data, '\n')
					nextName = (nextName + 1) % len(lines)
				}
				_ = conn.SetWriteDeadline(time.Now().Add(30 * time.Second))
				if _, e = io.Copy(conn, bytes.NewReader(data)); e != nil {
					if !stop.Load() {
						t.Error(e)
					}
					return
				}
				sent.Add(1024)
				if os.Getenv("V2_BACKEND_OFFER_1M") == "1" {
					next = next.Add(32768 * time.Microsecond)
					time.Sleep(time.Until(next))
				}
			}
		}(producer)
	}
	wg.Wait()
	stop.Store(true)
	if path := os.Getenv("V2_BACKEND_SENDER_RESULT"); path != "" {
		if err := os.WriteFile(path, []byte(strconv.FormatUint(sent.Load(), 10)), 0600); err != nil {
			t.Fatal(err)
		}
	}
	_ = syscall.Getrusage(syscall.RUSAGE_SELF, &u1)
	seconds := func(u syscall.Rusage) float64 {
		return float64(u.Utime.Sec+u.Stime.Sec) + float64(u.Utime.Usec+u.Stime.Usec)/1e6
	}
	fmt.Printf("V2_TCP_SENDERS connections=%d full_write_points=%d seconds=%.6f points_per_second=%.3f cpu_seconds=%.6f maxrss_native_units=%d\n", connections, sent.Load(), time.Since(start).Seconds(), float64(sent.Load())/time.Since(start).Seconds(), seconds(u1)-seconds(u0), u1.Maxrss)
}

func v2BenchPositiveInt(t *testing.T, key string, fallback int) int {
	t.Helper()
	if value := os.Getenv(key); value != "" {
		n, err := strconv.Atoi(value)
		if err != nil || n < 1 {
			t.Fatal("invalid " + key)
		}
		return n
	}
	return fallback
}

func nativeRSSBytes() uint64 {
	b, _ := os.ReadFile("/proc/self/status")
	for _, line := range strings.Split(string(b), "\n") {
		var kb uint64
		if _, err := fmt.Sscanf(line, "VmRSS: %d kB", &kb); err == nil {
			return kb * 1024
		}
	}
	return 0
}

// Same sender implementation and CPU placement as the daemon arm, with a TCP
// discard sink on the daemon CPUs. This establishes sender headroom explicitly.
func TestV2DaemonSenderCapacity(t *testing.T) {
	if os.Getenv("V2_BACKEND_SENDER_CAPACITY") != "1" {
		t.Skip("explicit native benchmark only")
	}
	runtime.GOMAXPROCS(v2BenchPositiveInt(t, "V2_BACKEND_CPUS", 96))
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	var wg sync.WaitGroup
	go func() {
		for {
			c, e := l.Accept()
			if e != nil {
				return
			}
			wg.Add(1)
			go func() { defer wg.Done(); defer c.Close(); _, _ = io.Copy(io.Discard, c) }()
		}
	}()
	stopFile := filepath.Join(t.TempDir(), "stop")
	cmd := exec.Command("taskset", "-c", os.Getenv("V2_BACKEND_SENDER_CPUS"), os.Args[0], "-test.run=^TestV2DaemonExternalSender$", "-test.v", "-test.timeout=60s")
	cmd.Env = append(os.Environ(), "V2_BACKEND_SENDER_ADDR="+l.Addr().String(), "V2_BACKEND_SENDER_STOP="+stopFile)
	cmd.Stdout, cmd.Stderr = os.Stdout, os.Stderr
	if err = cmd.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if cmd.ProcessState == nil {
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
		}
	}()
	time.Sleep(15 * time.Second)
	if err = os.WriteFile(stopFile, []byte("stop"), 0600); err != nil {
		t.Fatal(err)
	}
	if err = cmd.Wait(); err != nil {
		t.Fatal(err)
	}
	wg.Wait()
}

func nativeWriteBytes() uint64 {
	b, _ := os.ReadFile("/proc/self/io")
	for _, line := range strings.Split(string(b), "\n") {
		var n uint64
		if _, err := fmt.Sscanf(line, "write_bytes: %d", &n); err == nil {
			return n
		}
	}
	return 0
}

func TestV2DaemonCoreSIGKILLReplay(t *testing.T) {
	if isolatedTestProcess(t) {
		return
	}
	t.Setenv("V2_DAEMON_CRASH_ENGINE", "core")
	TestV2DaemonSIGKILLReplay(t)
}
func TestV2DaemonSIGKILLReplay(t *testing.T) {
	if isolatedTestProcess(t) {
		return
	}
	defer zapwriter.Test()()
	root := os.Getenv("V2_DAEMON_CRASH_ROOT")
	child := root != ""
	if !child {
		root = t.TempDir()
	}
	app := v2Fixture(t, &oracleBackend{})
	app.v2Open = nil
	app.Config.V2.WriteEngine = os.Getenv("V2_DAEMON_CRASH_ENGINE")
	app.Config.V2.DataDir = filepath.Join(root, "parts")
	app.Config.V2.Owners = 1
	app.Config.V2.Shards = 1
	app.Config.V2.PartInterval.Duration = time.Hour
	schema, ok := app.Config.Whisper.Schemas.Match("routing.value")
	if !ok {
		t.Fatal("schema")
	}
	step := int64(schema.Retentions[0].SecondsPerPoint())
	if child {
		if err := app.Start(); err != nil {
			t.Fatal(err)
		}
		stamp := time.Now().Unix()/step*step - 5*step
		conn, err := net.Dial("tcp", app.Receivers[0].Receiver.(*tcp.TCP).Addr().String())
		if err != nil {
			t.Fatal(err)
		}
		for i, v := range []float64{math.Copysign(0, -1), math.SmallestNonzeroFloat64, 1.2345678901234567, math.MaxFloat64} {
			if _, err = fmt.Fprintf(conn, "routing.value %.17g %d\n", v, stamp+int64(i)*step); err != nil {
				t.Fatal(err)
			}
		}
		_ = conn.Close()
		for deadline := time.Now().Add(5 * time.Second); app.v2.Backend.Stats().IngestedPoints < 4; {
			if time.Now().After(deadline) {
				t.Fatal("admission timeout")
			}
			time.Sleep(time.Millisecond)
		}
		// Wait across two group-sync ticks without publishing a part.
		time.Sleep(2200 * time.Millisecond)
		// Publish the timestamp atomically: ReadFile can observe an empty newly
		// created file before WriteFile fills it on a busy filesystem.
		if err = os.WriteFile(filepath.Join(root, "ready.tmp"), []byte(strconv.FormatInt(stamp, 10)), 0600); err != nil {
			t.Fatal(err)
		}
		if err = os.Rename(filepath.Join(root, "ready.tmp"), filepath.Join(root, "ready")); err != nil {
			t.Fatal(err)
		}
		select {}
	}
	cmd := exec.Command(os.Args[0], "-test.run=^"+t.Name()+"$", "-test.timeout=30s")
	cmd.Env = append(os.Environ(), "V2_DAEMON_CRASH_ROOT="+root)
	var childOutput bytes.Buffer
	cmd.Stdout = &childOutput
	cmd.Stderr = &childOutput
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cmd.Process.Kill() }()
	var ready []byte
	for deadline := time.Now().Add(15 * time.Second); ; {
		var err error
		ready, err = os.ReadFile(filepath.Join(root, "ready"))
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
			t.Fatalf("child readiness: %s", childOutput.String())
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err := cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	if err := cmd.Wait(); err == nil {
		t.Fatal("child exited without SIGKILL")
	}
	if err := app.Start(); err != nil {
		t.Fatal(err)
	}
	stats := app.v2.Backend.Stats()
	if stats.IngestedPoints != 4 {
		t.Fatalf("recovery counts %+v", stats)
	}
	stamp, err := strconv.ParseInt(string(ready), 10, 64)
	if err != nil {
		t.Fatalf("invalid child readiness timestamp %q: %v", ready, err)
	}
	got, err := app.v2.Backend.Fetch(context.Background(), "routing.value", int(stamp-step), int(stamp+3*step))
	if err != nil {
		t.Fatal(err)
	}
	if got == nil {
		t.Fatalf("missing replay response for timestamp %d", stamp)
	}
	if got.Release != nil {
		defer got.Release()
	}
	want := []float64{math.Copysign(0, -1), math.SmallestNonzeroFloat64, 1.2345678901234567, math.MaxFloat64}
	if len(got.Values) != len(want) {
		t.Fatalf("slots%d", len(got.Values))
	}
	for i, v := range want {
		if math.Float64bits(got.Values[i]) != math.Float64bits(v) {
			t.Fatalf("slot%d bits%x != %x", i, math.Float64bits(got.Values[i]), math.Float64bits(v))
		}
	}
	t.Log("real TCP daemon SIGKILL: four synced unsealed journal records recovered with exact count and bits")
}
