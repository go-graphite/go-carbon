package carbonserver

import (
	"bufio"
	"bytes"
	"fmt"
	"net"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/cache"
	"github.com/go-graphite/go-carbon/points"
	"github.com/go-graphite/go-carbon/receiver"
	"github.com/go-graphite/go-carbon/receiver/tcp"
)

func ingestTrie(b *testing.B, names []string) *trieIndex {
	b.Helper()
	ti := newTrie(".wsp", 0, nil)
	for _, name := range names {
		if _, err := ti.insert("/"+strings.ReplaceAll(name, ".", "/")+".wsp", 0, 0, 0, 0); err != nil {
			b.Fatal(err)
		}
	}
	if _, err := ti.applyQuotas(time.Minute, &Quota{Pattern: "/", Throughput: 159_000_000}, &Quota{Pattern: "services.*", Throughput: 50_000_000}); err != nil {
		b.Fatal(err)
	}
	return ti
}

func ingestName(i int) string {
	return fmt.Sprintf("services.namespace-%04d.host-%08d.component.requests.region.zone.%s.value", i%1000, i, strings.Repeat("x", 8+i%71))
}

func ingestNames(n int) []string {
	names := make([]string, n)
	for i := range names {
		names[i] = ingestName(i)
	}
	return names
}

func ingestWorkingNames(resident int, all []string) []string {
	if resident <= 262144 && all != nil {
		return all
	}
	names := make([]string, min(resident, 262144))
	for i := range names {
		index := i
		if resident > len(names) {
			index = (i * 7919) % resident
		}
		if all != nil {
			names[i] = all[index]
		} else {
			names[i] = ingestName(index)
		}
	}
	return names
}

func BenchmarkThroughputThrottle(b *testing.B) {
	names := ingestNames(16384)
	ti := ingestTrie(b, names)
	ps := points.OnePoint(names[0], 1, 1)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ps.Metric = names[i%len(names)]
		if ti.throughputThrottle(ps) {
			b.Fatal("unexpected throttle")
		}
	}
}

func ingestCPU() float64 {
	var r syscall.Rusage
	syscall.Getrusage(syscall.RUSAGE_SELF, &r)
	return float64(r.Utime.Sec+r.Stime.Sec) + float64(r.Utime.Usec+r.Stime.Usec)/1e6
}

// Each operation sends 256 points through a real loopback TCP socket, parser,
// quota hook and cache. The sender is a separate process, excluded from CPU.
var ingestTCPRegister sync.Once

func BenchmarkPlainTCPIngest(b *testing.B) {
	benchmarkPlainTCPIngest(b, 32768)
}

func BenchmarkPlainTCPIngestLarge(b *testing.B) {
	n, _ := strconv.Atoi(os.Getenv("INGEST_BENCH_METRICS"))
	if n == 0 {
		b.Skip("set INGEST_BENCH_METRICS=20000000")
	}
	benchmarkPlainTCPIngest(b, n)
}

type ingestTCPFixture struct {
	cache            *cache.Cache
	resident, repeat int
}

var ingestTCPResident struct {
	sync.Once
	fixture *ingestTCPFixture
}

func newIngestTCPFixture(b *testing.B, resident int) *ingestTCPFixture {
	const batch = 256
	repeat, _ := strconv.Atoi(os.Getenv("INGEST_BENCH_REPEAT"))
	if repeat < 1 {
		repeat = 1
	}
	if batch%repeat != 0 {
		b.Fatal("INGEST_BENCH_REPEAT must divide 256")
	}
	allNames := ingestNames(resident)
	names := ingestWorkingNames(resident, allNames)
	ti := ingestTrie(b, names)
	listener := NewCarbonserverListener(nil)
	listener.UpdateFileIndex(&fileIndex{trieIdx: ti})
	c := cache.New()
	c.SetMaxSize(0)
	c.SetWriteoutBatching(960, 960*time.Second)
	c.SetWriteoutSpread(.2)
	for _, name := range allNames {
		c.Add(points.OnePoint(name, 1, 1790000000))
	}
	// Active metrics start at a representative buffered length, so repeated
	// trials do not alternate between different compact-buffer growth steps.
	warm := make([]points.Point, 160)
	for i := range warm {
		warm[i] = points.Point{Value: 1, Timestamp: 1790000000}
	}
	for _, name := range names {
		c.Add(&points.Points{Metric: name, Data: warm})
	}
	c.SetBloomSize(50000000)
	c.SetNewMetricsChan(make(chan string, 1))
	c.SetMetricExists(func(string) bool { return true })
	c.SetThrottleWithState(listener.ShouldThrottleMetricWithState)
	for _, name := range names {
		c.AddPoint([]byte(name), 1, 1790000000)
	}
	return &ingestTCPFixture{cache: c, resident: resident, repeat: repeat}
}

func benchmarkPlainTCPIngest(b *testing.B, resident int) {
	const batch = 256
	var fixture *ingestTCPFixture
	if resident > 32768 {
		ingestTCPResident.Do(func() {
			ingestTCPResident.fixture = newIngestTCPFixture(b, resident)
			runtime.GC()
		})
		fixture = ingestTCPResident.fixture
	} else {
		fixture = newIngestTCPFixture(b, resident)
	}
	c := fixture.cache
	ingestTCPRegister.Do(tcp.Register)
	opts := tcp.NewOptions()
	opts.Listen = "127.0.0.1:0"
	options, err := receiver.WithProtocol(opts, "tcp")
	if err != nil {
		b.Fatal(err)
	}
	r, err := receiver.New("ingest-bench", options, c.Add)
	if err != nil {
		b.Fatal(err)
	}
	rcv := r.(*tcp.TCP)
	defer rcv.Stop()
	done := make(chan struct{})
	received := 0
	if target, ok := any(rcv).(interface {
		SetPointsOut(func([]byte, []points.Point))
	}); ok {
		target.SetPointsOut(func(name []byte, data []points.Point) {
			if target, ok := any(c).(interface{ AddPoints([]byte, []points.Point) }); ok {
				target.AddPoints(name, data)
			} else {
				for _, p := range data {
					c.AddPoint(name, p.Value, p.Timestamp)
				}
			}
			received += len(data)
			if received == b.N*batch {
				close(done)
			}
		})
	} else {
		rcv.SetPointOut(func(name []byte, value float64, ts int64) {
			c.AddPoint(name, value, ts)
			received++
			if received == b.N*batch {
				close(done)
			}
		})
	}
	var stderr bytes.Buffer
	child := exec.Command(os.Args[0], "-test.run=^TestIngestTCPBenchmarkClient$", "-test.timeout=30s")
	child.Env = append(os.Environ(), "INGEST_TCP_CLIENT=1", "INGEST_TCP_ADDRESS="+rcv.Addr().String(), "INGEST_TCP_RESIDENT="+strconv.Itoa(fixture.resident), "INGEST_TCP_REPEAT="+strconv.Itoa(fixture.repeat), "INGEST_TCP_WRITES="+strconv.Itoa(b.N))
	child.Stderr = &stderr
	input, err := child.StdinPipe()
	if err != nil {
		b.Fatal(err)
	}
	output, err := child.StdoutPipe()
	if err != nil {
		b.Fatal(err)
	}
	if err := child.Start(); err != nil {
		b.Fatal(err)
	}
	defer func() {
		child.Process.Kill()
		child.Wait()
	}()
	if ready, err := bufio.NewReader(output).ReadString('\n'); err != nil || ready != "ready\n" {
		b.Fatalf("sender startup: %q %v", ready, err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	cpu := ingestCPU()
	if _, err := input.Write([]byte("go\n")); err != nil {
		b.Fatal(err)
	}
	input.Close()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		b.Fatal("receiver did not drain")
	}
	cpu = ingestCPU() - cpu
	b.StopTimer()
	if err := child.Wait(); err != nil {
		b.Fatalf("sender: %v: %s", err, stderr.String())
	}
	b.ReportMetric(float64(b.N*batch)/cpu, "points/core-s")
	b.ReportMetric(float64(b.N*batch)/b.Elapsed().Seconds(), "points/s")
}

// A test-binary helper keeps sender CPU out of receiver measurements. It
// builds payloads before signalling readiness, then waits for the timed phase.
func TestIngestTCPBenchmarkClient(t *testing.T) {
	if os.Getenv("INGEST_TCP_CLIENT") != "1" {
		t.Skip("benchmark helper")
	}
	resident, _ := strconv.Atoi(os.Getenv("INGEST_TCP_RESIDENT"))
	repeat, _ := strconv.Atoi(os.Getenv("INGEST_TCP_REPEAT"))
	writes, _ := strconv.Atoi(os.Getenv("INGEST_TCP_WRITES"))
	const batch = 256
	names := ingestWorkingNames(resident, nil)
	var payloads [][]byte
	for start := 0; start < len(names); start += batch / repeat {
		var payload strings.Builder
		for _, name := range names[start : start+batch/repeat] {
			for range repeat {
				fmt.Fprintf(&payload, "%s 1 1790000000\n", name)
			}
		}
		payloads = append(payloads, []byte(payload.String()))
	}
	conn, err := net.Dial("tcp", os.Getenv("INGEST_TCP_ADDRESS"))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	fmt.Fprintln(os.Stdout, "ready")
	if _, err := bufio.NewReader(os.Stdin).ReadString('\n'); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < writes; i++ {
		if _, err := conn.Write(payloads[i%len(payloads)]); err != nil {
			t.Fatal(err)
		}
	}
}

func BenchmarkThroughputThrottleCached(b *testing.B) {
	names := ingestNames(16384)
	ti := ingestTrie(b, names)
	listener := NewCarbonserverListener(nil)
	listener.UpdateFileIndex(&fileIndex{trieIdx: ti})
	states := make([]any, len(names))
	ps := points.OnePoint(names[0], 1, 1)
	for i, name := range names {
		ps.Metric = name
		listener.ShouldThrottleMetricWithState(ps, true, &states[i])
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		j := i % len(names)
		ps.Metric = names[j]
		if listener.ShouldThrottleMetricWithState(ps, true, &states[j]) {
			b.Fatal("unexpected throttle")
		}
	}
}
