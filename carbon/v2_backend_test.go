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
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

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
	if e = app.v2.Backend.Flush(); e != nil {
		t.Fatal(e)
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
	if isolatedTestProcess(t) {
		return
	}
	defer zapwriter.Test()()
	app := v2Fixture(t, &oracleBackend{})
	app.v2Open = nil
	app.Config.Common.MaxCPU = 32
	app.Config.V2.Owners = 16
	app.Config.V2.Shards = 16
	app.Config.V2.PartInterval.Duration = time.Second
	if err := app.Start(); err != nil {
		t.Fatal(err)
	}
	addr := app.Receivers[0].Receiver.(*tcp.TCP).Addr().String()
	var stop atomic.Bool
	var wg sync.WaitGroup
	var usage0, usage1 syscall.Rusage
	_ = syscall.Getrusage(syscall.RUSAGE_SELF, &usage0)
	io0 := nativeWriteBytes()
	start := time.Now()
	for producer := range 32 {
		wg.Add(1)
		go func(p int) {
			defer wg.Done()
			conn, err := net.Dial("tcp", addr)
			if err != nil {
				t.Error(err)
				return
			}
			defer conn.Close()
			var packet bytes.Buffer
			stamp := time.Now().Unix() - 30
			for n := range 1024 {
				fmt.Fprintf(&packet, "load.p%d.metric%d %.17g %d\n", p, n%128, float64(n)+0.125, stamp)
			}
			data := packet.Bytes()
			for !stop.Load() {
				_ = conn.SetWriteDeadline(time.Now().Add(time.Second))
				if _, err = conn.Write(data); err != nil {
					return
				}
			}
		}(producer)
	}
	time.Sleep(120 * time.Second)
	elapsed := time.Since(start).Seconds()
	stats := app.v2.Backend.Stats()
	_ = syscall.Getrusage(syscall.RUSAGE_SELF, &usage1)
	io1 := nativeWriteBytes()
	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)
	seconds := func(x syscall.Timeval) float64 { return float64(x.Sec) + float64(x.Usec)/1e6 }
	cpu := seconds(usage1.Utime) + seconds(usage1.Stime) - seconds(usage0.Utime) - seconds(usage0.Stime)
	t.Logf("V2_DAEMON_THROUGHPUT seconds=%.3f accepted=%d points_per_second=%.1f cpu_cores=%.3f maxrss_native_units=%d heap_bytes=%d device_write_bytes=%d device_MB_per_second=%.3f stats=%+v", elapsed, stats.IngestedPoints, float64(stats.IngestedPoints)/elapsed, cpu/elapsed, usage1.Maxrss, mem.HeapAlloc, io1-io0, float64(io1-io0)/elapsed/1e6, stats)
	stop.Store(true)
	wg.Wait()
	if err := app.DumpStop(); err != nil {
		t.Fatal(err)
	}
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
		if err = os.WriteFile(filepath.Join(root, "ready"), []byte(strconv.FormatInt(stamp, 10)), 0600); err != nil {
			t.Fatal(err)
		}
		select {}
	}
	cmd := exec.Command(os.Args[0], "-test.run=^TestV2DaemonSIGKILLReplay$", "-test.timeout=30s")
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
	stamp, _ := strconv.ParseInt(string(ready), 10, 64)
	got, err := app.v2.Backend.Fetch(context.Background(), "routing.value", int(stamp-step), int(stamp+3*step))
	if err != nil {
		t.Fatal(err)
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
