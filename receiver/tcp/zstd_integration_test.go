//go:build zstdintegration && unix

package tcp

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/points"
)

// This opt-in integration uses the production receiver and its real descriptor
// handoff across processes, plus an independently built nanotube executable.
// Every parser output is observed before storage can coalesce duplicate points.
type relayCapture struct {
	mu     sync.Mutex
	points []*points.Points
}

func (c *relayCapture) add(p *points.Points) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.points = append(c.points, p)
}
func (c *relayCapture) count() int { c.mu.Lock(); defer c.mu.Unlock(); return len(c.points) }

func TestZstdInheritedProcess(t *testing.T) {
	path := os.Getenv("ZSTD_HANDOFF_MANIFEST")
	if path == "" {
		t.Skip("integration worker")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var leftovers [][]byte
	if err = json.Unmarshal(data, &leftovers); err != nil {
		t.Fatal(err)
	}
	h := &Handoff{Listener: os.NewFile(3, "listener")}
	for i, leftover := range leftovers {
		h.Conns = append(h.Conns, Conn{File: os.NewFile(uintptr(4+i), "connection"), Leftover: leftover})
	}
	var received relayCapture
	next, err := NewInherited("tcp", &Options{Enabled: true, Listen: os.Getenv("ZSTD_HANDOFF_ADDRESS")}, func(p *points.Points) { received.add(p); fmt.Println("POINT") }, h)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println("READY")
	_, _ = io.Copy(io.Discard, os.Stdin)
	next.Stop()
	data, err = json.Marshal(received.points)
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(path+".points", data, 0600); err != nil {
		t.Fatal(err)
	}
}

func TestZstdNanotubeIntegration(t *testing.T) {
	binary := os.Getenv("NANOTUBE_BIN")
	if binary == "" {
		t.Skip("set NANOTUBE_BIN to a locally built nanotube")
	}
	for _, connections := range []int{1, 4} {
		t.Run(fmt.Sprint(connections), func(t *testing.T) {
			root := t.TempDir()
			var compressed, plain relayCapture
			options := NewOptions()
			options.Listen = "127.0.0.1:0"
			old, err := newTCP("tcp", options, compressed.add)
			if err != nil {
				t.Fatal(err)
			}
			defer old.Stop()
			other, err := newTCP("tcp", options, plain.add)
			if err != nil {
				t.Fatal(err)
			}
			defer other.Stop()
			reserve, e := net.Listen("tcp", "127.0.0.1:0")
			if e != nil {
				t.Fatal(e)
			}
			inputAddr := reserve.Addr().String()
			_ = reserve.Close()
			write := func(name, body string) string {
				p := filepath.Join(root, name)
				if e := os.WriteFile(p, []byte(body), 0600); e != nil {
					t.Fatal(e)
				}
				return p
			}
			clusterPath := write("clusters.toml", fmt.Sprintf("[[cluster]]\nname='example'\ntype='toall'\n[[cluster.hosts]]\nname='127.0.0.1'\nport=%d\nmtcp=%d\n[[cluster.hosts]]\nname='127.0.0.1'\nport=%d\ncompression='none'\n", old.Addr().(*net.TCPAddr).Port, connections, other.Addr().(*net.TCPAddr).Port))
			rulesPath := write("rules.toml", "[[rule]]\nprefixes=['example.']\nclusters=['example']\n")
			config := write("config.toml", fmt.Sprintf("ClustersConfig=%q\nRulesConfig=%q\nListenTCP=%q\nListenUDP=''\nListenGRPC=''\nTCPOutCompression='zstd'\nTCPOutZstdFrameSize=4096\nTCPOutBufFlushPeriodSec=1\nTCPOutConnectionRefreshPeriodSec=1\nTCPInitialConnCheck=false\nPprofPort=-1\nPromPort=0\nMainQueueBatchSize=1\nWorkerPoolSize=1\nNormalizeRecords=false\n", clusterPath, rulesPath, inputAddr))
			relay := exec.Command(binary, "-config", config)
			logFile, e := os.Create(filepath.Join(root, "relay.log"))
			if e != nil {
				t.Fatal(e)
			}
			defer logFile.Close()
			relay.Stdout = logFile
			relay.Stderr = logFile
			if e = relay.Start(); e != nil {
				t.Fatal(e)
			}
			relayDone := false
			defer func() {
				if !relayDone {
					_ = relay.Process.Kill()
					_ = relay.Wait()
				}
			}()
			var input net.Conn
			deadline := time.Now().Add(10 * time.Second)
			for {
				input, e = net.DialTimeout("tcp", inputAddr, 100*time.Millisecond)
				if e == nil {
					break
				}
				if time.Now().After(deadline) {
					data, _ := os.ReadFile(logFile.Name())
					t.Fatalf("relay not listening: %s", data)
				}
				time.Sleep(10 * time.Millisecond)
			}
			defer input.Close()
			const total = 900
			send := func(from, to int) {
				for i := from; i < to; i++ {
					line := fmt.Sprintf("example.point%d %.2f %d\n", i, float64(i)+0.25, 1700000000+i)
					if _, e = input.Write([]byte(line)); e != nil {
						t.Fatal(e)
					}
				}
			}
			wait := func(f func() bool) {
				end := time.Now().Add(10 * time.Second)
				for !f() {
					if time.Now().After(end) {
						t.Fatalf("timed out: compressed=%d plaintext=%d", compressed.count(), plain.count())
					}
					time.Sleep(10 * time.Millisecond)
				}
			}
			send(0, 300)
			wait(func() bool { return compressed.count() == 300 && plain.count() == 300 })
			// An idle interval followed by more input exercises nanotube's normal
			// connection refresh, without manufacturing ambiguous failed writes.
			time.Sleep(1100 * time.Millisecond)
			send(300, 600)
			wait(func() bool { return compressed.count() == 600 && plain.count() == 600 })
			// Keep writing through the receiver restart. All accepted sockets and any
			// unread compressed prefixes travel through real inherited descriptors.
			send(600, 750)
			h, e := old.Detach()
			if e != nil {
				t.Fatal(e)
			}
			if len(h.Conns) < min(connections, 2) {
				t.Fatalf("handoff connections=%d, expected at least %d", len(h.Conns), connections)
			}
			manifest := filepath.Join(root, "handoff.json")
			var tails [][]byte
			files := []*os.File{h.Listener}
			for _, c := range h.Conns {
				tails = append(tails, c.Leftover)
				files = append(files, c.File)
			}
			data, e := json.Marshal(tails)
			if e != nil {
				t.Fatal(e)
			}
			if e = os.WriteFile(manifest, data, 0600); e != nil {
				t.Fatal(e)
			}
			successor := exec.Command(os.Args[0], "-test.run=^TestZstdInheritedProcess$", "-test.timeout=30s")
			successor.Env = append(os.Environ(), "ZSTD_HANDOFF_MANIFEST="+manifest, "ZSTD_HANDOFF_ADDRESS="+old.Addr().String())
			successor.ExtraFiles = files
			in, e := successor.StdinPipe()
			if e != nil {
				t.Fatal(e)
			}
			out, e := successor.StdoutPipe()
			if e != nil {
				t.Fatal(e)
			}
			successor.Stderr = logFile
			if e = successor.Start(); e != nil {
				t.Fatal(e)
			}
			successorDone := false
			defer func() {
				if !successorDone {
					_ = successor.Process.Kill()
					_ = successor.Wait()
				}
			}()
			for _, f := range files {
				_ = f.Close()
			}
			reader := bufio.NewReader(out)
			ready, e := reader.ReadString('\n')
			if e != nil || strings.TrimSpace(ready) != "READY" {
				t.Fatalf("successor not ready: %q %v", ready, e)
			}
			var successorPoints atomic.Int64
			go func() {
				scan := bufio.NewScanner(reader)
				for scan.Scan() {
					if scan.Text() == "POINT" {
						successorPoints.Add(1)
					}
				}
			}()
			send(750, total)
			_ = input.Close()
			wait(func() bool { return plain.count() == total })
			if e = relay.Process.Signal(syscall.SIGTERM); e != nil {
				t.Fatal(e)
			}
			if e = relay.Wait(); e != nil {
				t.Fatal(e)
			}
			relayDone = true
			wait(func() bool { return plain.count() == total })
			// Wait for actual parser callbacks before stopping the successor.
			wait(func() bool { return int(successorPoints.Load())+compressed.count() == total })
			_ = in.Close()
			if e = successor.Wait(); e != nil {
				t.Fatal(e)
			}
			successorDone = true
			data, e = os.ReadFile(manifest + ".points")
			if e != nil {
				t.Fatal(e)
			}
			var after []*points.Points
			if e = json.Unmarshal(data, &after); e != nil {
				t.Fatal(e)
			}
			combined := append(append([]*points.Points(nil), compressed.points...), after...)
			verify := func(name string, got []*points.Points) {
				seen := map[int]int{}
				for _, p := range got {
					i, e := strconv.Atoi(strings.TrimPrefix(p.Metric, "example.point"))
					if e != nil || i < 0 || i >= total || len(p.Data) != 1 || p.Data[0].Value != float64(i)+0.25 || p.Data[0].Timestamp != 1700000000+int64(i) {
						t.Fatalf("%s: point changed", name)
					}
					seen[i]++
				}
				if len(got) != total || len(seen) != total {
					t.Fatalf("%s: outputs=%d distinct=%d expected=%d", name, len(got), len(seen), total)
				}
				for _, n := range seen {
					if n != 1 {
						t.Fatalf("%s: duplicate point", name)
					}
				}
			}
			verify("zstd across process handoff", combined)
			verify("plaintext override", plain.points)
			t.Logf("%d points exact once per host; %d compressed connections handed to successor; plaintext and zstd mixed; refresh reconnect verified", total, len(h.Conns))
			// Reject a compressed wire prefix accidentally routed into the plaintext path.
			if atomicCount := atomic.LoadUint64(&other.zstdStats.connections); atomicCount != 0 {
				t.Fatal("plaintext host was compressed")
			}
		})
	}
}
