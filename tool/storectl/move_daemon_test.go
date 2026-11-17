package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/internal/sealrollup"
	"github.com/go-graphite/go-carbon/internal/v2core"
	"github.com/go-graphite/go-carbon/internal/v2daemon"
	"github.com/go-graphite/go-carbon/internal/v2part"
	"github.com/go-graphite/go-carbon/internal/v2recv"
	"github.com/go-graphite/go-carbon/points"
)

type moveAddresses struct{ TCP, HTTP string }
type moveSnapshot struct {
	Directory string
	Manifest  v2core.PartSnapshot
}

// Each child is a real running journal/core/TCP daemon on private ephemeral
// ports. Prepare/adopt/status use the same admin handler as the daemon listener.
func TestMoveDaemonProcess(t *testing.T) {
	root := os.Getenv("MOVE_DAEMON_ROOT")
	if root == "" {
		return
	}
	ctx := context.Background()
	count := 3
	if os.Getenv("MOVE_DAEMON_CRASH") == "1" {
		_ = os.WriteFile(filepath.Join(root, "target"), []byte("4"), 0600)
	}
	if _, err := os.Stat(filepath.Join(root, "target")); err == nil {
		count = 4
	}
	s, err := v2daemon.Open(root, v2daemon.Config{Layer: 3, Producers: count, Shards: count, GroupInterval: time.Second, Hooks: v2core.Hooks{Stage: func(stage string) {
		if cut := os.Getenv("MOVE_PROGRESS_CUT"); cut != "" && stage == cut {
			marker := filepath.Join(root, "cut-fired")
			if _, err := os.Stat(marker); os.IsNotExist(err) {
				_ = os.WriteFile(marker, []byte(stage), 0600)
				deadline := time.Now().Add(90 * time.Second)
				for time.Now().Before(deadline) {
					if _, e := os.Stat(filepath.Join(root, "cut-release")); e == nil {
						break
					}
					time.Sleep(10 * time.Millisecond)
				}
			}
		}

		if stage == "move-after-ingest" && os.Getenv("MOVE_DAEMON_CRASH") == "1" {
			if _, err := os.Stat(filepath.Join(root, "arm-crash")); err != nil {
				return
			}
			marker := filepath.Join(root, "adopt-committed")
			if _, err := os.Stat(marker); os.IsNotExist(err) {
				_ = os.WriteFile(marker, []byte(stage), 0600)
				select {}
			}
		}
	}}})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if count == 3 {
		today := time.Now().Unix() / 86400 * 86400
		orders := make([]uint64, count)
		for n := 0; n < 12; n++ {
			name := fmt.Sprintf("move.mature.%02d", n)
			producer := s.Producers[v2recv.OwnerFor(name, count)]
			// Restart only happens on B. These initial source/reference inputs
			// span nearly two years and have no retained raw at adoption.
			policy, err := v2core.EncodeRollupPolicy(v2core.RollupPolicy{Name: v2part.ID(name), Group: "test", Config: sealrollup.Config{Method: sealrollup.Sum, XFilesFactor: .5, Archives: []sealrollup.Archive{{Step: 1, Count: 7 * 86400}, {Step: 10, Count: 2 * 365 * 8640}}}})
			if err != nil {
				t.Fatal(err)
			}
			for _, age := range []int64{729, 365, 8} {
				old := today - age*86400
				for window := 0; window < 3; window++ {
					samples := []points.Point{}
					for i := 0; i < 5; i++ {
						samples = append(samples, points.Point{Timestamp: old + int64(window*10+i), Value: float64(n + window + i + 1)})
					}
					index := v2recv.OwnerFor(name, count)
					orders[index]++
					if err := producer.AppendApplications([]v2daemon.Application{{Name: name, Policy: policy, Order: orders[index], Clock: old + 30, Points: samples, Sequence: []uint64{1, 2, 3, 4, 5}}}); err != nil {
						t.Fatal(err)
					}
				}
			}
		}
		for _, p := range s.Producers {
			if err := p.Flush(); err != nil {
				t.Fatal(err)
			}
		}
	}
	address := os.Getenv("MOVE_DAEMON_TCP")
	if address == "" {
		address = "127.0.0.1:0"
	}
	l, err := net.Listen("tcp", address)
	if err != nil {
		t.Fatal(err)
	}
	receiver, err := s.ReceiveApplicationsTCP(l, v2recv.Config{SyncInterval: time.Second, BatchSize: 1}, func(name string) ([]byte, error) {
		return v2core.EncodeRollupPolicy(v2core.RollupPolicy{Name: v2part.ID(name), Group: "test", Config: sealrollup.Config{Method: sealrollup.Sum, XFilesFactor: .5, Archives: []sealrollup.Archive{{Step: 1, Count: 7 * 86400}, {Step: 10, Count: 2 * 365 * 8640}}}})
	})
	if err != nil {
		t.Fatal(err)
	}
	control, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	respond := func(w http.ResponseWriter, value any, err error) {
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		_ = json.NewEncoder(w).Encode(value)
	}
	flush := func() error {
		for i, p := range s.Producers {
			if err := receiver.OnOwner(i, func() error { return p.Flush() }); err != nil {
				return err
			}
		}
		return nil
	}
	mux.HandleFunc("/flush", func(w http.ResponseWriter, r *http.Request) { respond(w, true, flush()) })
	mux.HandleFunc("/stats", func(w http.ResponseWriter, r *http.Request) { respond(w, receiver.Stats(), nil) })
	mux.HandleFunc("/arm", func(w http.ResponseWriter, r *http.Request) {
		respond(w, true, os.WriteFile(filepath.Join(root, "arm-crash"), []byte("armed"), 0600))
	})
	mux.HandleFunc("/snapshot", func(w http.ResponseWriter, r *http.Request) {
		err := flush()
		var result moveSnapshot
		if err == nil {

			var readers moveSnapshotReaders
			for owner, db := range s.Core.DBs() {
				snapshot := db.NewSnapshot()
				defer snapshot.Close()
				readers = append(readers, v2core.SealedSnapshotReader{Snapshot: snapshot, Directory: filepath.Join(root, "placement-group-00000001", "core", fmt.Sprintf("owner-%03d", owner))})
			}
			result.Directory = filepath.Join(root, fmt.Sprintf("snapshot-%d", time.Now().UnixNano()))
			result.Manifest, err = v2core.LinkPartSnapshot(ctx, result.Directory, readers)
		}
		respond(w, result, err)
	})
	mux.Handle("/admin/v2/move/", v2daemon.NewMoveAdmin(root, s, receiver.OnOwner))
	mux.HandleFunc("/read", func(w http.ResponseWriter, r *http.Request) {
		out := map[string]map[string]uint64{}

		var err error
		for _, db := range s.Core.DBs() {
			snapshot := db.NewSnapshot()
			err = v2core.ScanRollups(snapshot, func(policy v2core.RollupPolicy, _ v2core.RollupMetadata, records []sealrollup.Record) error {
				if !strings.HasPrefix(string(policy.Name), "move.") {
					return nil
				}
				if _, exists := out[string(policy.Name)]; exists {
					return fmt.Errorf("name exposed by multiple receiving owners: %s", policy.Name)
				}
				values := map[string]uint64{}
				out[string(policy.Name)] = values
				for _, record := range records {
					a := policy.Config.Archives[record.Archive]
					if record.Sample.Time < time.Now().Unix()-a.Step*a.Count {
						continue
					}
					values[fmt.Sprintf("%d/%d", record.Archive, record.Sample.Time)] = record.Sample.Bits
				}
				return nil
			})
			snapshot.Close()
			if err != nil {
				break
			}
		}
		respond(w, out, err)
	})
	server := &http.Server{Handler: mux}
	go server.Serve(control)
	b, _ := json.Marshal(moveAddresses{l.Addr().String(), "http://" + control.Addr().String()})
	if err = os.WriteFile(filepath.Join(root, "addresses.json"), b, 0600); err != nil {
		t.Fatal(err)
	}
	select {}
}

type moveChild struct {
	root string
	cmd  *exec.Cmd
	addr moveAddresses
}

func startMoveChild(t *testing.T, root, tcp string, crash bool) *moveChild {
	t.Helper()
	_ = os.Remove(filepath.Join(root, "addresses.json"))
	cmd := exec.Command(os.Args[0], "-test.run=^TestMoveDaemonProcess$", "-test.timeout=4m")
	cmd.Env = append(os.Environ(), "MOVE_DAEMON_ROOT="+root, "MOVE_DAEMON_TCP="+tcp, "MOVE_DAEMON_CRASH="+fmt.Sprint(map[bool]int{true: 1, false: 0}[crash]))
	log, err := os.Create(filepath.Join(root, fmt.Sprintf("daemon-%d.log", time.Now().UnixNano())))
	if err != nil {
		t.Fatal(err)
	}
	cmd.Stdout = log
	cmd.Stderr = log
	if err = cmd.Start(); err != nil {
		t.Fatal(err)
	}
	_ = log.Close()
	c := &moveChild{root: root, cmd: cmd}
	t.Cleanup(func() {
		if t.Failed() && cmd.ProcessState == nil {
			// Dump only this test's own daemon, while the failed request is stalled.
			_ = cmd.Process.Signal(syscall.SIGQUIT)
			done := make(chan struct{})
			go func() { _ = cmd.Wait(); close(done) }()
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				_ = cmd.Process.Kill()
				<-done
			}
		} else {
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
		}
		if dir := os.Getenv("MOVE_DAEMON_EVIDENCE"); dir != "" {
			if data, err := os.ReadFile(log.Name()); err == nil {
				_ = os.WriteFile(filepath.Join(dir, fmt.Sprintf("daemon-%d.log", cmd.Process.Pid)), data, 0600)
			}
		}
	})
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		b, err := os.ReadFile(filepath.Join(root, "addresses.json"))
		if err == nil && json.Unmarshal(b, &c.addr) == nil {
			return c
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatal("daemon did not open ports")
	return nil
}

func moveGet(t *testing.T, base, path string, out any) {
	t.Helper()
	client := http.Client{Timeout: 30 * time.Second}
	response, err := client.Get(base + path)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != 200 {
		b, _ := io.ReadAll(response.Body)
		t.Fatalf("%s: %s", path, b)
	}
	if out != nil {
		if err = json.NewDecoder(response.Body).Decode(out); err != nil {
			t.Fatal(err)
		}
	}
}

func copyMoveWithKill(t *testing.T, snapshot moveSnapshot, target string) {
	t.Helper()
	if len(snapshot.Manifest.Parts) < 2 {
		t.Fatal("move snapshot has too few files")
	}
	var block atomic.Bool
	block.Store(true)
	var verifiedRequests atomic.Int64
	blocked := make(chan struct{}, 1)
	handler := partSnapshotHandler(snapshot.Directory, snapshot.Manifest, "")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/parts/"+snapshot.Manifest.Parts[0].ID {
			verifiedRequests.Add(1)
		}
		if block.Load() && r.URL.Path == "/parts/"+snapshot.Manifest.Parts[1].ID {
			w.Header().Set("Content-Length", fmt.Sprint(snapshot.Manifest.Parts[1].Bytes))
			_, _ = w.Write([]byte{0})
			w.(http.Flusher).Flush()
			select {
			case blocked <- struct{}{}:
			default:
			}
			<-r.Context().Done()
			return
		}
		handler.ServeHTTP(w, r)
	}))
	defer server.Close()
	cmd := exec.Command(os.Args[0], "-test.run=^TestPartReceiverProcess$", "-test.timeout=2m")
	cmd.Env = append(os.Environ(), "PART_COPY_CHILD=1", "PART_COPY_URL="+server.URL, "PART_COPY_DIR="+target)
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cmd.Process.Kill() }()
	select {
	case <-blocked:
	case <-time.After(30 * time.Second):
		t.Fatal("copier did not reach interrupted file")
	}
	if _, err := os.Stat(filepath.Join(target, snapshot.Manifest.Parts[0].ID+".part")); err != nil {
		t.Fatal("first verified file absent", err)
	}
	if err := cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	_ = cmd.Wait()
	block.Store(false)
	result, err := receivePartSnapshot(context.Background(), server.Client(), server.URL, "", target, 1)
	if err != nil || result.Skipped != 1 || verifiedRequests.Load() != 1 {
		t.Fatalf("copy SIGKILL resume=%+v requests=%d err=%v", result, verifiedRequests.Load(), err)
	}
}

func TestMoveOwnDaemonsDualWrite(t *testing.T) {
	ctx := context.Background()
	a := startMoveChild(t, t.TempDir(), "", false)
	b := startMoveChild(t, t.TempDir(), "", true)
	reference := startMoveChild(t, t.TempDir(), "", false)
	connect := func(address string) net.Conn {
		c, err := net.Dial("tcp", address)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { c.Close() })
		return c
	}
	ca, cb, cr := connect(a.addr.TCP), connect(b.addr.TCP), connect(reference.addr.TCP)
	base := time.Now().Unix() - 200
	write := func(c net.Conn, name string, value float64, ts int64) {
		if _, err := fmt.Fprintf(c, "%s %g %d\n", name, value, ts); err != nil {
			t.Fatal(err)
		}
	}
	// Different first names force donor and receiver numeric IDs to disagree.
	write(cb, "receiver.only", 1, base)
	write(ca, "move.history", 9, base)
	write(cr, "move.history", 9, base)
	for i := 0; i < 10; i++ {
		write(ca, "move.metric", float64(i), base+int64(i))
		write(cr, "move.metric", float64(i), base+int64(i))
	}
	for n := 0; n < 12; n++ {
		name := fmt.Sprintf("move.other.%02d", n)
		for i := 0; i < 10; i++ {
			write(ca, name, float64(n+i), base+int64(i))
			write(cr, name, float64(n+i), base+int64(i))
			if i >= 5 {
				write(cb, name, float64(n+i), base+int64(i))
			}
		}
	}
	var stop atomic.Bool
	var produced atomic.Int64
	produceErr := make(chan error, 1)
	var producer sync.WaitGroup
	producer.Add(1)
	go func() {
		defer producer.Done()
		for i := 10; !stop.Load(); i++ {
			line := fmt.Sprintf("move.metric %d %d\nmove.other.%02d %d %d\n", i, base+int64(i), i%12, i, base+int64(i))
			for _, conn := range []net.Conn{ca, cb, cr} {
				_, _ = io.WriteString(conn, line)
			}
			produced.Store(int64(i))
			if i%50 == 0 {
				// Bound queued work by the two continuously available daemons'
				// durable progress. Keep writing throughout copy/restart, with
				// at most 100 points queued between these explicit durable fences.
				// B may be killed at any point in this loop.
				client := http.Client{Timeout: 30 * time.Second}
				for _, child := range []*moveChild{a, reference} {
					response, err := client.Get(child.addr.HTTP + "/flush")
					if err != nil {
						produceErr <- err
						return
					}
					_, _ = io.Copy(io.Discard, response.Body)
					response.Body.Close()
					if response.StatusCode != http.StatusOK {
						produceErr <- fmt.Errorf("producer flush status %d", response.StatusCode)
						return
					}
				}
			}
			// Exercise concurrent intake without turning this correctness test
			// into an unbounded queue of tiny durable seals on a loaded laptop.
			time.Sleep(200 * time.Millisecond)
		}
	}()
	t.Cleanup(func() { stop.Store(true); producer.Wait() })
	for produced.Load() < 20 {
		time.Sleep(time.Millisecond)
	}
	waitAccepted := func(c *moveChild, want uint64) {
		t.Helper()
		deadline := time.Now().Add(30 * time.Second)
		for {
			var stats v2recv.Stats
			moveGet(t, c.addr.HTTP, "/stats", &stats)
			if stats.Accepted >= want {
				return
			}
			if time.Now().After(deadline) {
				t.Fatalf("intake did not reach %d: %+v", want, stats)
			}
			time.Sleep(time.Millisecond)
		}
	}
	// OnOwner serializes callbacks with intake, but can run ahead of input
	// still queued by the TCP reader. Fence the historical prefix explicitly.
	waitAccepted(a, 131+2*(20-9))
	var snapshot moveSnapshot
	moveGet(t, a.addr.HTTP, "/snapshot", &snapshot)
	target := t.TempDir()
	copyMoveWithKill(t, snapshot, target)
	moveAdopt(t, b.addr.HTTP, target)
	var initial map[string]map[string]uint64
	moveGet(t, b.addr.HTTP, "/read", &initial)
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("0/%d", base+int64(i))
		if value, ok := initial["move.metric"][key]; !ok || value != math.Float64bits(float64(i)) {
			t.Fatalf("live adoption missing historical slot %s: %v", key, initial)
		}
	}
	// Exercise the successful publication before arming the next attachment's
	// crash. Both TCP streams continue while this second snapshot is copied.
	before := produced.Load()
	for produced.Load() < before+10 {
		time.Sleep(time.Millisecond)
	}
	moveGet(t, a.addr.HTTP, "/snapshot", &snapshot)
	crashCopy := httptest.NewServer(partSnapshotHandler(snapshot.Directory, snapshot.Manifest, ""))
	defer crashCopy.Close()
	if _, err := receivePartSnapshot(ctx, crashCopy.Client(), crashCopy.URL, "", target, 1); err != nil {
		t.Fatal(err)
	}
	crashMoveID := movePrepare(t, b.addr.HTTP, target)
	moveGet(t, b.addr.HTTP, "/arm", nil)
	// Kill the actual receiving daemon after its atomic manifest edit, before
	// reducer publication or the administrative response can complete.
	adoptDone := make(chan struct{})
	var adoptErr error
	go func() {
		defer close(adoptDone)
		var output bytes.Buffer
		adoptErr = moveAdmin([]string{"--url", b.addr.HTTP, "--action", "adopt", "--id", crashMoveID}, &output)
	}()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(filepath.Join(b.root, "adopt-committed")); err == nil {
			break
		}
		select {
		case <-adoptDone:
			t.Fatalf("adopt returned before crash boundary: %v", adoptErr)
		default:
		}
		time.Sleep(10 * time.Millisecond)
	}
	if _, err := os.Stat(filepath.Join(b.root, "adopt-committed")); err != nil {
		t.Fatal("adoption did not reach commit")
	}
	if err := b.cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	_ = b.cmd.Wait()
	<-adoptDone
	oldTCP := b.addr.TCP
	b = startMoveChild(t, b.root, oldTCP, false)
	var status bytes.Buffer
	if err := moveAdmin([]string{"--url", b.addr.HTTP, "--action", "status", "--id", crashMoveID}, &status); err != nil {
		t.Fatal(err)
	}
	var resumed v2daemon.MoveStatus
	if err := json.Unmarshal(status.Bytes(), &resumed); err != nil || resumed.Done {
		t.Fatalf("partial owner activation marked complete: %s %v", status.String(), err)
	}
	status.Reset()
	if err := moveAdmin([]string{"--url", b.addr.HTTP, "--action", "adopt", "--id", crashMoveID}, &status); err != nil {
		t.Fatal(err)
	}
	// Keep A and the reference writing during receiver death/replay. A final
	// incremental pass repairs the fresh slots that B missed during that time.
	stop.Store(true)
	producer.Wait()
	select {
	case err := <-produceErr:
		t.Fatal(err)
	default:
	}
	cb.Close()
	cb = connect(b.addr.TCP)
	last := base + produced.Load() + 1
	write(cb, "move.metric", 4, last)
	write(cr, "move.metric", 4, last)
	write(ca, "move.metric", 3, last)
	for _, c := range []*moveChild{a, reference, b} {
		want := uint64(131 + 2*(produced.Load()-9) + 1)
		if c == b {
			want = 1
		}
		waitAccepted(c, want)
	}
	moveGet(t, b.addr.HTTP, "/flush", nil)
	moveGet(t, reference.addr.HTTP, "/flush", nil)
	moveGet(t, a.addr.HTTP, "/snapshot", &snapshot)
	server2 := httptest.NewServer(partSnapshotHandler(snapshot.Directory, snapshot.Manifest, ""))
	defer server2.Close()
	copyResult, err := receivePartSnapshot(ctx, server2.Client(), server2.URL, "", target, 1)
	if err != nil {
		t.Fatal(err)
	}
	if copyResult.Skipped == 0 {
		t.Fatal("incremental copy resent every verified part")
	}
	moveAdopt(t, b.addr.HTTP, target)
	// Retry must skip the verified physical files, including after daemon death.
	retry, err := receivePartSnapshot(ctx, server2.Client(), server2.URL, "", target, 1)
	if err != nil || retry.Bytes != 0 || retry.Skipped != len(snapshot.Manifest.Parts) {
		t.Fatalf("retry %+v %v", retry, err)
	}
	var got, want map[string]map[string]uint64
	moveGet(t, b.addr.HTTP, "/read", &got)
	moveGet(t, reference.addr.HTTP, "/read", &want)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("moved/reference mismatch\ngot %v\nwant %v", got, want)
	}
	if got["move.metric"][fmt.Sprintf("0/%d", last)] != math.Float64bits(4) {
		t.Fatal("late A copy rolled back B-only input")
	}
	total := 0
	for _, values := range got {
		total += len(values)
	}
	for n := 0; n < 12; n++ {
		values := got[fmt.Sprintf("move.mature.%02d", n)]
		if len(values) != 9 {
			t.Fatalf("mature history expected nine coarse windows and no raw: %v", values)
		}
		for key := range values {
			if !strings.HasPrefix(key, "1/") {
				t.Fatalf("expired mature raw visible: %s", key)
			}
		}
	}
	t.Logf("exact slots=%d names=%d source producers/owners=3 target=4 raw=7d coarse=2y mature=8/365/729d incremental copied=%d skipped=%d; copier/receiver SIGKILL and move-level resume passed", total, len(got), copyResult.Parts, copyResult.Skipped)
}

func movePrepare(t *testing.T, base, dir string) string {
	t.Helper()
	var output bytes.Buffer
	if err := moveAdmin([]string{"--url", base, "--action", "prepare", "--parts-dir", dir}, &output); err != nil {
		t.Fatal(err)
	}
	var status v2daemon.MoveStatus
	if err := json.Unmarshal(output.Bytes(), &status); err != nil {
		t.Fatal(err)
	}
	return status.ID
}
func moveAdopt(t *testing.T, base, dir string) {
	t.Helper()
	id := movePrepare(t, base, dir)
	var output bytes.Buffer
	if err := moveAdmin([]string{"--url", base, "--action", "adopt", "--id", id}, &output); err != nil {
		t.Fatal(err)
	}
	output.Reset()
	if err := moveAdmin([]string{"--url", base, "--action", "status", "--id", id}, &output); err != nil {
		t.Fatal(err)
	}
	var status v2daemon.MoveStatus
	if err := json.Unmarshal(output.Bytes(), &status); err != nil || !status.Done {
		t.Fatalf("move status: %s %v", output.String(), err)
	}
}

type moveSnapshotReaders []v2core.SealedSnapshotReader

func (readers moveSnapshotReaders) ReadPartFiles(ctx context.Context) ([]v2core.SnapshotPart, error) {
	var out []v2core.SnapshotPart
	for _, reader := range readers {
		files, err := reader.ReadPartFiles(ctx)
		if err != nil {
			return nil, err
		}
		out = append(out, files...)
	}
	return out, nil
}
