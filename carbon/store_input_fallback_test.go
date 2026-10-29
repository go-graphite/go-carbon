package carbon

import (
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/internal/handoff"
	"github.com/go-graphite/go-carbon/persister"
	"github.com/go-graphite/go-carbon/points"
	"github.com/lomik/zapwriter"
)

// acceptLog records, per metric and timestamp, the last value a TCP receiver
// of any instance accepted, in the order the receiver passed them on, and when
// each instance last and first accepted a point.
type acceptLog struct {
	mu        sync.Mutex
	last      map[string]map[int64]float64
	instances atomic.Int32
	accepted  [2]atomic.Int64
	first     [2]time.Time
	latest    [2]time.Time
	corrected [2]map[string]bool
	changed   chan struct{}
}

func (l *acceptLog) record(instance int, metric string, value float64, ts int64) {
	now := time.Now()
	l.mu.Lock()
	m := l.last[metric]
	if m == nil {
		m = map[int64]float64{}
		l.last[metric] = m
	}
	m[ts] = value
	if l.first[instance].IsZero() {
		l.first[instance] = now
	}
	l.latest[instance] = now
	if value != 0 {
		l.corrected[instance][metric] = true
	}
	l.mu.Unlock()
	select {
	case l.changed <- struct{}{}:
	default:
	}
}

// install hooks every receiver started from now on. Each startReceivers call
// is one instance: the first is the stopping one, the second its successor.
func (l *acceptLog) install(t *testing.T) {
	l.last = map[string]map[int64]float64{}
	l.corrected = [2]map[string]bool{{}, {}}
	l.changed = make(chan struct{}, 1)
	receiverStoreHook = func(add func(*points.Points), addPoint func([]byte, float64, int64)) (func(*points.Points), func([]byte, float64, int64)) {
		instance := min(int(l.instances.Add(1))-1, 1)
		counter := &l.accepted[instance]
		return func(p *points.Points) {
				for _, pt := range p.Data {
					l.record(instance, p.Metric, pt.Value, pt.Timestamp)
				}
				counter.Add(int64(len(p.Data)))
				add(p)
			}, func(name []byte, value float64, ts int64) {
				l.record(instance, string(name), value, ts)
				counter.Add(1)
				addPoint(name, value, ts)
			}
	}
	t.Cleanup(func() { receiverStoreHook = nil })
}

// Wait for actual corrected traffic on every metric, rather than assuming a
// fixed delay let all clients reach the instance being exercised.
func (l *acceptLog) waitCorrections(t *testing.T, clients []*aggregatorClient, instance int) {
	t.Helper()
	timeout := time.NewTimer(10 * time.Second)
	defer timeout.Stop()
	for {
		l.mu.Lock()
		ready := true
		for _, client := range clients {
			for _, metric := range client.metrics {
				ready = ready && l.corrected[instance][metric]
			}
		}
		l.mu.Unlock()
		if ready {
			return
		}
		select {
		case <-l.changed:
		case <-timeout.C:
			t.Fatalf("instance %d did not accept corrections for every metric", instance)
		}
	}
}

func (l *acceptLog) snapshot(metric string) map[int64]float64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make(map[int64]float64, len(l.last[metric]))
	for ts, v := range l.last[metric] {
		out[ts] = v
	}
	return out
}

// gap is how long no instance accepted input across the restart.
func (l *acceptLog) gap() time.Duration {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.first[1].IsZero() || l.latest[0].IsZero() {
		return 0
	}
	return l.first[1].Sub(l.latest[0])
}

// aggregatorClient models the upstream aggregator of secondly counters: each
// interval it emits a group with 0 for every metric it owns at the next
// second, and the real value for the second lag groups earlier. Last write
// wins, so a stored 0 means the correction was lost or applied before the 0.
// Like the production sender it buffers at most buffer groups while it cannot
// write and drops newer groups beyond that; after a write fails it reconnects
// and writes that group again.
type aggregatorClient struct {
	metrics  []string
	base     int64
	lag      int
	interval time.Duration
	buffer   int
	// groups is how many seconds were emitted in full.
	groups      atomic.Int64
	dropped     atomic.Int64
	reconnects  atomic.Int64
	writeErrors atomic.Int64
}

func aggregatorValue(k, slot int) float64 { return float64(slot+1)*100 + float64(k+1) }

func (c *aggregatorClient) group(s int, finishing bool) []byte {
	var b strings.Builder
	if !finishing {
		for _, m := range c.metrics {
			fmt.Fprintf(&b, "%s 0 %d\n", m, c.base+int64(s))
		}
	}
	if slot := s - c.lag; slot >= 0 {
		for k, m := range c.metrics {
			fmt.Fprintf(&b, "%s %v %d\n", m, aggregatorValue(k, slot), c.base+int64(slot))
		}
	}
	return []byte(b.String())
}

// run emits groups until stop is closed, then the outstanding corrections,
// and returns once the server read everything written.
func (c *aggregatorClient) run(addr string, stop <-chan struct{}) error {
	queue := make(chan []byte, c.buffer)
	go func() {
		defer close(queue)
		ticker := time.NewTicker(c.interval)
		defer ticker.Stop()
		for s := 0; ; s++ {
			select {
			case <-stop:
				// The outstanding corrections are never dropped.
				for end := s + c.lag; s < end; s++ {
					queue <- c.group(s, true)
				}
				c.groups.Store(int64(s - c.lag))
				return
			case <-ticker.C:
			}
			select {
			case queue <- c.group(s, false):
			default:
				c.dropped.Add(1)
			}
		}
	}()
	dial := func() (net.Conn, error) {
		deadline := time.Now().Add(90 * time.Second)
		for {
			conn, err := net.Dial("tcp", addr)
			if err == nil {
				return conn, nil
			}
			if time.Now().After(deadline) {
				return nil, err
			}
			time.Sleep(5 * time.Millisecond)
		}
	}
	conn, err := dial()
	if err != nil {
		return err
	}
	defer func() {
		if conn != nil {
			_ = conn.Close()
		}
	}()
	for data := range queue {
		for {
			_ = conn.SetWriteDeadline(time.Now().Add(30 * time.Second))
			if _, err := conn.Write(data); err == nil {
				break
			}
			c.writeErrors.Add(1)
			_ = conn.Close()
			if conn, err = dial(); err != nil {
				return err
			}
			c.reconnects.Add(1)
		}
	}
	// Closing with unread data would reset: wait until the server read all.
	if tc, ok := conn.(*net.TCPConn); ok {
		_ = tc.CloseWrite()
	}
	_ = conn.SetReadDeadline(time.Now().Add(30 * time.Second))
	_, _ = conn.Read(make([]byte, 1))
	return nil
}

// readSeriesOrEmpty is readSeries for a metric that may not be indexed yet.
func readSeriesOrEmpty(t *testing.T, listen, target string, from, until int64) map[int64]float64 {
	t.Helper()
	url := fmt.Sprintf("http://%s/render/?target=%s&from=%d&until=%d&format=json", listen, target, from, until)
	resp, err := http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode == http.StatusNotFound {
		_ = resp.Body.Close()
		return map[int64]float64{}
	}
	return decodeSeries(t, resp)
}

// stopTimeline keeps the log lines of the stop and the handoff.
func stopTimeline(logs string) string {
	var b strings.Builder
	for _, line := range strings.Split(logs, "\n") {
		if strings.Contains(line, "[dump]") || strings.Contains(line, "[restart]") || strings.Contains(line, "input") {
			b.WriteString(line)
			b.WriteByte('\n')
		}
	}
	return b.String()
}

// fallbackCheck compares what is stored with what receivers accepted and with
// what clients meant to write.
type fallbackCheck struct {
	// Accepted points that are not stored as accepted, by cause.
	missing, zeroed, other int
	// Client-visible: seconds stored as 0 or missing that clients corrected.
	visibleZero, visibleMissing int
	checked                     int
}

func (f fallbackCheck) String() string {
	return fmt.Sprintf("accepted points checked %d: %d missing, %d read 0 instead of the accepted correction, %d other; client-visible: %d seconds read 0, %d missing",
		f.checked, f.missing, f.zeroed, f.other, f.visibleZero, f.visibleMissing)
}

func checkFallback(t *testing.T, listen string, log *acceptLog, clients []*aggregatorClient) fallbackCheck {
	t.Helper()
	var f fallbackCheck
	for _, c := range clients {
		groups := int(c.groups.Load())
		for k, m := range c.metrics {
			got := readSeriesOrEmpty(t, listen, m, c.base-1, c.base+int64(groups)+int64(c.lag)+1)
			for ts, want := range log.snapshot(m) {
				f.checked++
				v, ok := got[ts]
				switch {
				case !ok:
					f.missing++
				case v == want:
				case v == 0:
					f.zeroed++
				default:
					f.other++
				}
			}
			for s := 0; s < groups; s++ {
				v, ok := got[c.base+int64(s)]
				switch {
				case !ok:
					f.visibleMissing++
				case v == 0:
					f.visibleZero++
				case v != aggregatorValue(k, s):
					f.other++
				}
			}
		}
	}
	return f
}

// TestStoreInputFallbackKeepsEveryPoint replays the 2026-10-27 rollback of a
// shared store from a release with store input handoff to one without it.
// The stop's flush before input closes outlasted inputHandoffTimeout (25.6 s
// against 20 s) while input already went to the tail, so input stopped before
// the successor even started; the successor then loaded the whole tail before
// opening input, a 32 s gap, and the upstream aggregator, which buffers only
// so much, dropped the corrections of seconds whose 0 was already stored.
//
// An aggregator writes secondly counters as 0 followed by a correction through
// a stop with a slow flush, into successors that do and do not take the input.
// Every point a receiver accepted must be stored, a correction must never be
// overwritten by the 0 it corrects, a capable successor must take input over,
// and a successor that cannot take it must not see a gap the aggregator's
// buffer cannot bridge, so no second reads 0 or is missing.
func testStoreInputFallbackKeepsEveryPoint(t *testing.T) {
	for _, tc := range []struct {
		name string
		// successor: "old" behaves as a release without store input
		// handoff: it registers at once, asks for the read listener once
		// its reads open, does not understand the input offer and binds
		// normally. "silent" registers and never asks; "handoff" takes over.
		successor string
		// claimsSupport is what the binary the stopping instance checks
		// reports. A rollback through a service override starts another
		// binary than the one it was invoked as, so "old" may claim support.
		claimsSupport bool
		// serviceManager is whether the stopping instance learns the binary
		// the service manager starts next.
		serviceManager bool
		// bridged is whether the aggregator's buffer must bridge the gap.
		bridged bool
	}{
		{"store input handoff", "handoff", true, false, true},
		{"rollback to a release without store input handoff", "old", true, false, true},
		{"rollback seen in the service manager", "old", true, true, true},
		{"release without store input handoff", "old", false, false, true},
		{"capable successor never asks", "silent", true, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if isolatedTestProcess(t) {
				return
			}
			defer zapwriter.Test()()
			saved := inputHandoffTimeout
			// Preserve the 1.1 s executable-probe margin below while keeping the
			// slow flush beyond the handoff window.
			inputHandoffTimeout = 2 * time.Second
			t.Cleanup(func() { inputHandoffTimeout = saved })
			var accepted acceptLog
			accepted.install(t)

			root := t.TempDir()
			cfg := handoffTestConfig(t, root)
			// Every second the clients write stays within retention and in
			// the past, however long the restart takes.
			if err := os.WriteFile(cfg.Whisper.SchemasFilename, []byte("[all]\npattern = .*\nretentions = 1s:6h\n"), 0600); err != nil {
				t.Fatal(err)
			}
			schemas, err := persister.ReadWhisperSchemas(cfg.Whisper.SchemasFilename)
			if err != nil {
				t.Fatal(err)
			}
			cfg.Whisper.Schemas = schemas
			cfg.Whisper.StorageBackend = "pebble-chunk"
			cfg.Whisper.StoreEncoding = "exact"
			cfg.Whisper.StoreIncrementalMigration = true
			cfg.Whisper.FLock = true
			cfg.Whisper.StoreDir = t.TempDir()
			cfg.Whisper.StoreCacheSize = 8 << 20
			cfg.Whisper.StoreMemTableSize = 8 << 20
			cfg.Whisper.StoreMigrationWorkers = 1
			cfg.Whisper.StoreMigrationMinFreeBytes = 0
			cfg.Whisper.StoreMigrationBytesInFlight = 8 << 20
			cfg.Whisper.StoreMigrationControlFile = filepath.Join(root, "migration-control.json")
			cfg.Buckyd.Enabled = false
			if err := os.MkdirAll(cfg.Whisper.DataDir, 0750); err != nil {
				t.Fatal(err)
			}
			incrementalLease(t, cfg, 1, 0)
			cfg.Dump.StoreInputHandoff = true

			first := New(filepath.Join(root, "go-carbon.conf"))
			first.Config = cfg
			if err := first.Start(); err != nil {
				t.Fatal(err)
			}
			defer first.Stop()
			deadline := time.Now().Add(5 * time.Second)
			// Completed conversion uses the catalog index; input-only handoff
			// does not require a memory-mapped filesystem snapshot.
			for first.Carbonserver.CurrentFileIndex() == nil {
				if time.Now().After(deadline) {
					t.Fatal("initial index did not complete")
				}
				time.Sleep(10 * time.Millisecond)
			}
			// The flush the stop waits for before input closes takes longer
			// than inputHandoffTimeout, as on the production host.
			flusher := &slowFlusher{db: first.MetricStore, delay: inputHandoffTimeout + 500*time.Millisecond}
			savedFlush := stopFlushStore
			stopFlushStore = func(*store.Store) stopFlusher { return flusher }
			t.Cleanup(func() { stopFlushStore = savedFlush })

			next := New(filepath.Join(root, "go-carbon.conf"))
			nextConfig := *cfg
			// Capacity is not under test here. Leave room for the conservative
			// shard slack bound so every tail point uses the O(1) capacity check.
			nextConfig.Cache.MaxSize = handoffRecoveryCacheSize
			next.Config = &nextConfig
			defer next.Stop()
			nextStarted := make(chan error, 1)
			startNext := func(register bool) {
				go func() {
					if register {
						next.RegisterReadHandoff()
					}
					nextStarted <- next.Start()
				}()
			}
			oldBinary := storeHandoffSuccessor(t, false)
			var releaseOnce sync.Once
			release := func() {
				releaseOnce.Do(func() {
					switch tc.successor {
					case "handoff":
						startNext(true)
					case "old":
						// Registers right after parsing its config and
						// starts; its read listener request comes once its
						// reads open (1.77 s in production), here late
						// enough that waiting for it shows in the tail.
						requestAfter := inputHandoffTimeout * 3 / 4
						if conn, err := net.DialTimeout("unix", readHandoffPath(cfg), time.Second); err == nil {
							go func() {
								time.Sleep(requestAfter)
								_, _ = conn.Write([]byte{'R'})
								_ = conn.SetReadDeadline(time.Now().Add(10 * time.Second))
								_, _ = conn.Read(make([]byte, 1))
								_ = conn.Close()
							}()
						}
						startNext(false)
					case "silent":
						// Registers and never asks; it starts once the
						// stopping instance gave input up.
						if conn, err := net.DialTimeout("unix", readHandoffPath(cfg), time.Second); err == nil {
							t.Cleanup(func() { _ = conn.Close() })
						}
						startNext(false)
					}
				})
			}
			first.SetReadHandoff(storeHandoffSuccessor(t, tc.claimsSupport), release)
			if tc.serviceManager {
				first.SetSuccessorResolver(func() string { return oldBinary })
			}
			// What registers is this test process; the binary the successor
			// really runs is the stand-in for its release.
			registered := storeHandoffSuccessor(t, tc.successor != "old")
			savedPeer := handoff.PeerExecutable
			handoff.PeerExecutable = func(*net.UnixConn) (string, error) { return registered, nil }
			t.Cleanup(func() { handoff.PeerExecutable = savedPeer })

			// Two clients with two metrics cover independent connections and
			// corrections without making receiver throughput the restart gate.
			// Each client buffers 2.5 s of its output.
			const connections, perConnection, lag = 2, 2, 40
			const interval = 2 * time.Millisecond
			base := time.Now().Unix() - 5*3600
			var clients []*aggregatorClient
			stopClients := make(chan struct{})
			clientErrs := make(chan error, connections)
			for c := range connections {
				client := &aggregatorClient{base: base, lag: lag, interval: interval, buffer: int(2500 * time.Millisecond / interval)}
				for k := range perConnection {
					client.metrics = append(client.metrics, fmt.Sprintf("fallback.c%d.m%d", c, k))
				}
				clients = append(clients, client)
				go func() { clientErrs <- client.run(cfg.Tcp.Listen, stopClients) }()
			}
			accepted.waitCorrections(t, clients, 0)

			stopped := make(chan error, 1)
			go func() {
				err := first.DumpStop()
				if err == nil {
					first.ReleaseForHandoff()
					release()
				}
				stopped <- err
			}()
			select {
			case err := <-nextStarted:
				if err != nil {
					t.Fatal("next start", err, zapwriter.TestString())
				}
			// Under the race detector a successor replays a 3 s tail
			// (185k points) in about 40 s.
			case <-time.After(120 * time.Second):
				t.Fatal("next did not start", zapwriter.TestString())
			}
			if err := <-stopped; err != nil {
				t.Fatal("dump stop", err)
			}
			// Keep writing to the successor for a while, then let every
			// client write its outstanding corrections.
			accepted.waitCorrections(t, clients, 1)
			close(stopClients)
			for range connections {
				if err := <-clientErrs; err != nil {
					t.Fatal("client", err)
				}
			}
			logs := zapwriter.TestString()

			// Once every accepted point reads back as accepted, nothing
			// else can change: what clients meant to write but no receiver
			// accepted never arrives.
			var result fallbackCheck
			deadline = time.Now().Add(30 * time.Second)
			for {
				result = checkFallback(t, cfg.Carbonserver.Listen, &accepted, clients)
				if result.missing+result.zeroed+result.other == 0 || time.Now().After(deadline) {
					break
				}
				time.Sleep(200 * time.Millisecond)
			}
			var reconnects, dropped int64
			for _, c := range clients {
				reconnects += c.reconnects.Load()
				dropped += c.dropped.Load()
			}
			// The tail's counters, which production checks: every point
			// written to the tail is loaded by the successor, none lost.
			tailFor, tailPoints, tailLost, loaded := -1.0, int64(-1), int64(-1), int64(-1)
			if m := regexp.MustCompile(`input (?:stopped|handed over) .*"tail_for": ([0-9.e-]+), "tail_points": ([0-9]+), "tail_bytes": [0-9]+, "tail_lost_points": ([0-9]+)`).FindStringSubmatch(logs); m != nil {
				tailFor, _ = strconv.ParseFloat(m[1], 64)
				tailPoints, _ = strconv.ParseInt(m[2], 10, 64)
				tailLost, _ = strconv.ParseInt(m[3], 10, 64)
			}
			if m := regexp.MustCompile(`input tail loaded .*"points": ([0-9]+)`).FindStringSubmatch(logs); m != nil {
				loaded, _ = strconv.ParseInt(m[1], 10, 64)
			}
			if tailFor >= 0 && (tailPoints <= 0 || tailPoints != loaded || tailLost != 0) {
				t.Errorf("tail wrote %d points (%d lost), successor loaded %d", tailPoints, tailLost, loaded)
			}
			t.Logf("%s; accepted by stopping instance %d, by successor %d; input gap %v, input tail %.3f s (%d points); client reconnects %d, groups dropped %d",
				result, accepted.accepted[0].Load(), accepted.accepted[1].Load(), accepted.gap().Round(time.Millisecond), tailFor, tailPoints, reconnects, dropped)

			if result.missing+result.zeroed+result.other != 0 {
				t.Errorf("accepted points lost or out of order: %s", result)
			}
			if tc.successor == "handoff" {
				if !strings.Contains(logs, "input handed off to successor") {
					t.Error("input not handed over")
				}
				if reconnects != 0 {
					t.Errorf("senders reconnected %d times across a handoff", reconnects)
				}
			}
			if tc.bridged {
				if dropped != 0 {
					t.Errorf("input gap %v outlasted the senders' buffer: %d groups dropped", accepted.gap().Round(time.Millisecond), dropped)
				}
				// Closing a connection discards what its receiver had not
				// read yet, as in any restart: the client sees the reset on
				// a later write, reconnects and rewrites only that one.
				// Each lost group costs one second per metric. A receiver
				// writing to the tail lags the client by a few groups (up
				// to 4 seen on a loaded host); allow 10, 20 ms of output.
				// A gap the client's buffer must bridge shows as thousands.
				if lost := result.visibleZero + result.visibleMissing; int64(lost) > 10*reconnects*perConnection {
					t.Errorf("seconds read 0 or missing beyond in-flight writes per reconnect: %s", result)
				}
			}
			defer func() {
				if t.Failed() || os.Getenv("DEBUG_TIMELINE") != "" {
					t.Log(stopTimeline(logs))
				}
			}()
			switch {
			case tc.successor == "old" && tc.serviceManager:
				if tailFor >= 0 || !strings.Contains(logs, "service manager starts another binary next") {
					t.Errorf("input tail kept for a successor the service manager reports (tail %.3f s)", tailFor)
				}
			case tc.successor == "old" && tc.claimsSupport:
				// Input received meanwhile is loaded from the tail before
				// the successor reads input: give it up once it registers,
				// not when its request shows it cannot take input. The
				// margin covers running its binary, which on a loaded
				// macOS host took up to 1.1 s.
				if tailFor < 0 || tailFor > (inputHandoffTimeout*2/3).Seconds() {
					t.Errorf("input kept in the tail %.3f s for a successor that cannot take it", tailFor)
				}
				if !strings.Contains(logs, "successor cannot take input") {
					t.Error("registered successor's binary not checked")
				}
			case tc.successor == "silent":
				// A capable successor keeps its full window from release.
				if tailFor < inputHandoffTimeout.Seconds() || !strings.Contains(logs, "input handoff not taken") {
					t.Errorf("input tail %.3f s for a capable successor that never asks", tailFor)
				}
			}
		})
	}
}
