//go:build unix

package carbon

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/signal"
	"path/filepath"
	"runtime/pprof"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/internal/handoff"
	"github.com/go-graphite/go-carbon/internal/whisperio"
	"github.com/go-graphite/go-carbon/receiver/stream/codec"
	"github.com/go-graphite/go-carbon/receiver/tcp"
	"github.com/klauspost/compress/zstd"
	"github.com/lomik/zapwriter"
)

// Opt-in real App/real TCP experiment. Each relay owns an 800,000-record
// nonblocking queue. The offered clock is independent of socket progress.
// No receiver callbacks are replaced: stream retains its cached-name path.
// Handoff uses the real descriptor/tail protocol, with the existing executable
// capability stub because both App generations run in this isolated process.
func TestZeroMissedWritesRelayGate(t *testing.T) {
	mode := os.Getenv("ZERO_CODEC")
	if mode == "" {
		t.Skip("set ZERO_CODEC=plaintext|zstd|stream")
	}
	if mode != "plaintext" && mode != "zstd" && mode != "stream" {
		t.Fatal("unknown codec")
	}
	if isolatedTestProcessSerial(t) {
		return
	}
	defer zapwriter.Test()()
	number := func(key string, fallback int) int {
		if s := os.Getenv(key); s != "" {
			n, e := strconv.Atoi(s)
			if e != nil || n < 1 {
				t.Fatal(key, s)
			}
			return n
		}
		return fallback
	}
	relays, rate, seconds := number("ZERO_RELAYS", 1), number("ZERO_RATE", 160000), number("ZERO_SECONDS", 12)
	names := number("ZERO_NAMES", 1024)
	nameBytes := number("ZERO_NAME_BYTES", 1)
	streamMaxNames := number("ZERO_STREAM_MAX_NAMES", codec.DefaultMaxNames)
	tailHold, tailHoldErr := time.ParseDuration(os.Getenv("ZERO_TAIL_HOLD"))
	if os.Getenv("ZERO_TAIL_HOLD") != "" && (tailHoldErr != nil || tailHold < 0 || tailHold > 8*time.Second) {
		t.Fatal("ZERO_TAIL_HOLD must be between 0s and 8s", tailHoldErr)
	}
	if nameBytes > 2048 {
		t.Fatal("fixture metric name limit exceeded")
	}
	reclaim := os.Getenv("ZERO_RECLAIM") == "1"
	pause, pauseErr := time.ParseDuration(os.Getenv("ZERO_SENDER_PAUSE"))
	if os.Getenv("ZERO_SENDER_PAUSE") != "" && pauseErr != nil {
		t.Fatal(pauseErr)
	}
	const batch = 256
	const queueRecords = 800000
	if relays > 12 || seconds > 60 || rate > 2000000 {
		t.Fatal("fixture envelope exceeded")
	}
	root := t.TempDir()
	cfg := handoffTestConfig(t, root)
	cfg.Common.MaxCPU = number("ZERO_CPUS", 8)
	cfg.Whisper.StorageBackend = "pebble-chunk"
	cfg.Whisper.StoreEncoding = "exact"
	cfg.Whisper.StoreIncrementalMigration = true
	cfg.Whisper.StoreDir = filepath.Join(root, "shared")
	cfg.Whisper.DataDir = filepath.Join(root, "files")
	cfg.Whisper.FLock = true
	if err := os.MkdirAll(cfg.Whisper.DataDir, 0750); err != nil {
		t.Fatal(err)
	}
	cfg.Whisper.StoreCacheSize = 16 << 20
	cfg.Whisper.StoreMemTableSize = 8 << 20
	cfg.Whisper.StoreMetadataCacheSize = 64 << 20
	cfg.Whisper.StoreMetadataCachePrewarm = reclaim
	cfg.Whisper.StoreSyncInterval = &Duration{time.Second}
	cfg.Whisper.StoreTimeBuckets = true
	cfg.Whisper.StoreTimeBucketsRelayoutMinFree = 1
	cfg.Whisper.StoreTimeBucketsRelayoutMaxDebt = 1 << 40
	cfg.Whisper.StoreTimeBucketsRelayoutReclaimBytes = 4 << 20
	cfg.Whisper.StoreCompactionRate = 2 << 20
	if reclaim {
		cfg.Whisper.StoreCompactionRate = 64 << 10
	}
	cfg.Whisper.StoreTimeBucketsRelayoutRate = 64 << 20
	cfg.Whisper.MaxUpdatesPerSecond = 20000
	cfg.Whisper.Workers = 4
	cfg.Cache.MaxSize = 100000000
	cfg.Cache.WriteoutMinPoints = 960
	cfg.Cache.WriteoutMaxDelay = Duration{960 * time.Second}
	cfg.Dump.Format = "blocks"
	cfg.Dump.StoreInputHandoff = true
	if os.Getenv("ZERO_READ_AHEAD_BYTES") != "" {
		cfg.Tcp.HandoffReadAheadBytes = number("ZERO_READ_AHEAD_BYTES", 8<<20)
	}
	cfg.InputLog.Enabled = os.Getenv("ZERO_INPUT_LOG") != "0"
	cfg.InputLog.Path = filepath.Join(root, "input-log")
	cfg.InputLog.SegmentSize = 16 << 20
	cfg.InputLog.SyncInterval = Duration{time.Second}
	cfg.InputLog.MaxAge = Duration{time.Hour}
	cfg.Buckyd.Enabled = false
	missingSecret := os.Getenv("ZERO_MISSING_JWT") == "1"
	if missingSecret {
		cfg.Buckyd.Enabled = true
		cfg.Buckyd.Bind = "127.0.0.1:0"
		cfg.Buckyd.JWTSecretFile = filepath.Join(root, "late-buckyd.secret")
		cfg.Buckyd.WaitForJWTSecret = true
	}
	cfg.Carbonserver.ScanFrequency = &Duration{time.Hour}
	horizon := max(3600, rate*seconds/names+120)
	if err := os.WriteFile(cfg.Whisper.SchemasFilename, []byte(fmt.Sprintf("[all]\npattern = .*\nretentions = 1s:%ds\n", horizon+120)), 0600); err != nil {
		t.Fatal(err)
	}
	if reclaim {
		seedReclaimGate(t, cfg)
	}
	path := filepath.Join(root, "go-carbon.conf")
	writeSharedConfig(t, path, cfg)
	first := New(path)
	if err := first.ParseConfig(); err != nil {
		t.Fatal(err)
	}
	cfg = startHandoffTestApp(t, first)
	defer first.Stop()
	checkMissingSecret := func(app *App) {
		t.Helper()
		w := httptest.NewRecorder()
		app.Buckyd.Handler().ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/metrics", http.NoBody))
		if w.Code != http.StatusServiceUnavailable || app.Buckyd.AuthSecretReady() {
			t.Fatal("missing management secret must return 503 while input remains available", w.Code)
		}
	}
	if missingSecret {
		checkMissingSecret(first)
	}
	firstCache, firstLog := first.Cache, first.inputLog.Load()
	metricNames := make([][]string, relays)
	for r := range metricNames {
		metricNames[r] = make([]string, names)
		for m := range names {
			n := fmt.Sprintf("gate.relay%02d.metric%04d", r, m)
			// Stable, distinct long names model the wire bytes of busy stores.
			// Generate outside the offer clock; avoid a compressible padding run.
			for len(n) < nameBytes {
				sum := sha256.Sum256([]byte(n))
				n += fmt.Sprintf(".%x", sum)
			}
			metricNames[r][m] = n
		}
	}
	name := func(r, m int) string { return metricNames[r][m] }
	// Production restarts receive established series. Create their catalog
	// entries before the timed offer, including the trailing sentinels: startup
	// discovery may be gated while recovery drains, independently of intake.
	for r := range relays {
		for m := 0; m <= names; m++ {
			metric := fmt.Sprintf("gate.sentinel%02d", r)
			if m < names {
				metric = name(r, m)
			}
			if _, err := first.MetricStore.Create(context.Background(), store.MetricConfig{
				Name: metric, Retentions: []store.Retention{{Step: 1, Count: horizon + 120}},
				AggregationMethod: store.Average, XFilesFactor: .5,
			}); err != nil {
				t.Fatal(err)
			}
		}
	}
	first.Carbonserver.RequestFileScan()
	indexDeadline := time.Now().Add(10 * time.Second)
	for r := range relays {
		for m := 0; m <= names; m++ {
			metric := fmt.Sprintf("gate.sentinel%02d", r)
			if m < names {
				metric = name(r, m)
			}
			for !first.Carbonserver.MetricExists(metric) {
				if time.Now().After(indexDeadline) {
					t.Fatal("fixture catalog did not become visible", metric)
				}
				first.Carbonserver.RequestFileScan()
				time.Sleep(10 * time.Millisecond)
			}
		}
	}
	base := time.Now().Unix() - int64(horizon) + 60
	type span struct{ first, n int }
	type relay struct {
		conn                       net.Conn
		q                          chan span
		dropped                    []bool
		offered, sent, drops, high atomic.Int64
		done                       chan error
	}
	rs := make([]*relay, relays)
	total := rate * seconds / batch * batch
	var consumers sync.WaitGroup
	for r := range rs {
		metricNames := make([][]byte, names)
		for m := range metricNames {
			metricNames[m] = []byte(name(r, m))
		}
		conn, err := net.Dial("tcp", cfg.Tcp.Listen)
		if err != nil {
			t.Fatal(err)
		}
		_ = conn.(*net.TCPConn).SetWriteBuffer(32 << 10)
		s := &relay{conn: conn, q: make(chan span, queueRecords/batch), dropped: make([]bool, total), done: make(chan error, 1)}
		rs[r] = s
		t.Cleanup(func() { _ = conn.Close() })
		consumers.Go(func() {
			var encoder *codec.Encoder
			var compressor *zstd.Encoder
			var err error
			if mode == "stream" {
				encoder, err = codec.NewEncoder(codec.Options{MaxNames: streamMaxNames})
				if err == nil {
					var pre []byte
					pre, err = codec.AppendPreamble(nil, 0)
					if err == nil {
						_, err = conn.Write(pre)
					}
				}
				if encoder != nil {
					defer encoder.Close()
				}
			}
			if mode == "zstd" {
				compressor, err = zstd.NewWriter(nil, zstd.WithEncoderConcurrency(1))
				if compressor != nil {
					defer compressor.Close()
				}
			}
			if err != nil {
				s.done <- err
				return
			}
			var wire, compressed []byte
			if pause > 0 {
				time.Sleep(pause)
			}
			write := func(metric []byte, v float64, ts int64) {
				if mode == "stream" {
					if err == nil {
						err = encoder.Add(metric, v, ts)
					}
				} else {
					wire = append(wire, metric...)
					wire = append(wire, ' ')
					wire = strconv.AppendFloat(wire, v, 'f', -1, 64)
					wire = append(wire, ' ')
					wire = strconv.AppendInt(wire, ts, 10)
					wire = append(wire, '\n')
				}
			}
			flush := func() {
				if mode == "stream" {
					wire, err = encoder.Flush(wire[:0])
				}
				if err != nil {
					return
				}
				payload := wire
				if compressor != nil {
					compressed = compressor.EncodeAll(wire, compressed[:0])
					payload = compressed
				}
				_ = conn.SetWriteDeadline(time.Now().Add(15 * time.Second))
				_, err = conn.Write(payload)
				wire = wire[:0]
			}
			for p := range s.q {
				for seq := p.first; seq < p.first+p.n; seq++ {
					write(metricNames[seq%names], float64(seq), base+int64(seq/names))
				}
				flush()
				if err != nil {
					s.done <- err
					return
				}
				s.sent.Add(int64(p.n))
			}
			write([]byte(fmt.Sprintf("gate.sentinel%02d", r)), float64(total), time.Now().Unix()-1)
			flush()
			s.done <- err
		})
	}
	// Keep a low-rate probe flowing after the finite offer finishes, so reload
	// liveness is measured even if a slow local handoff outlasts that offer.
	probe, err := net.Dial("tcp", cfg.Tcp.Listen)
	if err != nil {
		t.Fatal(err)
	}
	probeStop, probeDone := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(probeDone)
		ticker := time.NewTicker(5 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-probeStop:
				return
			case <-ticker.C:
				_ = probe.SetWriteDeadline(time.Now().Add(2 * time.Second))
				if _, err := fmt.Fprintf(probe, "gate.reload_probe 1 %d\n", time.Now().Unix()-1); err != nil {
					return
				}
			}
		}
	}()
	defer func() { close(probeStop); _ = probe.Close(); <-probeDone }()
	// Sample actual receiver counters, without wrapping the live cache sink.
	var sampleMu sync.Mutex
	receivers := []*tcp.TCP{first.Receivers[0].Receiver.(*tcp.TCP)}
	var received atomic.Int64
	var maxPause atomic.Int64
	sampleStop, sampleDone := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(sampleDone)
		timer := time.NewTicker(10 * time.Millisecond)
		defer timer.Stop()
		last := time.Now()
		for {
			select {
			case <-sampleStop:
				return
			case <-timer.C:
				sampleMu.Lock()
				delta := int64(0)
				for _, r := range receivers {
					r.Stat(func(k string, v float64) {
						if k == "metricsReceived" {
							delta += int64(v)
						}
					})
				}
				sampleMu.Unlock()
				if delta > 0 {
					received.Add(delta)
					d := int64(time.Since(last))
					for old := maxPause.Load(); d > old; old = maxPause.Load() {
						if maxPause.CompareAndSwap(old, d) {
							break
						}
					}
					last = time.Now()
				}
			}
		}
	}()
	defer func() { close(sampleStop); <-sampleDone }()
	started := time.Now()
	var receivedAtDeadline, sentAtDeadline, offeredAtDeadline atomic.Int64
	deadlineDone := make(chan struct{})
	go func() {
		defer close(deadlineDone)
		timer := time.NewTimer(time.Duration(seconds) * time.Second)
		defer timer.Stop()
		<-timer.C
		receivedAtDeadline.Store(received.Load())
		for _, s := range rs {
			sentAtDeadline.Add(s.sent.Load())
			offeredAtDeadline.Add(s.offered.Load())
		}
	}()
	var producers sync.WaitGroup
	for _, s := range rs {
		producers.Go(func() {
			defer close(s.q)
			ticker := time.NewTicker(time.Millisecond)
			defer ticker.Stop()
			next := 0
			for at := range ticker.C {
				target := min(total, int(at.Sub(started).Seconds()*float64(rate))/batch*batch)
				for next < target {
					s.offered.Add(batch)
					select {
					case s.q <- span{next, batch}:
					default:
						s.drops.Add(batch)
						for i := next; i < next+batch; i++ {
							s.dropped[i] = true
						}
					}
					if high := int64(len(s.q) * batch); high > s.high.Load() {
						s.high.Store(high)
					}
					next += batch
				}
				if next == total {
					return
				}
			}
		})
	}
	// Deliver a real SIGHUP to this isolated test process. The handler calls the
	// same production entrypoint as main; no production process is addressed.
	hup := make(chan os.Signal, 1)
	signal.Notify(hup, syscall.SIGHUP)
	defer signal.Stop(hup)
	reload := func(app *App) time.Duration {
		before := received.Load()
		beforeTime := time.Now()
		_ = syscall.Kill(os.Getpid(), syscall.SIGHUP)
		select {
		case <-hup:
		case <-time.After(time.Second):
			t.Fatal("SIGHUP not delivered")
		}
		updated := *cfg
		updated.Cache.MaxSize = app.Config.Cache.MaxSize - 1024
		updated.Cache.WriteoutMinPoints = 961
		updated.Cache.WriteoutMaxDelay = Duration{959 * time.Second}
		updated.Whisper.MaxUpdatesPerSecond = 19000
		if app.Config.Cache.WriteoutMinPoints == 961 {
			updated.Cache.WriteoutMinPoints = 960
			updated.Cache.WriteoutMaxDelay = Duration{960 * time.Second}
		}
		if app.Config.Whisper.MaxUpdatesPerSecond == 19000 {
			updated.Whisper.MaxUpdatesPerSecond = 20000
		}
		writeSharedConfig(t, path, &updated)
		if err := app.ReloadConfig(); err != nil {
			t.Fatal(err)
		}
		for received.Load() == before {
			if time.Since(beforeTime) > time.Second {
				t.Fatal("reload paused receive over 1s", before, received.Load(), zapwriter.TestString())
			}
			time.Sleep(time.Millisecond)
		}
		elapsed := time.Since(beforeTime)
		if elapsed >= time.Second {
			t.Fatal("reload and receive progress exceeded the conservative 1s gate", elapsed)
		}
		return elapsed
	}
	time.Sleep(time.Duration(seconds) * time.Second / 6)
	reloadBefore := reload(first)
	next := New(path)
	nextCfg := *cfg
	next.Config = &nextCfg
	defer next.Stop()
	nextStarted := make(chan error, 1)
	successor := storeHandoffSuccessor(t, true)
	first.SetReadHandoff(successor, func() {
		go func() {
			next.RegisterReadHandoff()
			nextStarted <- next.Start()
		}()
	})
	savedPeer := handoff.PeerExecutable
	handoff.PeerExecutable = func(*net.UnixConn) (string, error) { return successor, nil }
	defer func() { handoff.PeerExecutable = savedPeer }()
	var reclaimBefore store.RelayoutStats
	var compacting int64
	var reloadInFlight time.Duration
	stackConfirmed := false
	if reclaim {
		// Release the normal restart maintenance gate for this experiment;
		// production keeps its existing warming/backlog admission policy.
		gate := first.storeMaintenance
		if gate == nil {
			t.Fatal("reclaim experiment needs its maintenance gate")
		}
		gate.once.Do(func() { gate.reason = "local reclaim experiment"; close(gate.done) })
		deadline := time.Now().Add(15 * time.Second)
		for {
			reclaimBefore = first.MetricStore.RelayoutStats()
			compacting = first.MetricStore.Stats().CompactionsInProgress
			if reclaimBefore.UnreclaimedBytes > 0 && compacting > 0 && reclaimCompactInFlight() {
				stackConfirmed = true
				break
			}
			if time.Now().After(deadline) {
				t.Fatal("reclaim never entered", reclaimBefore, zapwriter.TestString())
			}
			time.Sleep(time.Millisecond)
		}
		reloadInFlight = reload(first)
		reclaimBefore = first.MetricStore.RelayoutStats()
		compacting = first.MetricStore.Stats().CompactionsInProgress
		if reclaimBefore.UnreclaimedBytes == 0 || compacting == 0 || !reclaimCompactInFlight() {
			t.Fatal("reclaim ended before restart; in-flight stop was not exercised")
		}
	} else {
		time.Sleep(time.Duration(seconds) * time.Second / 6)
	}
	stopAt := time.Now()
	if tailHold > 0 {
		// An earlier empty tail, locked by this fixture, holds the real ordered
		// replay after inherited handlers and read-ahead start. The sender keeps
		// offering independently, just as when production replays a large tail.
		tailPath := filepath.Join(cfg.Dump.Path, "input.0.0.tail.bin")
		if err := os.WriteFile(tailPath, nil, 0600); err != nil {
			t.Fatal(err)
		}
		unlock, err := handoff.LockExclusive(tailPath)
		if err != nil {
			t.Fatal(err)
		}
		release := sync.OnceFunc(func() { _ = unlock() })
		cancel, done := make(chan struct{}), make(chan struct{})
		t.Cleanup(func() { close(cancel); release(); <-done })
		go func() {
			defer close(done)
			deadline := time.Now().Add(15 * time.Second)
			for !strings.Contains(zapwriter.TestString(), "took over input from stopping instance") && time.Now().Before(deadline) {
				select {
				case <-cancel:
					return
				case <-time.After(time.Millisecond):
				}
			}
			select {
			case <-cancel:
			case <-time.After(tailHold):
			}
			release()
		}()
	}
	stopped := make(chan error, 1)
	go func() {
		err := first.DumpStop()
		if err == nil {
			first.ReleaseForHandoff()
		}
		stopped <- err
	}()
	select {
	case err := <-nextStarted:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("handoff timed out", zapwriter.TestString())
	}
	if err := <-stopped; err != nil {
		t.Fatal(err)
	}
	if missingSecret {
		checkMissingSecret(next)
		if err := os.WriteFile(cfg.Buckyd.JWTSecretFile, []byte("fixture-secret"), 0600); err != nil {
			t.Fatal(err)
		}
		w := httptest.NewRecorder()
		next.Buckyd.Handler().ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/metrics", http.NoBody))
		if w.Code != http.StatusForbidden || !next.Buckyd.AuthSecretReady() {
			t.Fatal("delivered management secret must enable authentication", w.Code)
		}
	}
	stopTime := time.Since(stopAt)
	sampleMu.Lock()
	receivers = append(receivers, next.Receivers[0].Receiver.(*tcp.TCP))
	sampleMu.Unlock()
	writeSharedConfig(t, path, &nextCfg)
	reloadAfter := reload(next)
	producers.Wait()
	consumers.Wait()
	for _, s := range rs {
		if err := <-s.done; err != nil {
			t.Fatal(err)
		}
	}
	// A sentinel after the final encoded frame proves input has reached the
	// normal cache/store merge. Query every non-dropped identity, including tail.
	for r := range rs {
		deadline := time.Now().Add(10 * time.Second)
		for {
			metric := fmt.Sprintf("gate.sentinel%02d", r)
			if !next.Carbonserver.MetricExists(metric) {
				if time.Now().After(deadline) {
					t.Fatal("sentinel not indexed", r)
				}
				next.Carbonserver.RequestFileScan()
				time.Sleep(10 * time.Millisecond)
				continue
			}
			got := readSeries(t, cfg.Carbonserver.Listen, metric, time.Now().Unix()-10, time.Now().Unix()+2)
			if len(got) > 0 {
				break
			}
			if time.Now().After(deadline) {
				t.Fatal("sentinel missing", r)
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
	for _, s := range rs {
		_ = s.conn.Close()
	}
	missing, corrupt, checked := 0, 0, 0
	// New names can overflow the deliberately small realtime-index queue in
	// the positive control. Settle discovery before using render as an oracle;
	// this does not change receive, queue admission or the expected identities.
	next.Carbonserver.RequestFileScan()
	for r, s := range rs {
		for m := range names {
			deadline := time.Now().Add(10 * time.Second)
			for !next.Carbonserver.MetricExists(name(r, m)) {
				if time.Now().After(deadline) {
					t.Fatal("oracle name never indexed", name(r, m))
				}
				next.Carbonserver.RequestFileScan()
				time.Sleep(10 * time.Millisecond)
			}
			got := readSeries(t, cfg.Carbonserver.Listen, name(r, m), base-1, base+int64(total/names)+2)
			for seq := m; seq < total; seq += names {
				if s.dropped[seq] {
					continue
				}
				checked++
				v, ok := got[base+int64(seq/names)]
				if !ok {
					missing++
				} else if v != float64(seq) {
					corrupt++
				}
			}
		}
	}
	var offered, dropped, high int64
	<-deadlineDone
	for _, s := range rs {
		offered += s.offered.Load()
		dropped += s.drops.Load()
		high = max(high, s.high.Load())
	}
	unlogged := firstCache.LogDropped() + next.Cache.LogDropped()
	var logErrors int64
	if firstLog != nil {
		logErrors += firstLog.Metrics().WriteErrors
	}
	if log := next.inputLog.Load(); log != nil {
		logErrors += log.Metrics().WriteErrors
	}
	result := map[string]any{
		"codec": mode, "cpus": cfg.Common.MaxCPU, "names_per_relay": names,
		"continuous_input_log": cfg.InputLog.Enabled,
		"relays":               relays, "rate_per_relay": rate, "seconds": seconds,
		"offered": offered, "relay_dropped": dropped, "queue_high_records": high,
		"store_missing": missing, "store_corrupt": corrupt, "oracle_checked": checked,
		"stop_seconds": stopTime.Seconds(), "reload_before_seconds": reloadBefore.Seconds(),
		"reload_after_seconds":    reloadAfter.Seconds(),
		"reload_inflight_seconds": reloadInFlight.Seconds(),
		"input_log_unlogged":      unlogged, "input_log_write_errors": logErrors,
		"max_receive_counter_gap_seconds": time.Duration(maxPause.Load()).Seconds(),
		"reclaim":                         reclaim, "reclaim_pending_at_stop": reclaimBefore.UnreclaimedBytes,
		"reclaim_compactions_at_stop": compacting,
		"reclaim_stack_confirmed":     stackConfirmed,
		"offered_at_deadline":         offeredAtDeadline.Load(), "socket_sent_at_deadline": sentAtDeadline.Load(),
		"receiver_counter_at_deadline_including_probe": receivedAtDeadline.Load(),
		"positive_control_pause_seconds":               pause.Seconds(),
		"ordered_tail_hold_seconds":                    tailHold.Seconds(),
		"handoff_read_ahead_config_bytes":              cfg.Tcp.HandoffReadAheadBytes,
		"metric_name_bytes":                            len(name(0, 0)),
		"missing_buckyd_secret_during_restart":         missingSecret,
	}
	b, err := json.Marshal(result)
	if err != nil {
		t.Fatal(err)
	}
	t.Log("ZERO_GATE " + string(b))
	if missing != 0 || corrupt != 0 {
		t.Fatalf("store oracle failed: missing=%d corrupt=%d", missing, corrupt)
	}
	if unlogged != 0 || logErrors != 0 {
		t.Errorf("continuous input log lost coverage: unlogged=%d errors=%d", unlogged, logErrors)
	}
	if dropped != 0 && os.Getenv("ZERO_EXPECT_DROPS") != "1" {
		t.Errorf("relay dropped %d records", dropped)
	}
	if dropped == 0 && os.Getenv("ZERO_EXPECT_DROPS") == "1" {
		t.Error("positive control failed to detect relay drops")
	}
	if stopTime >= 10*time.Second {
		t.Errorf("stop exceeded target: %s", stopTime)
	}
	if !strings.Contains(zapwriter.TestString(), "input handed off to successor") {
		t.Fatal("no real input handoff", zapwriter.TestString())
	}
	if reclaim && (reclaimBefore.UnreclaimedBytes == 0 || compacting == 0) {
		t.Fatal("fixture did not stop with reclaim in flight")
	}
}

func reclaimCompactInFlight() bool {
	var stacks bytes.Buffer
	_ = pprof.Lookup("goroutine").WriteTo(&stacks, 2)
	var reclaim, span bool
	for _, stack := range strings.Split(stacks.String(), "\n\n") {
		if strings.Contains(stack, "reclaimRelayout(") {
			reclaim = true
			if strings.Contains(stack, "pebble.(*DB).Compact(") {
				return true // whole-span base control
			}
		}
		if strings.Contains(stack, "compactSpans.func") && strings.Contains(stack, "pebble.(*DB).Compact(") {
			span = true
		}
	}
	// Span calls run in joined workers; the reclaimer waits in the coordinator.
	return reclaim && span
}

// Seed a physical v6 store through the production continuation codec. Normal
// startup upgrades it to v7 and reclaims the old blocks while TCP input runs.
func seedReclaimGate(t *testing.T, cfg *Config) {
	t.Helper()
	ctx := context.Background()
	if err := os.MkdirAll(cfg.Whisper.DataDir, 0750); err != nil {
		t.Fatal(err)
	}
	db, err := store.Open(cfg.Whisper.StoreDir, store.Options{ExactBlocks: true, Continuation: &whisperio.ContinuationCodec{}, IncrementalSource: cfg.Whisper.DataDir, CacheSize: 8 << 20, MemTableSize: 4 << 20, TargetFileSize: 1 << 20, SyncInterval: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	input := make([]store.Point, 4096)
	random := uint64(19)
	for i := range input {
		random ^= random << 13
		random ^= random >> 7
		random ^= random << 17
		input[i] = store.Point{Timestamp: time.Now().Unix() - 5000 + int64(i), Value: float64(random%1000000) / 17}
	}
	for i := range 256 {
		metric := fmt.Sprintf("seed.metric%04d", i)
		if _, err := db.Create(ctx, store.MetricConfig{Name: metric, Retentions: []store.Retention{{Step: 1, Count: 7200}}, AggregationMethod: store.Average, XFilesFactor: .5}); err != nil {
			t.Fatal(err)
		}
		if err := db.UpdateMany(ctx, metric, input); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Compact(); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	// Verify preparation really created old data, rather than an empty journal.
	marker, err := os.ReadFile(filepath.Join(cfg.Whisper.StoreDir, "FORMAT"))
	if err == nil && bytes.Contains(marker, []byte("v7")) {
		t.Fatal("seed unexpectedly v7")
	}
}
