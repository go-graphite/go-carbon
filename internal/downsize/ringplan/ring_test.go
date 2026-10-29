package ringplan

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/go-graphite/go-carbon/buckyd"
	"github.com/go-graphite/go-carbon/carbon"
	"github.com/go-graphite/go-carbon/internal/chunkstore"
	hashing "github.com/go-graphite/go-carbon/internal/downsize/buckyhash"
)

func fixtureSpec(root string, n int) specification {
	s := specification{Version: 1, Cluster: "metrics", Site: "region-a"}
	s.Old.Hash, s.New.Hash = "jump_fnv1a", "jump_fnv1a"
	s.Old.Replicas, s.New.Replicas = 1, 1
	for i := 0; i < n; i++ {
		host := fmt.Sprintf("store-metrics-%d.region-a.example.net", 1201+i)
		m := member{ID: fmt.Sprintf("metrics-%03d", i), Node: fmt.Sprintf("%s:2003=%03d", host, i), Endpoint: "http://" + host + ":4242", MTCP: 5}
		s.Old.Nodes = append(s.Old.Nodes, m)
		s.Inputs = append(s.Inputs, input{Source: m.ID, Path: filepath.Join(root, fmt.Sprintf("inventory-%03d.txt", i)), Format: "lines", EstimatedBytes: 600})
	}
	s.New.Nodes = append([]member(nil), s.Old.Nodes[:max(1, n/2)]...)
	return s
}

func writeFixture(t testing.TB, root string, total int) string {
	t.Helper()
	s := fixtureSpec(root, 6)
	r, err := newRing(&s.Old)
	if err != nil {
		t.Fatal(err)
	}
	files := make([]*os.File, len(s.Inputs))
	writers := make([]*bufio.Writer, len(files))
	for i, in := range s.Inputs {
		f, err := os.Create(in.Path)
		if err != nil {
			t.Fatal(err)
		}
		files[i] = f
		writers[i] = bufio.NewWriterSize(f, 256<<10)
	}
	var scratch [1]int
	for i := 0; i < total; i++ {
		name := fmt.Sprintf("aggregations.minutely.metric_%012d.by_service.service_%03d.by_host.host_%04d", i, i%997, i%1777)
		owner := r.compiled.Lookup(name, scratch[:0])[0]
		if _, err := writers[owner].WriteString(name + "\n"); err != nil {
			t.Fatal(err)
		}
	}
	for i, w := range writers {
		if err := w.Flush(); err != nil {
			t.Fatal(err)
		}
		if err := files[i].Close(); err != nil {
			t.Fatal(err)
		}
	}
	path := filepath.Join(root, "spec.json")
	blob, err := json.MarshalIndent(s, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, blob, 0600); err != nil {
		t.Fatal(err)
	}
	return path
}

func testOptions(out string) options {
	return options{Out: out, Apply: true, Workers: 2, PerSourceWorkers: 1, SegmentMetrics: 37, HTTPTimeout: 5 * time.Second}
}

func TestPlanShrinkResumeAndIntegrity(t *testing.T) {
	path := writeFixture(t, t.TempDir(), 3000)
	s, old, next, id, err := loadSpec(path)
	if err != nil {
		t.Fatal(err)
	}
	out := filepath.Join(t.TempDir(), "plan")
	opt := testOptions(out)
	m, err := plan(context.Background(), s, old, next, id, opt)
	if err != nil {
		t.Fatal(err)
	}
	if m.Summary.Metrics != 3000 || m.Summary.MovedFraction < .45 || m.Summary.MovedFraction > .55 || m.Summary.Copies != m.Summary.MovedMetrics {
		t.Fatalf("bad summary: %+v", m.Summary)
	}
	if m.Summary.EstimatedBytes != 600*m.Summary.Copies {
		t.Fatal("bad byte total")
	}
	var count uint64
	var scratch [1]int
	for _, stream := range m.Streams {
		f, err := os.Open(filepath.Join(out, stream.Path))
		if err != nil {
			t.Fatal(err)
		}
		dec := json.NewDecoder(f)
		var rec struct {
			Type, Format, PlanID, Source, Destination, Metric string
			Version                                           int
			Metrics                                           uint64
		}
		if err := dec.Decode(&rec); err != nil || rec.Type != "header" || rec.Version != 1 || rec.Source != stream.Source || rec.Destination != stream.Destination {
			t.Fatalf("header %+v %v", rec, err)
		}
		n := uint64(0)
		for {
			if err := dec.Decode(&rec); err != nil {
				t.Fatal(err)
			}
			if rec.Type == "end" {
				break
			}
			if rec.Type != "move" {
				t.Fatal(rec)
			}
			owner := next.compiled.Lookup(rec.Metric, scratch[:0])[0]
			if next.members[owner].ID != stream.Destination {
				t.Fatal("wrong destination")
			}
			n++
		}
		f.Close()
		if n != stream.Metrics || n != rec.Metrics {
			t.Fatal("stream count mismatch")
		}
		count += n
	}
	if count != m.Summary.Copies {
		t.Fatal("missing records")
	}
	again, err := plan(context.Background(), s, old, next, id, opt)
	if err != nil || !reflect.DeepEqual(m, again) {
		t.Fatalf("resume differs: %v", err)
	}
	first := filepath.Join(out, m.Streams[0].Path)
	if err := os.WriteFile(first, []byte("corrupt\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := plan(context.Background(), s, old, next, id, opt); err == nil {
		t.Fatal("corrupt checkpoint accepted")
	}
	if _, err := os.Stat(filepath.Join(out, "manifest.json")); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("failed run published manifest")
	}
}

func TestPlanInterruptedSegmentResume(t *testing.T) {
	path := writeFixture(t, t.TempDir(), 1000)
	s, old, next, id, err := loadSpec(path)
	if err != nil {
		t.Fatal(err)
	}
	out := t.TempDir()
	opt := testOptions(out)
	full, err := plan(context.Background(), s, old, next, id, opt)
	if err != nil {
		t.Fatal(err)
	}
	// Rewind a checkpoint to the first complete segment, preserving orphaned
	// later files as if the process died after rename and before checkpoint.
	statePath := filepath.Join(out, "input-0005.state.json")
	var state inputState
	data, _ := os.ReadFile(statePath)
	if err := json.Unmarshal(data, &state); err != nil {
		t.Fatal(err)
	}
	state = inputState{PlanID: id, Size: state.Size, MTime: state.MTime}
	inputData, err := os.ReadFile(s.Inputs[5].Path)
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(string(inputData), "\n")
	for _, line := range lines[:opt.SegmentMetrics] {
		state.Offset += int64(len(line) + 1)
		state.Previous = line
	}
	state.Segment = 1
	state.Summary = summary{Metrics: uint64(opt.SegmentMetrics), MovedMetrics: uint64(opt.SegmentMetrics), Copies: uint64(opt.SegmentMetrics), EstimatedBytes: uint64(opt.SegmentMetrics) * 600}
	for _, stream := range full.Streams {
		if strings.HasPrefix(stream.Path, "streams/0005-000000-") {
			state.Streams = append(state.Streams, stream)
			addPair(&state.Summary, pairCount{Source: stream.Source, Destination: stream.Destination, Metrics: stream.Metrics, EstimatedBytes: stream.EstimatedBytes})
		}
	}
	if err := atomicJSON(statePath, state); err != nil {
		t.Fatal(err)
	}
	again, err := plan(context.Background(), s, old, next, id, opt)
	if err != nil || !reflect.DeepEqual(full, again) {
		t.Fatalf("orphan resume differs: %v", err)
	}
	// An append invalidates even a completed input.
	f, err := os.OpenFile(s.Inputs[0].Path, os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = f.WriteString("zzzz\n")
	f.Close()
	if _, err := plan(context.Background(), s, old, next, id, opt); err == nil {
		t.Fatal("changed inventory accepted")
	}
}

func TestPlanDryRunGrowAndReplace(t *testing.T) {
	path := writeFixture(t, t.TempDir(), 1200)
	s, old, _, id, err := loadSpec(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, mode := range []string{"grow", "replace", "same"} {
		t.Run(mode, func(t *testing.T) {
			n := s
			n.New = n.Old
			n.New.Nodes = append([]member(nil), n.Old.Nodes...)
			if mode == "grow" {
				n.New.Nodes = append(n.New.Nodes, member{ID: "new", Node: "new:2003=006", Endpoint: "http://new:4242"})
			}
			if mode == "replace" {
				n.New.Nodes[2].ID = "replacement"
				n.New.Nodes[2].Node = "replacement:2003=002"
				n.New.Nodes[2].Endpoint = "http://replacement:4242"
			}
			next, err := newRing(&n.New)
			if err != nil {
				t.Fatal(err)
			}
			out := filepath.Join(t.TempDir(), "not-created")
			opt := testOptions(out)
			opt.Apply = false
			m, err := plan(context.Background(), n, old, next, id, opt)
			if err != nil {
				t.Fatal(err)
			}
			if mode == "same" && m.Summary.Copies != 0 {
				t.Fatal("unchanged ring moved")
			}
			if mode != "same" && (m.Summary.MovedFraction < .1 || m.Summary.MovedFraction > .2) {
				t.Fatal(m.Summary)
			}
			if _, err := os.Stat(out); !errors.Is(err, os.ErrNotExist) {
				t.Fatal("dry-run created output")
			}
		})
	}
}

func TestInputFormatsAndErrors(t *testing.T) {
	for _, format := range []string{"lines", "tsv", "jsonl"} {
		t.Run(format, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "names")
			text := "a\nb\n"
			if format == "tsv" {
				text = "a\t10\nb\t20\n"
			}
			if format == "jsonl" {
				text = "{\"metric\":\"a\",\"estimated_bytes\":10}\n{\"metric\":\"b\",\"estimated_bytes\":20}\n"
			}
			if err := os.WriteFile(path, []byte(text), 0600); err != nil {
				t.Fatal(err)
			}
			inv, err := openInventory(context.Background(), input{Path: path, Format: format, EstimatedBytes: 15}, 0, "", time.Second)
			if err != nil {
				t.Fatal(err)
			}
			defer inv.close()
			name, size, err := inv.next()
			if name != "a" || err != nil || (format != "lines" && size != 10) {
				t.Fatalf("%s %d %v", name, size, err)
			}
			offset := inv.offset
			resumed, err := openInventory(context.Background(), input{Path: path, Format: format}, offset, name, time.Second)
			if err != nil {
				t.Fatal(err)
			}
			defer resumed.close()
			name, _, err = resumed.next()
			if name != "b" || err != nil {
				t.Fatal(name, err)
			}
		})
	}
	for _, name := range []string{"", "white space", "bad\x00", "bad\x7f", "bad\xff"} {
		if validMetric(name) == nil {
			t.Fatalf("accepted %q", name)
		}
	}
	for _, name := range []string{`metric."quote`, "metric.日本語", "metric.\u0085", "metric.\\slash"} {
		if err := validMetric(name); err != nil {
			t.Fatal(err)
		}
		line := strconv.AppendQuote(nil, name)
		var decoded string
		if err := json.Unmarshal(line, &decoded); err != nil || decoded != name {
			t.Fatal("non-JSON quote", string(line), err)
		}
	}
}

func TestCommittedSample(t *testing.T) {
	data, err := os.ReadFile("testdata/sample-plan/manifest.json")
	if err != nil {
		t.Fatal(err)
	}
	var m manifest
	if err := json.Unmarshal(data, &m); err != nil {
		t.Fatal(err)
	}
	blob, err := json.Marshal(m.Spec)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(blob)
	if m.Version != version || m.Format != formatName || m.PlanID != hex.EncodeToString(sum[:]) {
		t.Fatal("sample plan identity mismatch")
	}
	if err := checkStreams("testdata/sample-plan", m.Streams); err != nil {
		t.Fatal(err)
	}
	if len(m.Streams) != 1 || m.Streams[0].Metrics != m.Summary.Copies {
		t.Fatal("sample stream summary mismatch")
	}
}

func TestHTTPInventoryAuthRedirectAndTruncation(t *testing.T) {
	t.Setenv("RING_TEST_READ_TOKEN", "test-read-only")
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		if r.Header.Get("X-Buckyd-Authorization") != "test-read-only" {
			http.Error(w, "missing token", 403)
			return
		}
		if r.URL.Path == "/redirect" {
			http.Redirect(w, r, "/metrics", http.StatusFound)
			return
		}
		if r.URL.Path == "/truncated" {
			_, _ = io.WriteString(w, `["a",`)
			return
		}
		_, _ = io.WriteString(w, `["a","b"]`)
	}))
	defer server.Close()
	in := input{Format: "buckyd", URL: server.URL + "/metrics", TokenEnv: "RING_TEST_READ_TOKEN", EstimatedBytes: 600}
	inv, err := openInventory(context.Background(), in, 0, "", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"a", "b"} {
		name, _, err := inv.next()
		if err != nil || name != want {
			t.Fatal(name, err)
		}
	}
	if _, _, err := inv.next(); !errors.Is(err, io.EOF) {
		t.Fatal(err)
	}
	inv.close()
	in.URL = server.URL + "/redirect"
	if _, err := openInventory(context.Background(), in, 0, "", time.Second); err == nil {
		t.Fatal("redirect followed")
	}
	if requests.Load() != 2 {
		t.Fatal("redirect leaked a second request")
	}
	in.URL = server.URL + "/truncated"
	inv, err = openInventory(context.Background(), in, 0, "", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer inv.close()
	if _, _, err := inv.next(); err != nil {
		t.Fatal(err)
	}
	if _, _, err := inv.next(); err == nil || errors.Is(err, io.EOF) {
		t.Fatal("truncated array accepted")
	}
}

func TestRejectUnownedAndIncompleteCoverage(t *testing.T) {
	path := writeFixture(t, t.TempDir(), 100)
	s, old, next, id, err := loadSpec(path)
	if err != nil {
		t.Fatal(err)
	}
	opt := testOptions("")
	opt.Apply = false
	s.Inputs[0].Source = "wrong"
	if _, err := plan(context.Background(), s, old, next, id, opt); err == nil {
		t.Fatal("missing coverage accepted")
	}
	s.PartialInventory = true
	if _, err := plan(context.Background(), s, old, next, id, opt); err == nil {
		t.Fatal("unowned source accepted")
	}
	s.AllowUnowned = true
	m, err := plan(context.Background(), s, old, next, id, opt)
	if err != nil || m.Summary.Unowned == 0 {
		t.Fatal(m.Summary, err)
	}
}

func TestCompareAndConfig(t *testing.T) {
	s := fixtureSpec(t.TempDir(), 6)
	old, _ := newRing(&s.Old)
	next, _ := newRing(&s.New)
	s.Inputs = nil
	rows, err := compare(context.Background(), s, old, 3, 5000, 1000, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 60 {
		t.Fatal(len(rows))
	}
	var tail *comparison
	for i := range rows {
		r := &rows[i]
		if r.Baseline == "current" && r.NewHash == "jump_fnv1a" && reflect.DeepEqual(r.Keep, []string{"metrics-000", "metrics-001", "metrics-002"}) {
			tail = r
		}
	}
	if tail == nil || tail.Fraction < .46 || tail.Fraction > .54 {
		t.Fatal(tail)
	}
	if _, err := compare(context.Background(), s, old, 3, 100, 3, time.Second); err == nil {
		t.Fatal("unbounded candidate search")
	}
	s.Old.Nodes[0].Endpoint = "http://control-a.example.net:4242"
	out := filepath.Join(t.TempDir(), "config")
	if err := generateConfig(s, old, next, "test", testOptions(out), io.Discard); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(out, "relay-new.toml"))
	if err != nil {
		t.Fatal(err)
	}
	var cfg struct {
		Clusters []struct {
			Name, Type string
			Hosts      []struct {
				Name              string
				Index, Port, MTCP int
			} `toml:"hosts"`
		} `toml:"cluster"`
	}
	if _, err := toml.Decode(string(data), &cfg); err != nil {
		t.Fatal(err)
	}
	if len(cfg.Clusters) != 1 || len(cfg.Clusters[0].Hosts) != 3 || cfg.Clusters[0].Hosts[2].Index != 2 || cfg.Clusters[0].Hosts[0].MTCP != 5 {
		t.Fatalf("bad relay config %+v", cfg)
	}
	data, _ = os.ReadFile(filepath.Join(out, "buckyd-005.toml"))
	var bc struct{ Buckyd buckyd.Config }
	if _, err := toml.Decode(string(data), &bc); err != nil {
		t.Fatal(err)
	}
	if bc.Buckyd.Hash != "jump_fnv1a" || len(bc.Buckyd.Nodes) != 3 {
		t.Fatal(bc)
	}
	if len(bc.Buckyd.TransferSources) != len(s.Old.Nodes)+1 || !slices.Contains(bc.Buckyd.TransferSources, "control-a.example.net") {
		t.Fatal("transfer endpoint alias missing from generated configuration", bc)
	}
	for _, source := range s.Old.Nodes {
		parsed, _ := hashing.NewNodeParser(source.Node)
		if !slices.Contains(bc.Buckyd.TransferSources, parsed.Server) {
			t.Fatal("transfer source missing from generated configuration", bc)
		}
	}
}

func TestBuckydBuckytoolsAndRelayAgree(t *testing.T) {
	var vectors []struct {
		Metric  string         `json:"metric"`
		FNV64   uint64         `json:"fnv64"`
		Buckets map[string]int `json:"buckets"`
	}
	data, err := os.ReadFile("testdata/relay-vectors.json")
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(data, &vectors); err != nil {
		t.Fatal(err)
	}
	db, err := chunkstore.Open(t.TempDir(), chunkstore.Options{ExactBlocks: true, CacheSize: 1 << 20, MemTableSize: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for _, count := range []int{1, 2, 3, 6, 8, 64, 666} {
		t.Run(strconv.Itoa(count), func(t *testing.T) {
			raw := make([]string, count)
			for i := range raw {
				raw[count-i-1] = fmt.Sprintf("host-%03d:2003=%03d", i, i)
			}
			svc, err := buckyd.New(buckyd.Config{Hash: "jump_fnv1a", Replicas: 1, Nodes: raw, TmpDir: t.TempDir()}, db)
			if err != nil {
				t.Fatal(err)
			}
			rec := httptest.NewRecorder()
			svc.Handler().ServeHTTP(rec, httptest.NewRequest("GET", "/hashring", nil))
			var advertised hashing.JSONRingType
			if err := json.Unmarshal(rec.Body.Bytes(), &advertised); err != nil {
				t.Fatal(err)
			}
			if advertised.Algo != "jump_fnv1a" || advertised.Replicas != 1 {
				t.Fatal(advertised)
			}
			r := hashing.NewJumpHashRing(advertised.Replicas)
			for _, node := range advertised.Nodes {
				r.AddNode(node)
			}
			fast, err := hashing.Compile(advertised.Algo, advertised.Nodes, advertised.Replicas, 100)
			if err != nil {
				t.Fatal(err)
			}
			var scratch [1]int
			for _, v := range vectors {
				if got := hashing.Fnv1a64([]byte(v.Metric)); got != v.FNV64 {
					t.Fatalf("FNV disagrees for %q", v.Metric)
				}
				want := fmt.Sprintf("host-%03d", v.Buckets[strconv.Itoa(count)])
				if r.GetNode(v.Metric).Server != want || fast.Nodes()[fast.Lookup(v.Metric, scratch[:0])[0]].Server != want {
					t.Fatalf("buckyd/buckytools/planner/relay disagree for %q N=%d", v.Metric, count)
				}
			}
		})
	}
}

func startLocalApp(t *testing.T, rawNodes []string) (*carbon.App, *httptest.Server) {
	t.Helper()
	root := t.TempDir()
	path := carbon.TestConfig(root)
	cfg, err := carbon.ReadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	cfg.Common.MaxCPU = 0
	cfg.Udp.Enabled, cfg.Tcp.Enabled, cfg.Pickle.Enabled = false, false, false
	cfg.Grpc.Enabled, cfg.Carbonlink.Enabled = false, false
	cfg.Pprof.Enabled, cfg.Prometheus.Enabled = false, false
	cfg.Carbonserver.Enabled = false
	cfg.Whisper.StorageBackend, cfg.Whisper.StoreEncoding = "pebble-chunk", "exact"
	cfg.Whisper.StoreDir, cfg.Whisper.DataDir = filepath.Join(root, "store"), filepath.Join(root, "files")
	cfg.Whisper.StoreCacheSize, cfg.Whisper.StoreMemTableSize = 1<<20, 1<<20
	cfg.Buckyd.Enabled, cfg.Buckyd.Bind, cfg.Buckyd.TmpDir = true, "127.0.0.1:0", filepath.Join(root, "tmp")
	cfg.Buckyd.Hash, cfg.Buckyd.Nodes, cfg.Buckyd.Replicas = "jump_fnv1a", rawNodes, 1
	var b bytes.Buffer
	if err := toml.NewEncoder(&b).Encode(cfg); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, b.Bytes(), 0600); err != nil {
		t.Fatal(err)
	}
	app := carbon.New(path)
	if err := app.ParseConfig(); err != nil {
		t.Fatal(err)
	}
	if err := app.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(app.Stop)
	server := httptest.NewServer(app.Buckyd.Handler())
	t.Cleanup(server.Close)
	return app, server
}

func TestLocalPebbleBuckydPlanAndCopy(t *testing.T) {
	s := fixtureSpec(t.TempDir(), 2)
	old, err := newRing(&s.Old)
	if err != nil {
		t.Fatal(err)
	}
	next, _ := newRing(&s.New)
	raw := []string{s.Old.Nodes[0].Node, s.Old.Nodes[1].Node}
	apps := make([]*carbon.App, 2)
	for i := range apps {
		app, server := startLocalApp(t, raw)
		apps[i] = app
		s.Inputs[i] = input{Source: s.Old.Nodes[i].ID, URL: server.URL + "/metrics", Format: "buckyd", EstimatedBytes: 600}
	}
	var scratch [1]int
	ctx := context.Background()
	now := time.Now().Unix()/60*60 - 120
	for i := 0; i < 100; i++ {
		name := fmt.Sprintf("aggregations.minutely.metric.%03d", i)
		owner := old.compiled.Lookup(name, scratch[:0])[0]
		_, err := apps[owner].MetricStore.Create(ctx, chunkstore.MetricConfig{Name: name, Retentions: []chunkstore.Retention{{Step: 60, Count: 60}}, AggregationMethod: chunkstore.Sum})
		if err != nil {
			t.Fatal(err)
		}
		if err := apps[owner].MetricStore.UpdateMany(ctx, name, []chunkstore.Point{{Timestamp: now, Value: float64(i)}}); err != nil {
			t.Fatal(err)
		}
	}
	out := t.TempDir()
	m, err := plan(ctx, s, old, next, "local-e2e", testOptions(out))
	if err != nil {
		t.Fatal(err)
	}
	if m.Summary.Metrics != 100 || m.Summary.Copies == 0 {
		t.Fatal(m.Summary)
	}
	// Exercise the published move stream against the real embedded buckyd API.
	for _, stream := range m.Streams {
		f, err := os.Open(filepath.Join(out, stream.Path))
		if err != nil {
			t.Fatal(err)
		}
		d := json.NewDecoder(f)
		for d.More() {
			var rec struct{ Type, Metric string }
			if err := d.Decode(&rec); err != nil {
				t.Fatal(err)
			}
			if rec.Type != "move" {
				continue
			}
			resp, err := http.Get(s.Inputs[1].URL + "/" + rec.Metric)
			if err != nil {
				t.Fatal(err)
			}
			data, err := io.ReadAll(resp.Body)
			resp.Body.Close()
			if err != nil || resp.StatusCode != 200 {
				t.Fatal(resp.Status, err)
			}
			r := httptest.NewRequest("POST", "/metrics/"+rec.Metric, bytes.NewReader(data))
			result := httptest.NewRecorder()
			apps[0].Buckyd.Handler().ServeHTTP(result, r)
			if result.Code != 200 && result.Code != 201 && result.Code != 204 {
				t.Fatal(result.Code, result.Body.String())
			}
			src, err := apps[1].MetricStore.Snapshot(ctx, rec.Metric)
			if err != nil {
				t.Fatal(err)
			}
			dst, err := apps[0].MetricStore.Snapshot(ctx, rec.Metric)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(src.Archives, dst.Archives) {
				t.Fatal("copied points differ")
			}
			// No source deletion: plan consumption alone cannot authorize it.
		}
		f.Close()
	}
	all, err := apps[0].MetricStore.List(ctx, "")
	if err != nil || len(all) != 100 {
		t.Fatal(len(all), err)
	}
	resumed, err := plan(ctx, s, old, next, "local-e2e", testOptions(out))
	if err != nil || !reflect.DeepEqual(m, resumed) {
		t.Fatalf("completed HTTP inventory was not reused: %v", err)
	}
	// A final live re-list sees the deliberately copied duplicates on survivors.
	// They must be explicitly accounted for, not silently treated as old owners.
	s.AllowUnowned = true
	final, err := plan(ctx, s, old, next, "local-e2e-final", testOptions(t.TempDir()))
	if err != nil || final.Summary.Unowned != m.Summary.Copies || final.Summary.Copies != m.Summary.Copies {
		t.Fatalf("final duplicate reconciliation: %+v, %v", final.Summary, err)
	}
}

func BenchmarkPlanMillion(b *testing.B) {
	root := os.Getenv("RING_BENCH_DIR")
	if root == "" {
		root = b.TempDir()
	} else if err := os.MkdirAll(root, 0700); err != nil {
		b.Fatal(err)
	}
	path := writeFixture(b, root, 1000000)
	s, old, next, id, err := loadSpec(path)
	if err != nil {
		b.Fatal(err)
	}
	var inputBytes int64
	for _, in := range s.Inputs {
		info, err := os.Stat(in.Path)
		if err != nil {
			b.Fatal(err)
		}
		inputBytes += info.Size()
	}
	for _, workers := range []int{1, 2, 4, 8} {
		b.Run(fmt.Sprintf("workers=%d", workers), func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(inputBytes)
			for i := 0; i < b.N; i++ {
				opt := testOptions(filepath.Join(b.TempDir(), "plan"))
				opt.Workers = workers
				opt.SegmentMetrics = 100000
				m, err := plan(context.Background(), s, old, next, id, opt)
				if err != nil {
					b.Fatal(err)
				}
				if m.Summary.Metrics != 1000000 {
					b.Fatal("missing input")
				}
				b.ReportMetric(float64(m.Summary.Metrics)/b.Elapsed().Seconds(), "names/s")
			}
		})
	}
}

// Keep the name ordering constraint explicit: JSON or TSV bytes are irrelevant.
func TestLexicalNameOrder(t *testing.T) {
	for _, text := range []string{"b\na\n", "a\na\n"} {
		path := filepath.Join(t.TempDir(), "bad")
		_ = os.WriteFile(path, []byte(text), 0600)
		inv, err := openInventory(context.Background(), input{Path: path, Format: "lines"}, 0, "", time.Second)
		if err != nil {
			t.Fatal(err)
		}
		_, _, _ = inv.next()
		if _, _, err := inv.next(); err == nil {
			t.Fatal("accepted unordered names")
		}
		inv.close()
	}
	if !sort.StringsAreSorted([]string{"a.001", "a.002"}) || strings.Compare("a.001", "a.002") >= 0 {
		t.Fatal("unexpected order")
	}
}
