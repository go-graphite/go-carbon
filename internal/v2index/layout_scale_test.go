package v2index

import (
	"context"
	"fmt"
	"hash/fnv"
	"math/rand"
	"os"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"
)

// layoutName returns a deterministic graphite-like name of about 280 bytes:
// team.app.env.region.host then one of 256 metric paths, some of which carry
// a per-host high-entropy token so that not every tail front-codes away.
func layoutName(suffixes []string, i int) string {
	h, m := i/len(suffixes), i%len(suffixes)
	words := []string{"alpha", "bravo", "charlie", "delta", "echo", "foxtrot", "golf", "hotel", "india", "juliet"}
	suffix := suffixes[m]
	if m%8 == 0 {
		f := fnv.New64a()
		fmt.Fprint(f, h, m)
		suffix = strings.Replace(suffix, "TOKEN", strconv.FormatUint(f.Sum64(), 16), 1)
	}
	return fmt.Sprintf("team_%s_%d.%s_payments_availability_service_%02d.%s.region_europe_west_%d_availability_zone.host-%06d_rack-%02d_zone-%d_example_internal_network.%s",
		words[h%10], h%30, words[(h/30)%10], (h/30)%40, []string{"production", "staging"}[(h/1200)%2], (h/2400)%4,
		h, h%48, h%3, suffix)
}

func layoutSuffixes() []string {
	rng := rand.New(rand.NewSource(7))
	subsystems := []string{"jvm.memory_pools", "http_server.requests", "kafka_consumer.fetch_manager", "database_connection_pool",
		"distributed_cache.client", "operating_system.filesystem", "nginx_upstream.backends", "grpc_client.calls"}
	objects := []string{"G1_Old_Generation", "G1_Eden_Space", "endpoint_TOKEN", "partition_assignment", "primary_replica",
		"mount_var_lib_application_data", "upstream_backend_pool", "method_GetAvailabilityForProperty"}
	metrics := []string{"used_bytes_after_collection", "request_duration_seconds", "records_lag_maximum", "active_connections_in_use",
		"cache_lookup_hit_ratio", "available_inodes_fraction", "response_status_5xx_total", "deadline_exceeded_errors_total"}
	stats := []string{"count", "mean", "p50", "p95", "p99", "p999", "max", "rate_1m"}
	out := make([]string, 256)
	for i := range out {
		out[i] = fmt.Sprintf("%s.%s.%s.window_%ds_aggregated_by_relay_tier.%s", subsystems[rng.Intn(8)], objects[rng.Intn(8)], metrics[rng.Intn(8)], 10*(1+rng.Intn(6)), stats[i%8])
	}
	slices.Sort(out)
	return slices.Compact(out)
}

func heapInUse() uint64 {
	runtime.GC()
	runtime.GC()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return m.HeapAlloc
}

func pct(d []time.Duration, p float64) time.Duration {
	slices.Sort(d)
	return d[min(len(d)-1, int(float64(len(d))*p))]
}

// TestCatalogLayoutScale is an opt-in laptop measurement of one layout per
// process: V2_INDEX_LAYOUT_SCALE=1000000 V2_INDEX_LAYOUT=old|f4|f8|f16|mmap.
// mmap is f8 with the base level in a file (SetDir).
// Half the names are published at once, the rest arrive in 10k batches.
func TestCatalogLayoutScale(t *testing.T) {
	n, _ := strconv.Atoi(os.Getenv("V2_INDEX_LAYOUT_SCALE"))
	if n == 0 {
		t.Skip("set V2_INDEX_LAYOUT_SCALE")
	}
	layout := os.Getenv("V2_INDEX_LAYOUT")
	switch layout {
	case "f4":
		levelFanout = 4
	case "f8", "old", "mmap": // old is the plain-string index, fanout 4
		levelFanout = 8
	case "f16":
		levelFanout = 16
	default:
		t.Fatal("V2_INDEX_LAYOUT=old|f4|f8|f16|mmap")
	}
	defer func() { levelFanout = 8 }()
	ctx := context.Background()
	suffixes := layoutSuffixes()
	baseline := heapInUse()
	names := make([]string, n)
	total := 0
	for i := range names {
		names[i] = layoutName(suffixes, i)
		total += len(names[i])
	}
	rand.New(rand.NewSource(1)).Shuffle(n, func(i, j int) { names[i], names[j] = names[j], names[i] })
	const batch = 10_000
	half := n / 2
	type view interface {
		Len() int
		VisitPaths(context.Context, string, func(string) error) error
		MatchPaths(context.Context, string, func(string) error) error
	}
	var nc Catalog
	var oc oldCatalog
	var publish func(uint64, []string) error
	var apply func(uint64, []string) error
	var snap func() view
	var rewritten func(before, after any) int
	var levels func() int
	if layout == "old" {
		publish, apply = oc.Publish, func(r uint64, add []string) error { return oc.Apply(r, add, nil) }
		snap = func() view { return oc.Snapshot() }
		levels = func() int { return len(oc.Snapshot().levels) }
		rewritten = func(before, after any) int {
			b, a, out := before.(*oldSnapshot), after.(*oldSnapshot), 0
			for _, l := range a.levels {
				if !slices.ContainsFunc(b.levels, func(o []string) bool { return &o[0] == &l[0] }) {
					out += 16 * len(l) // string headers; name bytes stay shared
				}
			}
			return out
		}
	} else {
		publish, apply = nc.Publish, func(r uint64, add []string) error { return nc.Apply(r, add, nil) }
		snap = func() view { return nc.Snapshot() }
		levels = func() int { return len(nc.Snapshot().levels) }
		rewritten = func(before, after any) int {
			b, a, out := before.(*Snapshot), after.(*Snapshot), 0
			for _, l := range a.levels {
				if !slices.Contains(b.levels, l) {
					out += len(l.data) + 8*len(l.blocks)
				}
			}
			return out
		}
	}
	if layout == "mmap" {
		if err := nc.SetDir(t.TempDir()); err != nil {
			t.Fatal(err)
		}
		defer nc.Close()
	}
	start := time.Now()
	if err := publish(1, names[:half]); err != nil {
		t.Fatal(err)
	}
	full := time.Since(start)
	var applyTimes []time.Duration
	written, revision := 0, uint64(1)
	for lo := half; lo < n; lo += batch {
		hi := min(n, lo+batch)
		var before any
		if layout == "old" {
			before = oc.Snapshot()
		} else {
			before = nc.Snapshot()
		}
		revision++
		t0 := time.Now()
		if err := apply(revision, names[lo:hi]); err != nil {
			t.Fatal(err)
		}
		applyTimes = append(applyTimes, time.Since(t0))
		var after any
		if layout == "old" {
			after = oc.Snapshot()
		} else {
			after = nc.Snapshot()
		}
		written += rewritten(before, after)
	}
	var applySum time.Duration
	for _, d := range applyTimes {
		applySum += d
	}
	rng := rand.New(rand.NewSource(2))
	probes := make([]string, 2000)
	for i := range probes {
		probes[i] = names[rng.Intn(n)]
	}
	names = nil
	retained := heapInUse() - baseline
	if layout == "mmap" {
		// Heap before the last background base merge is installed, then
		// after: page-cache bytes are the base file.
		probesHeap := uint64(0)
		for _, p := range probes {
			probesHeap += uint64(len(p)) + 16
		}
		before := retained - probesHeap
		for nc.settle(); nc.job != nil; nc.settle() {
		}
		retained = heapInUse() - baseline
		file := 0
		if b := nc.Snapshot().levels[0]; b.path != "" {
			st, err := os.Stat(b.path)
			if err != nil {
				t.Fatal(err)
			}
			file = int(st.Size())
		}
		t.Logf("mmap index_heap_before_install=%.1fB/name index_heap_after_install=%.1fB/name page_cache=%.1fB/name levels_after_install=%d",
			float64(before)/float64(n), float64(retained-probesHeap)/float64(n), float64(file)/float64(n), levels())
	}
	s := snap()
	if s.Len() != n {
		t.Fatalf("len %d, want %d", s.Len(), n)
	}
	var finds []time.Duration
	for _, name := range probes {
		path := strings.ReplaceAll(name, ".", "/")
		hits := 0
		t0 := time.Now()
		if err := s.VisitPaths(ctx, path, func(string) error { hits++; return nil }); err != nil {
			t.Fatal(err)
		}
		finds = append(finds, time.Since(t0))
		if hits != 1 {
			t.Fatalf("exact find %q: %d hits", path, hits)
		}
	}
	t.Logf("names=%d layout=%s mean_name=%.0fB retained=%.1fB/name levels=%d full_publish=%v apply10k_mean=%v apply10k_max=%v rewritten=%.0fB/name exact_find_p50=%v p99=%v",
		n, layout, float64(total)/float64(n), float64(retained)/float64(n), levels(), full.Round(time.Millisecond),
		(applySum / time.Duration(len(applyTimes))).Round(time.Microsecond), slices.Max(applyTimes).Round(time.Microsecond),
		float64(written)/float64(n-half), pct(finds, .5), pct(finds, .99))
	globs := []struct{ kind, glob string }{
		{"prefix-small", "team_alpha_0/alpha_payments_availability_service_00/production/region_europe_west_0_availability_zone/host-000000_rack-00_zone-0_example_internal_network/jvm/*"},
		{"prefix-large", "team_alpha_0/*/*/*/*/*"},
		{"mid-small", "team_alpha_0/*/production/region_europe_west_0_availability_zone/*/grpc_client/calls/*"},
		{"mid-large", "team_alpha_0/*/*/*/*/*/*/*/*/*/count.wsp"},
		{"lead-small", "*/alpha_payments_availability_service_00"},
		{"lead-large", "*/*/*/*/*/jvm"},
	}
	for _, g := range globs {
		var lat []time.Duration
		hits := 0
		for range 5 {
			hits = 0
			t0 := time.Now()
			if err := s.MatchPaths(ctx, g.glob, func(string) error { hits++; return nil }); err != nil {
				t.Fatal(err)
			}
			lat = append(lat, time.Since(t0))
		}
		t.Logf("glob %s matches=%d median=%v", g.kind, hits, pct(lat, .5))
	}
	if layout != "old" {
		cs := nc.Snapshot()
		t0 := time.Now()
		seen := 0
		for range cs.Names() {
			seen++
		}
		seq := time.Since(t0)
		base := cs.levels[0]
		var buf []byte
		t0 = time.Now()
		for i := range 100_000 {
			buf = base.at((i*7919)%base.n, buf)
		}
		t.Logf("sequential_decode=%.1fns/name position_decode=%.0fns/name", float64(seq)/float64(seen), float64(time.Since(t0))/100_000)
	}
}
