//go:build linux || darwin

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"math/rand"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/internal/mixedread"
	"github.com/go-graphite/go-carbon/internal/partsconvert"
	"github.com/go-graphite/go-carbon/internal/partsformat"
	"github.com/go-graphite/go-carbon/internal/partsread"
	"github.com/go-graphite/go-carbon/internal/whisperio"
	pb "github.com/go-graphite/protocol/carbonapi_v3_pb"
)

// realOracle is deliberately an offline diagnostic, not a daemon publication.
// Every oracle value is an IEEE bit pattern; JSON floats cannot preserve NaNs.
type realOracle struct {
	Version                            int
	Now, Seed                          int64
	CatalogNames                       uint64
	CatalogNamespaces, CatalogPolicies map[string]uint64
	Queries                            []realQuery
}
type realQuery struct {
	Name                           string
	Metric                         partsread.Metric
	From, Until, Start, Stop, Step int64
	Bits                           []uint64
	LegacyFetchNS                  int64 `json:",omitempty"`
}

func realOptions(path string, now int64) (chunkstore.Options, chunkstore.TransferSourceOptions, error) {
	b, err := os.ReadFile(filepath.Join(path, "CHUNKSTORE"))
	if err != nil {
		return chunkstore.Options{}, chunkstore.TransferSourceOptions{}, err
	}
	clock := func() time.Time { return time.Unix(now, 0) }
	o := chunkstore.Options{ReadOnly: true, ExactBlocks: true, CacheSize: 8 << 20, Now: clock}
	t := chunkstore.TransferSourceOptions{CacheBytes: 8 << 20, Now: clock}
	if !strings.Contains(string(b), "chunks-v4\n") {
		codec := &whisperio.ContinuationCodec{}
		o.Continuation, t.Continuation = codec, codec
		o.TimeBuckets = !strings.Contains(string(b), "chunks-v6\n")
	}
	return o, t, nil
}

func captureReal(ctx context.Context, path, oraclePath string) error {
	return captureRealAt(ctx, path, oraclePath, time.Now().Unix())
}

func captureRealAt(ctx context.Context, path, oraclePath string, now int64) error {
	if oraclePath == "" {
		return errors.New("real oracle path is required")
	}
	r := realOracle{Version: 1, Now: now, Seed: 20261115, CatalogNamespaces: make(map[string]uint64), CatalogPolicies: make(map[string]uint64)}
	_, opts, err := realOptions(path, r.Now)
	if err != nil {
		return err
	}
	s, err := chunkstore.OpenTransferSource(path, opts)
	if err != nil {
		return err
	}
	rng := rand.New(rand.NewSource(r.Seed))
	var selected []chunkstore.Metadata
	after := ""
	for {
		page, e := s.ListCatalogPage(ctx, "", after, 10000)
		if e != nil {
			_ = s.Close()
			return e
		}
		if len(page) == 0 {
			break
		}
		for _, m := range page {
			namespace, _, _ := strings.Cut(m.Name, ".")
			var policy strings.Builder
			fmt.Fprintf(&policy, "%s/xff-%08x", m.AggregationMethod.String(), math.Float32bits(m.XFilesFactor))
			for _, a := range m.Retentions {
				fmt.Fprintf(&policy, "/%dx%d", a.Step, a.Count)
			}
			r.CatalogNamespaces[namespace]++
			r.CatalogPolicies[policy.String()]++
			if len(r.CatalogNamespaces) > 4096 || len(r.CatalogPolicies) > 4096 {
				_ = s.Close()
				return errors.New("copied catalogue exceeds 4096 namespace/policy accounting bound")
			}
			r.CatalogNames++
			if len(selected) < 2000 {
				selected = append(selected, m)
			} else if i := rng.Int63n(int64(r.CatalogNames)); i < 2000 {
				selected[i] = m
			}
		}
		after = page[len(page)-1].Name
	}
	if err = s.Close(); err != nil {
		return err
	}
	if len(selected) != 2000 {
		return fmt.Errorf("need at least 2000 copied names, got %d", len(selected))
	}
	slices.SortFunc(selected, func(a, b chunkstore.Metadata) int { return strings.Compare(a.Name, b.Name) })
	o, _, err := realOptions(path, r.Now)
	if err != nil {
		return err
	}
	legacy, err := chunkstore.Open(path, o)
	if err != nil {
		return err
	}
	defer legacy.Close()
	for i, m := range selected {
		if len(m.Retentions) == 0 {
			return errors.New("sample has no archives")
		}
		a := m.Retentions[rng.Intn(len(m.Retentions))]
		horizon := int64(a.Step) * int64(a.Count)
		// Historical ranges avoid changing cache tails. No older difference is
		// excused as a write newer than the acquisition snapshot.
		age := int64(a.Step) + rng.Int63n(max(int64(1), horizon-int64(a.Step)))
		from := r.Now - age
		until := min(r.Now-1, from+int64(a.Step)*min(int64(a.Count), 4096))
		started := time.Now()
		series, e := legacy.Fetch(ctx, m.Name, int(from), int(until))
		fetchNS := time.Since(started).Nanoseconds()
		if e != nil {
			return fmt.Errorf("offline oracle %s: %w", m.Name, e)
		}
		q := realQuery{Name: m.Name, From: from, Until: until, Start: int64(series.FromTime), Stop: int64(series.UntilTime), Step: int64(series.Step), Metric: partsread.Metric{ID: uint64(i + 1), AggregationMethod: m.AggregationMethod.String(), XFilesFactor: m.XFilesFactor}}
		q.LegacyFetchNS = fetchNS
		for _, a := range m.Retentions {
			q.Metric.Archives = append(q.Metric.Archives, partsread.Archive{Step: int64(a.Step), Points: int64(a.Count)})
		}
		for _, v := range series.Values {
			q.Bits = append(q.Bits, math.Float64bits(v))
		}
		r.Queries = append(r.Queries, q)
	}
	f, err := os.OpenFile(oraclePath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return err
	}
	err = json.NewEncoder(f).Encode(r)
	return errors.Join(err, f.Sync(), f.Close())
}

func compareRealSeries(q realQuery, start, stop, step int64, values []float64) error {
	if start != q.Start || stop != q.Stop || step != q.Step || len(values) != len(q.Bits) {
		return fmt.Errorf("shape differs for %s: %d:%d:%d/%d vs %d:%d:%d/%d", q.Name, start, stop, step, len(values), q.Start, q.Stop, q.Step, len(q.Bits))
	}
	for i, v := range values {
		if math.Float64bits(v) != q.Bits[i] {
			return fmt.Errorf("value bits differ for %s at %d: %016x vs %016x", q.Name, start+int64(i)*step, math.Float64bits(v), q.Bits[i])
		}
	}
	return nil
}

func sourceReal(ctx context.Context, base string, q realQuery) error {
	request := pb.MultiFetchRequest{Metrics: []*pb.FetchRequest{{Name: q.Name, PathExpression: q.Name, StartTime: q.From, StopTime: q.Until}}}
	b, err := request.MarshalVT()
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(base, "/")+"/render/?format=carbonapi_v3_pb", bytes.NewReader(b))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/x-carbonapi-v3-pb")
	req.Header.Set("Cache-Control", "no-cache")
	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return fmt.Errorf("source render %s: HTTP %d", q.Name, resp.StatusCode)
	}
	b, err = io.ReadAll(io.LimitReader(resp.Body, 16<<20))
	if err != nil {
		return err
	}
	var result pb.MultiFetchResponse
	if err = result.UnmarshalVT(b); err != nil {
		return err
	}
	if len(result.Metrics) != 1 || result.Metrics[0].Name != q.Name {
		return errors.New("source returned another series")
	}
	m := result.Metrics[0]
	return compareRealSeries(q, m.StartTime, m.StopTime, m.StepTime, m.Values)
}

func verifyReal(ctx context.Context, legacyPath, targetPath, oraclePath, sourceURL string) error {
	b, err := os.ReadFile(oraclePath)
	if err != nil {
		return err
	}
	var oracle realOracle
	if err = json.Unmarshal(b, &oracle); err != nil {
		return err
	}
	if oracle.Version != 1 || len(oracle.Queries) != 2000 {
		return errors.New("invalid real oracle")
	}
	b, err = os.ReadFile(filepath.Join(targetPath, "names", "name-cursor.json"))
	if err != nil {
		return err
	}
	var cursor mixedread.Cursor
	if err = json.Unmarshal(b, &cursor); err != nil {
		return err
	}
	if err = cursor.Validate(); err != nil {
		return err
	}
	if !cursor.Done {
		return errors.New("conversion is incomplete")
	}
	o, _, err := realOptions(legacyPath, oracle.Now)
	if err != nil {
		return err
	}
	legacy, err := chunkstore.Open(legacyPath, o)
	if err != nil {
		return err
	}
	defer legacy.Close()
	db, err := pebble.Open(filepath.Join(targetPath, "parts"), &pebble.Options{ReadOnly: true})
	if err != nil {
		return err
	}
	defer db.Close()
	queries := make(map[string]realQuery, len(oracle.Queries))
	for _, q := range oracle.Queries {
		queries[q.Name] = q
	}
	// The frozen oracle supplies query identities, never the converted schema.
	// Reconstruct the latter from actual target payloads and compare it below.
	metrics := make(map[string]partsread.Metric, len(oracle.Queries))
	catalog := partsread.CatalogFuncs{LookupFunc: func(n string) (partsread.Metric, bool) { m, ok := metrics[n]; return m, ok }, FindFunc: func(ctx context.Context, p string, y func(string) error) error {
		for _, q := range oracle.Queries {
			if p == "*" || p == q.Name {
				if err := y(q.Name); err != nil {
					return err
				}
			}
		}
		return ctx.Err()
	}}
	var blocks []partsread.SlotBlock
	it, err := db.NewIter(nil)
	if err != nil {
		return err
	}
	for ok := it.First(); ok; ok = it.Next() {
		key, e := partsformat.Decode(it.Key())
		if e != nil {
			_ = it.Close()
			return e
		}
		q, ok := queries[string(key.Metric)]
		if !ok {
			continue
		}
		stored, archive, ps, e := partsconvert.DecodeArchive(it.Value())
		if e != nil {
			_ = it.Close()
			return e
		}
		m := partsread.Metric{ID: q.Metric.ID, AggregationMethod: chunkstore.AggregationMethod(stored.Method).String(), XFilesFactor: math.Float32frombits(stored.XFilesFactorBits)}
		for _, a := range stored.Archives {
			m.Archives = append(m.Archives, partsread.Archive{Step: int64(a.Step), Points: int64(a.Slots)})
		}
		if archive != key.Archive || m.AggregationMethod != q.Metric.AggregationMethod || math.Float32bits(m.XFilesFactor) != math.Float32bits(q.Metric.XFilesFactor) || !slices.Equal(m.Archives, q.Metric.Archives) {
			_ = it.Close()
			return fmt.Errorf("converted policy differs for %s", q.Name)
		}
		metrics[q.Name] = m
		a := m.Archives[key.Archive]
		if len(ps) > 0 {
			blocks = append(blocks, partsread.SlotBlock{ID: m.ID, Archive: a, Slots: partsread.SlotRange{From: 0, Until: a.Points}, Key: bytes.Clone(it.Key())})
		}
	}
	if err = errors.Join(it.Error(), it.Close()); err != nil {
		return err
	}
	if len(metrics) != len(queries) {
		return errors.New("sampled names are missing from the converted archive catalogue")
	}
	index, err := partsread.NewSlotIndex(blocks, func(ctx context.Context, key []byte, _ []partsread.SlotRange, y func(partsread.Sample) error) error {
		v, closer, e := db.Get(key)
		if e != nil {
			return e
		}
		defer closer.Close()
		_, _, ps, e := partsconvert.DecodeArchive(v)
		if e != nil {
			return e
		}
		for _, p := range ps {
			if e := ctx.Err(); e != nil {
				return e
			}
			if e := y(partsread.Sample{Time: p.Timestamp, Bits: p.ValueBits, Sequence: 1}); e != nil {
				return e
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	publication := new(partsread.Publication)
	publication.Publish(partsread.View{Catalog: catalog, Parts: []partsread.Source{index}, RetainedBytes: index.RetainedBytes()})
	defer publication.Close()
	reader, err := mixedread.New(mixedread.Options{Legacy: legacy, LegacyCatalog: catalog, UnifiedCatalog: true, Converted: &cursor, Now: func() time.Time { return time.Unix(oracle.Now, 0) }, Parts: partsread.Adapter{Catalog: publication, Select: func(string) *partsread.Publication { return publication }, Budget: partsread.NewBudget(1 << 30), WorkBytes: 64 << 20}})
	if err != nil {
		return err
	}
	var legacyNS, capturedNS, convertedNS []int64
	for i, q := range oracle.Queries {
		converted := func() error {
			started := time.Now()
			err := reader.Fetch(ctx, q.Name, q.From, q.Until, func(s *partsread.Series) error { return compareRealSeries(q, s.From, s.Until, s.Step, s.Values) })
			convertedNS = append(convertedNS, time.Since(started).Nanoseconds())
			return err
		}
		live := func() error {
			started := time.Now()
			err := sourceReal(ctx, sourceURL, q)
			legacyNS = append(legacyNS, time.Since(started).Nanoseconds())
			return err
		}
		if q.LegacyFetchNS > 0 {
			capturedNS = append(capturedNS, q.LegacyFetchNS)
		}
		// Alternate which arm runs first. Both arms include their exact bit
		// comparison; the live arm additionally includes HTTP/protobuf work.
		if sourceURL != "" && i%2 == 0 {
			if err = live(); err != nil {
				return err
			}
		}
		if err = converted(); err != nil {
			return err
		}
		if sourceURL != "" && i%2 != 0 {
			if err = live(); err != nil {
				return err
			}
		}
	}
	comparison := "sequential single-pass; legacy before deletion, converted after; engine cache and OS cache uncontrolled; converted includes exact comparison"
	if sourceURL == "" {
		legacyNS = capturedNS
	} else {
		comparison = "paired same-query alternating order; live legacy HTTP/protobuf versus offline converted in-process; both include exact comparison; engine and OS caches uncontrolled; not an equal-transport engine latency comparison"
	}
	percentile := func(values []int64, percent int) int64 {
		if len(values) == 0 {
			return 0
		}
		slices.Sort(values)
		return values[(len(values)*percent+99)/100-1]
	}
	return json.NewEncoder(os.Stdout).Encode(struct {
		Names                       int
		Seed                        int64
		MixedExact, LiveSourceExact bool
		LegacyP99NS, ConvertedP99NS int64
		CapturedLegacyP99NS         int64
		PairedQueries               int
		LatencyComparison           string
	}{len(oracle.Queries), oracle.Seed, true, sourceURL != "", percentile(legacyNS, 99), percentile(convertedNS, 99), percentile(capturedNS, 99), func() int {
		if sourceURL != "" {
			return len(oracle.Queries)
		}
		return 0
	}(), comparison})
}
