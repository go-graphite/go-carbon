package main

import (
	"bytes"
	"context"
	"encoding/json"
	"math"
	"sort"
	"strings"
	"testing"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/internal/whisperio"
	"github.com/go-graphite/go-carbon/receiver/stream/codec"
)

type fixtureIO map[string][]byte

func (m fixtureIO) Get(k []byte) ([]byte, error) {
	v, ok := m[string(k)]
	if !ok {
		return nil, store.ErrNotFound
	}
	return bytes.Clone(v), nil
}
func (m fixtureIO) Set(k, v []byte) error { m[string(k)] = bytes.Clone(v); return nil }
func (m fixtureIO) Delete(k []byte) error { delete(m, string(k)); return nil }
func (m fixtureIO) DeletePrefix(k []byte) error {
	for key := range m {
		if bytes.HasPrefix([]byte(key), k) {
			delete(m, key)
		}
	}
	return nil
}
func (m fixtureIO) Visit(prefix []byte, visit func([]byte, []byte) error) error {
	keys := []string{}
	for k := range m {
		if bytes.HasPrefix([]byte(k), prefix) {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	for _, k := range keys {
		if err := visit([]byte(k), m[k]); err != nil {
			return err
		}
	}
	return nil
}
func TestNativeSnapshotsMatchPhysicalBits(t *testing.T) {
	for _, continuation := range []bool{false, true} {
		ctx := context.Background()
		config := store.MetricConfig{Name: "metric", Retentions: []store.Retention{{Step: 10, Count: 20}}, AggregationMethod: store.Average, XFilesFactor: .5}
		input := store.Snapshot{Metadata: store.Metadata{MetricConfig: config, ID: 1, Generation: 1, Revision: 1, Continuation: continuation}, Archives: []store.Archive{{Retention: config.Retentions[0], Points: []store.Point{{Timestamp: 100, Value: math.Copysign(0, -1)}, {Timestamp: 110, Value: math.Float64frombits(0x7ff8000000000042)}, {Timestamp: 120, Value: 1}}}}}
		memory := fixtureIO{}
		if continuation {
			if err := (&whisperio.ContinuationCodec{}).RestoreArchives(ctx, memory, input, func() time.Time { return time.Unix(200, 0) }); err != nil {
				t.Fatal(err)
			}
		} else {
			var block store.PointBlock
			for i, p := range input.Archives[0].Points {
				block.Present[0] |= 1 << i
				block.Points[i] = p
			}
			memory[string(make([]byte, 6))] = store.EncodePointBlock(&block)
		}
		record := store.TransferMetric{Config: config, Version: store.TransferVersion{Name: "metric", ID: 1, Generation: 1, Revision: 1}, Continuation: continuation}
		if err := memory.Visit(nil, func(k, v []byte) error {
			record.Keys = append(record.Keys, store.RelayoutKV{Key: k, Value: v})
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		var wire bytes.Buffer
		if err := store.WriteTransfer(&wire, []store.TransferMetric{record}); err != nil {
			t.Fatal(err)
		}
		result, err := decode(bytes.NewReader(wire.Bytes()), []window{{From: 95, Until: 115}})
		if err != nil {
			t.Fatal(err)
		}
		points := result["metric"].Archives[0].Points
		if len(points) != 2 || points["100"] != "8000000000000000" || points["110"] != "7ff8000000000042" {
			t.Fatal(points)
		}
		if result["metric"].Version != nil || result["metric"].NativeConfig != nil {
			t.Fatal("default proof unexpectedly exposes identity")
		}
		full, err := decodeIdentity(bytes.NewReader(wire.Bytes()), nil, true)
		if err != nil {
			t.Fatal(err)
		}
		got := full["metric"]
		if got.Version == nil || *got.Version != record.Version || got.NativeConfig == nil || got.NativeConfig.Name != config.Name || len(got.Archives[0].Points) != 3 || got.Archives[0].Points["120"] != "3ff0000000000000" {
			t.Fatalf("full physical identity/points lost: %+v", got)
		}
		raw := bytes.Clone(wire.Bytes())
		raw[len(raw)-1] ^= 1
		if _, err = decode(bytes.NewReader(raw), nil); err == nil {
			t.Fatal("bad native checksum accepted")
		}
	}
}

func TestStreamRoundTripsEveryFloatBit(t *testing.T) {
	input := []inputPoint{{Name: "metric", Timestamp: 100, Bits: "8000000000000000"}, {Name: "metric", Timestamp: 110, Bits: "7ff8000000000042"}, {Name: "metric", Timestamp: 120, Bits: "7ff8000000000043"}}
	raw, _ := json.Marshal(input)
	wire, err := stream(bytes.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = codec.ParsePreamble(wire[:codec.PreambleSize]); err != nil {
		t.Fatal(err)
	}
	decoder, err := codec.NewDecoder(codec.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer decoder.Close()
	var values []uint64
	_, err = decoder.ReadFrame(bytes.NewReader(wire[codec.PreambleSize:]), func(_ uint32, _ []byte, v float64, _ int64, _ *any) { values = append(values, math.Float64bits(v)) })
	if err != nil {
		t.Fatal(err)
	}
	if len(values) != 3 || values[0] != 0x8000000000000000 || values[1] != 0x7ff8000000000042 || values[2] != 0x7ff8000000000043 {
		t.Fatal(values)
	}
}

func TestLegacyNodeRejectionEvidenceUsesStorePolicy(t *testing.T) {
	for _, tc := range []struct {
		name     string
		rejected int
	}{
		{"long." + strings.Repeat("x", 300), 0},
		{"long." + strings.Repeat("x", 4096), 0},
		{"valid." + strings.Repeat("x", 255), 0},
		{"tagged.metric;job=" + strings.Repeat("x", 1000), 0},
	} {
		r := store.TransferMetric{Config: store.MetricConfig{Name: tc.name, Retentions: []store.Retention{{Step: 10, Count: 20}}, AggregationMethod: store.Average}, Version: store.TransferVersion{Name: tc.name, ID: 1, Generation: 1, Revision: 1}}
		var wire bytes.Buffer
		if err := store.WriteTransfer(&wire, []store.TransferMetric{r}); err != nil {
			t.Fatal(err)
		}
		result, err := decode(&wire, nil)
		if err != nil {
			t.Fatal(err)
		}
		if result[tc.name].RejectedNodeBytes != tc.rejected {
			t.Fatal(tc, result[tc.name])
		}
	}
}

func TestIdenticalPhysicalDuplicatesKeepBitsAndReportMultiplicity(t *testing.T) {
	var block store.PointBlock
	block.Present[0] = 3
	block.Points[0] = store.Point{Timestamp: 100, Value: math.Copysign(0, -1)}
	block.Points[1] = block.Points[0]
	r := store.TransferMetric{Config: store.MetricConfig{Name: "duplicate", Retentions: []store.Retention{{Step: 10, Count: 20}}, AggregationMethod: store.Average}, Version: store.TransferVersion{Name: "duplicate", ID: 1, Generation: 1, Revision: 1}}
	encode := func() []byte {
		r.Keys = []store.RelayoutKV{{Key: make([]byte, 6), Value: store.EncodePointBlock(&block)}}
		var wire bytes.Buffer
		if err := store.WriteTransfer(&wire, []store.TransferMetric{r}); err != nil {
			t.Fatal(err)
		}
		return wire.Bytes()
	}
	result, err := decode(bytes.NewReader(encode()), nil)
	if err != nil {
		t.Fatal(err)
	}
	a := result["duplicate"].Archives[0]
	if a.Points["100"] != "8000000000000000" || a.DuplicatePoints != 1 {
		t.Fatal(a)
	}
	block.Points[1].Value = 0
	if _, err = decode(bytes.NewReader(encode()), nil); err == nil {
		t.Fatal("conflicting signed zero duplicates accepted")
	}
}

// This is the rsys owner's real codec regression: a stale physical alias is
// unrepairable by rotation alone, while ordinary retained-window writes are
// readable and preserve every previously readable value, including signed zero.
func TestBlockedAliasOrdinaryGapWritesWithoutReconciliation(t *testing.T) {
	ctx := context.Background()
	config := store.MetricConfig{Name: "alias", Retentions: []store.Retention{{Step: 30, Count: 20}}, AggregationMethod: store.Average, XFilesFactor: .5}
	s := store.Snapshot{Metadata: store.Metadata{MetricConfig: config, ID: 1, Generation: 1, Revision: 1, Continuation: true}, Archives: []store.Archive{{Retention: config.Retentions[0], Points: []store.Point{{Timestamp: 999780, Value: 1}, {Timestamp: 999990, Value: 2}}}}}
	memory := fixtureIO{}
	c := &whisperio.ContinuationCodec{}
	clock := func() time.Time { return time.Unix(1000400, 0) }
	if err := c.RestoreArchives(ctx, memory, s, clock); err != nil {
		t.Fatal(err)
	}
	for k, v := range memory {
		key := []byte(k)
		if len(key) == 20 && key[9] == 'p' {
			var block store.PointBlock
			if err := store.DecodePointBlock(v, &block); err != nil {
				t.Fatal(err)
			}
			block.Present[0] |= 1 << 5
			block.Points[5] = store.Point{Timestamp: 1000380, Value: 9}
			memory[k] = store.EncodePointBlock(&block)
		}
	}
	plan, err := c.PlanArchiveAnchors(ctx, memory)
	if err != nil || len(plan.Archives) != 1 || plan.Archives[0].Blocked == "" {
		t.Fatalf("expected blocked alias: %+v %v", plan, err)
	}
	read := func() map[int64]uint64 {
		series, err := c.Fetch(ctx, memory, s.Metadata, 999800, 1000399, clock)
		if err != nil {
			t.Fatal(err)
		}
		out := map[int64]uint64{}
		for i, value := range series.Values {
			if !math.IsNaN(value) {
				out[int64(series.FromTime+i*series.Step)] = math.Float64bits(value)
			}
		}
		return out
	}
	before := read()
	points := []store.Point{{Timestamp: 999990, Value: 2}, {Timestamp: 1000320, Value: math.Copysign(0, -1)}}
	if err := c.Update(ctx, memory, s.Metadata, points, -1, clock, nil); err != nil {
		t.Fatal(err)
	}
	after := read()
	for ts, bits := range before {
		if after[ts] != bits {
			t.Fatalf("existing read changed: %d", ts)
		}
	}
	for _, point := range points {
		bits, ok := after[point.Timestamp]
		if !ok || bits != math.Float64bits(point.Value) {
			t.Fatalf("gap fill differs at %d", point.Timestamp)
		}
	}
}
