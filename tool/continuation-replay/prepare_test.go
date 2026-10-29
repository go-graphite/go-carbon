package main

import (
	"encoding/binary"
	"encoding/json"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

func TestIngressPreparationPreservesLineOrderAndBits(t *testing.T) {
	planPath, _, plan := replayFixture(t)
	trace := filepath.Join(t.TempDir(), "ingress.trace")
	f, err := os.Create(trace)
	if err != nil {
		t.Fatal(err)
	}
	put := func(value any) {
		t.Helper()
		if err := binary.Write(f, binary.LittleEndian, value); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := f.Write([]byte("GTREPL01")); err != nil {
		t.Fatal(err)
	}
	put(uint32(len(plan.Rows)))
	put(uint64(48))
	put(plan.StartNS)
	put(plan.StartNS + int64(24*time.Millisecond))
	for _, row := range plan.Rows {
		put(uint32(len(row.Metric)))
		if _, err := f.Write([]byte(row.Metric)); err != nil {
			t.Fatal(err)
		}
	}
	want := make(map[int][]replayPoint)
	for i := 0; i < 24; i++ {
		for metric := range plan.Rows {
			point := replayPoint{Timestamp: plan.StartNS/int64(time.Second) - 1 + int64(i%3), Bits: math.Float64bits(float64(i) + float64(metric)/10)}
			if i == 7 {
				point.Bits = 1 << 63 // Preserve negative zero through cache preparation.
			}
			want[metric] = append(want[metric], point)
			// Equal packet times retain the original line sequence.
			put(plan.StartNS + int64(i/2)*int64(time.Millisecond))
			put(uint32(metric))
			put(point.Timestamp)
			put(point.Bits)
		}
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	operations := filepath.Join(t.TempDir(), "prepared.jsonl")
	if err := prepareIngress(planPath, trace, operations); err != nil {
		t.Fatal(err)
	}
	got := make(map[int][]replayPoint)
	if err := eachOperation(operations, plan, func(_ int, op replayOperation) error {
		if op.Kind == "write" {
			got[op.Metric] = append(got[op.Metric], op.Points...)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(want, got) {
		t.Fatal("cache preparation changed timestamps, float bits or duplicate order")
	}
	data, err := os.ReadFile(operations + ".preparation.json")
	if err != nil {
		t.Fatal(err)
	}
	var receipt struct {
		Points uint64 `json:"ingress_points"`
		Writes uint64 `json:"write_batches"`
	}
	if err := json.Unmarshal(data, &receipt); err != nil {
		t.Fatal(err)
	}
	if receipt.Points != 48 || receipt.Writes >= receipt.Points || receipt.Writes < 2 {
		t.Fatal("writeout policy was bypassed")
	}
	for _, row := range plan.Rows {
		mainHash, err := fileSHA(filepath.Join(plan.SourceRoot, row.Path))
		if err != nil || mainHash != row.MainSHA256 {
			t.Fatal("preparation changed a source history")
		}
	}
	if err := prepareIngress(planPath, trace, operations); err == nil {
		t.Fatal("overwrote an existing prepared trace")
	}
	t.Run("truncated input", func(t *testing.T) {
		bad := filepath.Join(t.TempDir(), "truncated.trace")
		data, err := os.ReadFile(trace)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(bad, data[:len(data)-1], 0600); err != nil {
			t.Fatal(err)
		}
		output := filepath.Join(t.TempDir(), "must-not-exist.jsonl")
		if err := prepareIngress(planPath, bad, output); err == nil {
			t.Fatal("accepted an incomplete input")
		}
		if _, err := os.Stat(output); !os.IsNotExist(err) {
			t.Fatal("created output before validating the input")
		}
	})
}
