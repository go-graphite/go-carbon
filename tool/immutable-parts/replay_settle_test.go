package main

import (
	"testing"
	"time"
)

func TestReplaySettlementThresholdsAndTrend(t *testing.T) {
	sample := map[string]any{"estimated_debt_bytes": uint64(58328), "compactions_in_progress": int64(0), "flushes_in_progress": int64(0), "l0_below_compaction_threshold": true, "settlement_target_file_bytes": uint64(2 << 20)}
	var s replaySettlement
	for i := range 121 {
		sample["estimated_debt_bytes"] = uint64(58328 + i*16)
		s.observe(time.Duration(i)*5*time.Second, sample)
	}
	if !s.assess(sample, true) {
		t.Fatalf("bounded frontier should settle: %v", sample)
	}
	for _, key := range []string{"compactions_in_progress", "flushes_in_progress"} {
		sample[key] = int64(1)
		if s.assess(sample, true) {
			t.Fatal("accepted active work", key)
		}
		sample[key] = int64(0)
	}
	sample["l0_below_compaction_threshold"] = false
	if s.assess(sample, true) {
		t.Fatal("accepted compaction-ready L0")
	}
	sample["l0_below_compaction_threshold"] = true
	sample["estimated_debt_bytes"] = uint64(2 << 20)
	if s.assess(sample, true) {
		t.Fatal("accepted target-sized debt")
	}
	var rising replaySettlement
	for i := range 121 {
		sample["estimated_debt_bytes"] = uint64(i * 1000)
		rising.observe(time.Duration(i)*5*time.Second, sample)
	}
	if rising.assess(sample, true) {
		t.Fatal("accepted sustained debt growth below file threshold")
	}
	if s.assess(sample, false) {
		t.Fatal("finite strict gate accepted nonzero debt")
	}
	var short replaySettlement
	short.observe(0, sample)
	short.observe(599*time.Second, sample)
	if short.assess(sample, true) {
		t.Fatal("accepted less than ten minutes")
	}
}
