package v2core

import "testing"

func TestSettlementProbePersistedOptions(t *testing.T) {
	e, err := Open(t.TempDir(), Config{AppendParts: true, Shards: 2, Producers: 1, MaxRows: 100, PartitionSeconds: 3600}, Hooks{})
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	p, err := NewSettlementProbe(e)
	if err != nil {
		t.Fatal(err)
	}
	s := p.Sample()
	if s["settlement_target_file_bytes"].(uint64) == 0 || !s["l0_below_compaction_threshold"].(bool) || len(s["l0_compaction_thresholds"].([]int)) != 2 {
		t.Fatal(s)
	}
}
