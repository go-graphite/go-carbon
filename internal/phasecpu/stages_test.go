package phasecpu

import "testing"

func TestStageCPUAttribution(t *testing.T) {
	for _, tt := range []struct {
		stage, pebble string
		want          bool
	}{
		{"commit", "", true}, {"decode", "", true}, {"other", "", true},
		{"unknown", "", false}, {"commit", "flush", false}, {"commit", "wal-sync", false},
	} {
		var stages [len(PersisterStageNames)]uint64
		_, err := decodeStages(fixture([2]string{"go_carbon_phase", "persister"}, [2]string{"persister_stage", tt.stage}, [2]string{"pebble", tt.pebble}), &stages)
		if err != nil {
			t.Fatal(err)
		}
		var sum uint64
		for _, n := range stages {
			sum += n
		}
		if (sum == 70000000) != tt.want || (!tt.want && sum != 0) {
			t.Fatalf("%+v: %v", tt, stages)
		}
	}
}
