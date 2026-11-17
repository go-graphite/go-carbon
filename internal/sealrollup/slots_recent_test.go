package sealrollup

import (
	"math"
	"reflect"
	"testing"
)

func TestSlotsRecentCompleteness(t *testing.T) {
	config := Config{Archives: []Archive{{1, 120}, {10, 120}, {60, 120}}, Method: Average}
	prepared, err := Prepare(config)
	if err != nil {
		t.Fatal(err)
	}
	full, hot := prepared.NewSlots(), prepared.NewSlots()
	for epoch := int64(1); epoch <= 400; epoch++ {
		for a, ar := range config.Archives {
			stamp := epoch - epoch%ar.Step
			d := Delta{Archive: a, Slot: (stamp / ar.Step) % ar.Count, Sample: Sample{Time: stamp, Bits: math.Float64bits(float64(epoch)), Sequence: uint64(epoch)}}
			full.Install(d)
			hot.InstallRecent(d)
		}
		hot.KeepRecent(epoch)
		for a := 0; a < len(config.Archives)-1; a++ {
			step := config.Archives[a+1].Step
			for start := int64(0); start <= epoch; start += step {
				got, complete := hot.RecentRange(a, start, start+step, nil)
				if complete && !reflect.DeepEqual(got, full.Range(a, start, start+step, nil)) {
					t.Fatalf("false completeness epoch=%d archive=%d start=%d got=%v want=%v", epoch, a, start, got, full.Range(a, start, start+step, nil))
				}
			}
			bound := int(config.Archives[a+1].Step/config.Archives[a].Step) + 2
			if len(hot.data[a].data) > bound {
				t.Fatal("recent window bound exceeded")
			}
			start := epoch - epoch%config.Archives[a+1].Step
			if _, complete := hot.RecentRange(a, start, start+config.Archives[a+1].Step, nil); !complete {
				t.Fatal("open window was evicted")
			}
		}
	}
}
