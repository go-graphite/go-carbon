package carbon

import (
	"testing"
	"time"
)

func TestStartupCapacityAdaptsWithinCeilings(t *testing.T) {
	small := chooseStartupPolicy(2, 128<<20, 8<<30)
	large := chooseStartupPolicy(192, 1280<<30, 13<<40)
	if large.shards <= small.shards || large.readBuffer <= small.readBuffer || large.recoveryPoints <= small.recoveryPoints || large.spoolBytes <= small.spoolBytes {
		t.Fatal(small, large)
	}
	if large.readBuffer > 4<<20 || large.writeBuffer > 2<<20 || large.recoveryBytes != 320<<30 || large.spoolBytes != (13<<40)/4 {
		t.Fatal(large)
	}
	if captureTimeLimit(128<<30, 1<<30) != 128*time.Second || captureTimeLimit(1<<30, 4<<30) != time.Second/4 || captureTimeLimit(128<<30, 0) >= 0 {
		t.Fatal("convergence ETA capped or undefined margin hidden")
	}
}
