package carbon

import (
	"testing"
	"time"
)

func TestStartupCapacityAdaptsWithinCeilings(t *testing.T) {
	small := chooseStartupPolicy(2, 128<<20, 8<<30)
	large := chooseStartupPolicy(128, 512<<30, 8<<40)
	if large.shards <= small.shards || large.readBuffer <= small.readBuffer || large.recoveryPoints <= small.recoveryPoints || large.spoolBytes <= small.spoolBytes {
		t.Fatal("capacity did not adapt", small, large)
	}
	if large.shards > 64 || large.readBuffer > 4<<20 || large.writeBuffer > 2<<20 || large.recoveryBytes > 8<<30 || large.spoolBytes > 128<<30 {
		t.Fatal("unsafe ceiling", large)
	}
	constrained := chooseStartupPolicy(128, 32<<20, 64<<20)
	if constrained.shards > 4 || constrained.spoolBytes > 8<<20 || constrained.recoveryBytes > 32<<20 {
		t.Fatal("low-resource host overcommitted", constrained)
	}
	if captureTimeLimit(128<<30, 1<<30) != 120*time.Second || captureTimeLimit(1<<30, 4<<30) != 10*time.Second || captureTimeLimit(128<<30, 1) != 120*time.Second {
		t.Fatal("capture duration is not bounded")
	}
}
