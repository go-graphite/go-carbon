package carbon

import (
	"testing"

	"github.com/go-graphite/go-carbon/cache"
)

func TestRestartReadinessWaitsForRecoveryBoundaries(t *testing.T) {
	core := cache.New()
	restored, warmed := make(chan struct{}), make(chan struct{})
	capture := &startupInput{}
	capture.points.Store(100)
	capture.replayedPoints.Store(100)
	capture.size.Store(4096)
	if restartRecoveryReady(core, restored, warmed, capture) {
		t.Fatal("startup passed before restoration")
	}
	close(restored)
	if restartRecoveryReady(core, restored, warmed, capture) {
		t.Fatal("startup passed before warm-up")
	}
	close(warmed)
	if restartRecoveryReady(core, restored, warmed, capture) {
		t.Fatal("caught-up capture was treated as a completed cutover")
	}
	capture.direct.Store(true)
	if restartRecoveryReady(core, restored, warmed, capture) {
		t.Fatal("unretired capture sources passed the restart boundary")
	}
	capture.retiredBytes.Store(4096)
	if !restartRecoveryReady(core, restored, warmed, capture) {
		t.Fatal("completed recovery did not become ready")
	}
	capture.fallback.Store(true)
	if restartRecoveryReady(core, restored, warmed, capture) {
		t.Fatal("capture failure was hidden by a completed cutover")
	}
}
