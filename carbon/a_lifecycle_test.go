package carbon

import "testing"

// Start the longest independent matrices first so their waits overlap the
// shorter application tests. Each matrix owns its own process and case limit.
func TestPendingReadsBeforePersistenceAndInput(t *testing.T) {
	testPendingReadFormats(t)
}

func TestStoreInputFallbackKeepsEveryPoint(t *testing.T) {
	testStoreInputFallbackKeepsEveryPoint(t)
}

func TestSharedStoreRestartHandsInputOver(t *testing.T) {
	if isolatedTestProcess(t) {
		return
	}
	testSharedStoreRestartHandsInputOver(t)
}

func TestRestartHandsInputOver(t *testing.T) {
	testRestartHandsInputOver(t)
}

func TestShutdownWaitsForActiveRead(t *testing.T) {
	if isolatedTestProcess(t) {
		return
	}
	testShutdownWaitsForActiveRead(t)
}

func TestDumpStopStopsPersisterAlongsideWarmup(t *testing.T) {
	if isolatedTestProcess(t) {
		return
	}
	testDumpStopStopsPersisterAlongsideWarmup(t)
}

func TestIncrementalBuckydPreservesOwnershipAndMixedIndex(t *testing.T) {
	if isolatedTestProcess(t) {
		return
	}
	testIncrementalBuckydPreservesOwnershipAndMixedIndex(t)
}
