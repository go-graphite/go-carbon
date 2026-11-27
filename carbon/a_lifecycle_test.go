package carbon

import "testing"

// Start the longest independent matrices first so their waits overlap the
// shorter application tests. Each matrix owns its own process and case limit.
func TestPendingReadsBeforePersistenceAndInput(t *testing.T) {
	t.Parallel() // Children own isolated processes in the bounded storage pool.
	testPendingReadFormats(t)
}

// Senders in these matrices have explicit phase barriers. Bound storage
// processes with the same pool as other fixtures instead of serializing whole
// matrices behind a process-global lock.
func TestStoreInputFallbackKeepsEveryPoint(t *testing.T) {
	if isolatedTestProcess(t) {
		return
	}
	testStoreInputFallbackKeepsEveryPoint(t)
}

func TestSharedStoreRestartHandsInputOver(t *testing.T) {
	if isolatedTestProcess(t) {
		return
	}
	testSharedStoreRestartHandsInputOver(t)
}

func TestRestartHandsInputOver(t *testing.T) {
	if isolatedTestProcess(t) {
		return
	}
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
	parallelTestMatrix(t)
	testIncrementalBuckydPreservesOwnershipAndMixedIndex(t)
}
