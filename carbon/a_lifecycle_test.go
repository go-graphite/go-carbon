package carbon

import "testing"

// Start the longest independent matrices first so their waits overlap the
// shorter application tests. Each matrix owns its own process and case limit.
func TestPendingReadsBeforePersistenceAndInput(t *testing.T) {
	t.Parallel() // Children own isolated processes in the bounded storage pool.
	testPendingReadFormats(t)
}

func TestStoreInputFallbackKeepsEveryPoint(t *testing.T) {
	parallelTestMatrix(t)
	// Own the storage pool across the whole matrix: unrelated fixtures must not
	// consume the bounded sender buffer between its successive restart cases.
	if isolatedTestProcessSerial(t) {
		return
	}
	testStoreInputFallbackKeepsEveryPoint(t)
}

func TestSharedStoreRestartHandsInputOver(t *testing.T) {
	parallelTestMatrix(t)
	// Keep one exclusive fixture across the matrix, including the intervals
	// between cases, instead of reacquiring it behind unrelated startups.
	if isolatedTestProcessSerial(t) {
		return
	}
	testSharedStoreRestartHandsInputOver(t)
}

func TestRestartHandsInputOver(t *testing.T) {
	parallelTestMatrix(t) // Each subtest owns an isolated process.
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
