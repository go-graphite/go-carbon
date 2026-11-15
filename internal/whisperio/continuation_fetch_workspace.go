package whisperio

import (
	"sync"

	whisper "github.com/go-graphite/go-whisper"
)

// Whisper storedPoints copies each returned block before requesting another.
// Scratch belongs to one operation, never to a store, metric, or returned Series.
// Keep at most 1MiB per cached workspace; unusually large blocks are discarded.
const maxFetchBlockPoints = 1 << 16

type fetchBlockWorkspace struct{ points []whisper.TimeSeriesPoint }

var fetchBlockWorkspaces sync.Pool

func borrowFetchBlock(fs *continuationFiles) *fetchBlockWorkspace {
	var workspace *fetchBlockWorkspace
	if cached := fetchBlockWorkspaces.Get(); cached != nil {
		workspace = cached.(*fetchBlockWorkspace)
	} else {
		workspace = new(fetchBlockWorkspace)
	}
	fs.snapshotReads = true
	fs.snapshotBlock = workspace.points[:0]
	return workspace
}

func releaseFetchBlock(fs *continuationFiles, workspace *fetchBlockWorkspace) {
	points := fs.snapshotBlock
	fs.snapshotBlock = nil
	fs.snapshotBlockOK = false
	if cap(points) <= maxFetchBlockPoints {
		workspace.points = points[:0]
		fetchBlockWorkspaces.Put(workspace)
	}
}
