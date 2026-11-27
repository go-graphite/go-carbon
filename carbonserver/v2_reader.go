package carbonserver

import "github.com/go-graphite/go-carbon/internal/v2read"

// SetV2Reader selects canonical snapshot reads before serving requests. Runtime
// source changes require a restart, as do the writer and discovery source.
func (listener *CarbonserverListener) SetV2Reader(reader *v2read.Reader) {
	listener.v2Reader = reader
	if reader != nil {
		// Existing render caches have no v2 snapshot/frontier identity. Disable
		// them until cache keys bind the same immutable publication as the reader.
		listener.SetQueryCacheEnabled(false)
		listener.SetStreamingQueryCacheEnabled(false)
		_ = listener.SetQueryRangeCache(false, 0, 0)
	}
}
