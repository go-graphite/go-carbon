package persister

// MigrationMetrics is an advisory, nonblocking snapshot for HTTP collectors.
// ioMu can span opening a directory at startup/reload; do not wait behind it.
func (p *Whisper) MigrationMetrics() (out struct {
	IOUsed, IOSlots int
	ReclaimQueued   int64
	Reclaim         DeferredReclaimStats
}, ok bool) {
	if !p.ioMu.TryRLock() {
		return out, false
	}
	r, slots := p.reclaim, p.conversionIO
	p.ioMu.RUnlock()
	if slots != nil {
		if !slots.mu.TryLock() {
			return out, false
		}
		out.IOUsed, out.IOSlots = slots.used, slots.limit
		slots.mu.Unlock()
	}
	if r != nil {
		out.ReclaimQueued = r.queued.Load()
		out.Reclaim = r.stats()
	}
	return out, true
}
