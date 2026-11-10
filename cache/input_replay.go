package cache

// inputReplay holds live writeout behind a finite crash history. The history
// cache drains while its reader is still decoding: holding the entire log in
// memory, or holding persistence until decoding finishes, can deadlock bounded
// recovery admission. Live points keep their own input-log generations.
type inputReplay struct {
	history *Cache
	done    chan struct{}
}

// BeginInputReplay must run before receivers and persisters start. History must
// use a separate bounded cache, with no input log: the reader retains its durable
// original segments until every history batch is confirmed. This also preserves
// correction order across a second crash (relogging old values after live values
// would reverse that order in the next replay).
func (c *Cache) BeginInputReplay(history *Cache) {
	c.inputReplay.Store(&inputReplay{history: history, done: make(chan struct{})})
}

// FinishInputReplay runs after the reader has joined and history is empty,
// including in-flight writes. No history confirmations may remain at this point.
func (c *Cache) FinishInputReplay() {
	r := c.inputReplay.Swap(nil)
	if r != nil {
		close(r.done)
	}
}
