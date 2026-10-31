package cache

// SetRestartWarmFirst prioritizes ready state while SetRestartOrder is active.
// Both partitions keep oldest-point order. begin/end admit queue rebuild CPU
// through the same allowance as writes, without holding cache or store locks.
// begin returns false only when its consumer is stopping. Configure before
// starting the queue consumer; nil ready disables this scheduling.
func (c *Cache) SetRestartWarmFirst(ready func(string) bool, begin func(<-chan bool) bool, end func()) {
	c.mu.Lock()
	c.restartWarm, c.restartBegin, c.restartEnd = ready, begin, end
	c.mu.Unlock()
}

func (c *Cache) restartScheduling() (bool, func(<-chan bool) bool, func()) {
	c.mu.Lock()
	active, warm, begin, end := c.restartOrder, c.restartWarm, c.restartBegin, c.restartEnd
	c.mu.Unlock()
	return active != nil && warm != nil && active(), begin, end
}
