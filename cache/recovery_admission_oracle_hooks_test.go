package cache

// The differential runner replaces this file with legacy equivalents in its
// base-source overlay. The input, driver, collection and bit hash stay identical.
func configureRecoveryOracle(c *Cache) {
	c.SetRecoveryMemory(func() (uint64, bool) { return 1 << 40, true })
}

func recoveryOracleAdmit(c *Cache, name string, count int) bool {
	return c.HasRecoveryCapacityFor(name, count)
}
