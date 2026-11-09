package chunkstore

// These helpers exist only in the package's test build, allowing the external
// differential suite to exercise the real continuation adapter at crash points.
func (s *Store) NativeCombineHookForTest(hook func(string) error) { s.nativeTestHook = hook }
func (s *Store) NativeCombineSyncForTest() error                  { return s.syncWAL() }
