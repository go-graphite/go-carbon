package chunkstore

// SetTransferMergeAppendForTest switches the append merge of present
// continuation names on (the default) or off (always RestoreArchives).
func (s *Store) SetTransferMergeAppendForTest(on bool) { s.transferMergeAppendOff = !on }
