package chunkstore

// TransferMergeAppended reports merged names written by appending.
func TransferMergeAppended() int64 { return transferMergeAppended.Load() }
