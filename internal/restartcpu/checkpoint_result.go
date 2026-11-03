package restartcpu

import (
	"context"
	"errors"
)

// RecordCheckpointLoad records the final result, including legacy validation.
// A missing/invalid enabled accelerator is a failure even without an I/O error.
func (b *Budget) RecordCheckpointLoad(valid bool, err error) {
	if b == nil || valid && err == nil {
		return
	}
	b.checkpointFailures.Add(1)
	if errors.Is(err, context.DeadlineExceeded) {
		b.checkpointTimeouts.Add(1)
	}
}
