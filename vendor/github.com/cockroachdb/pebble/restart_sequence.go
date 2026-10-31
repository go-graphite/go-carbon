// Copyright 2026 The Pebble Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package pebble

// VisibleSequence returns the exclusive committed sequence fence. It is only
// a checkpoint identity when callers have stopped all database writers.
func (d *DB) VisibleSequence() uint64 { return d.mu.versions.visibleSeqNum.Load() }
