// Copyright 2024 the Blobloom authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//go:build !go1.24
// +build !go1.24

package blobloom

import "sync/atomic"

// setbit sets bit (i modulo BlockBits) of b, atomically.
func setbitAtomic(b *block, i uint32) {
	bit := uint32(1) << (i % wordSize)
	p := &(*b)[(i/wordSize)%blockWords]

	for {
		old := atomic.LoadUint32(p)
		if old&bit != 0 {
			// Checking here instead of checking the return value from
			// the CAS is between 50% and 80% faster on the benchmark.
			return
		}
		atomic.CompareAndSwapUint32(p, old, old|bit)
	}
}
