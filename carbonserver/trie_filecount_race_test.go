package carbonserver

import (
	"fmt"
	"sync"
	"testing"
)

// TestTrieFileCountConcurrentList lists metrics while realtime inserts add
// files, as /metrics/list/ does on a live host. Run with -race.
func TestTrieFileCountConcurrentList(t *testing.T) {
	t.Parallel()
	trie := newTrie(".wsp", 0, nil)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 2000; i++ {
			if _, err := trie.insert(fmt.Sprintf("/sys/app/host-%04d/cpu.wsp", i), 0, 0, 0, 0); err != nil {
				t.Error(err)
				return
			}
		}
	}()
	for i := 0; i < 200; i++ {
		_ = trie.allMetricsMutable('.')
	}
	wg.Wait()
	if got := trie.getFileCount(); got != 2000 {
		t.Fatalf("file count = %d, want 2000", got)
	}
}
