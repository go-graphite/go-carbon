package carbon

import (
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"testing/synctest"

	"github.com/go-graphite/go-carbon/cache"
	"github.com/go-graphite/go-carbon/points"
)

// A decoder callback may return while its worker still owns the last record.
// Neither deletion nor admission from the next source may cross that boundary.
func TestRestoreWorkersJoinBeforeRetiringFile(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		root := t.TempDir()
		cacheFile := filepath.Join(root, "cache.1.100.bin")
		inputFile := filepath.Join(root, "input.1.100.bin")
		for name, p := range map[string]*points.Points{cacheFile: points.OnePoint("old", 1, 1), inputFile: points.OnePoint("new", 2, 2)} {
			if err := os.WriteFile(name, p.AppendBinary(nil), 0600); err != nil {
				t.Fatal(err)
			}
		}
		core := cache.New()
		entered, release, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
		var mu sync.Mutex
		var seen []string
		core.DivertToPointWriter(func(p *points.Points) error {
			if p.Metric == "old" {
				close(entered)
				<-release
			}
			mu.Lock()
			seen = append(seen, p.Metric)
			mu.Unlock()
			return nil
		})
		core.SetRecoveryDrain(true)
		go func() { app := &App{Cache: core}; app.Restore(core.AddRestoredBeforeInput, root, 0); close(done) }()
		<-entered
		synctest.Wait()
		for _, name := range []string{cacheFile, inputFile} {
			if _, err := os.Stat(name); err != nil {
				t.Fatal("source retired before worker finished", err)
			}
		}
		mu.Lock()
		n := len(seen)
		mu.Unlock()
		if n != 0 {
			t.Fatal("later source crossed unfinished source")
		}
		close(release)
		<-done
		if !reflect.DeepEqual(seen, []string{"old", "new"}) {
			t.Fatal("source order", seen)
		}
		for _, name := range []string{cacheFile, inputFile} {
			if _, err := os.Stat(name); !os.IsNotExist(err) {
				t.Fatal("source not retired after joined admission", err)
			}
		}
	})
}
