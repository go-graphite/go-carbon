package carbon

import (
	"context"
	"fmt"
	"sync"
	"testing"
)

func BenchmarkIncrementalDiskAdmission(b *testing.B) {
	for _, workers := range []int{1024, 32768} {
		b.Run(fmt.Sprintf("workers=%d", workers), func(b *testing.B) {
			m := &incrementalStoreMigration{}
			directory := b.TempDir()
			ctx := context.Background()
			release, err := m.waitDisk(ctx, directory, 4096, 4096)
			if err != nil {
				b.Fatal(err)
			}
			release()
			start := make(chan struct{})
			var ready, done sync.WaitGroup
			ready.Add(workers)
			done.Add(workers)
			b.ResetTimer()
			b.StopTimer()
			for worker := range workers {
				go func() {
					defer done.Done()
					ready.Done()
					<-start
					for i := worker; i < b.N; i += workers {
						release, err := m.waitDisk(ctx, directory, 4096, 4096)
						if err != nil {
							b.Error(err)
							return
						}
						release()
					}
				}()
			}
			ready.Wait()
			b.StartTimer()
			close(start)
			done.Wait()
			b.StopTimer()
			b.ReportAllocs()
		})
	}
}
