package downsize

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"sync"
)

const maxAbsentProofsPerSource = 8

// A slot covers the complete HEAD + immediately following fresh catalog proof.
// The mover-scoped semaphore bounds every concurrent batch for this source,
// rather than multiplying eight requests by the number of transfer workers.
func (m *mover) parallelAbsentSourceProofs(ctx context.Context, source string, names []string) ([]string, error) {
	value, _ := m.absentProofSlots.LoadOrStore(source, make(chan struct{}, maxAbsentProofsPerSource))
	slots := value.(chan struct{})
	absentByIndex := make([]bool, len(names))
	failures := make([]error, len(names))
	var mu sync.Mutex
	next, stopAfter := 0, len(names)
	var workers sync.WaitGroup
	for range min(maxAbsentProofsPerSource, len(names)) {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for {
				if ctx.Err() != nil {
					return
				}
				select {
				case slots <- struct{}{}:
				case <-ctx.Done():
					return
				}
				mu.Lock()
				if ctx.Err() != nil || next >= len(names) || next > stopAfter {
					mu.Unlock()
					<-slots
					return
				}
				index := next
				next++
				mu.Unlock()
				absent, err := m.proveSourceNameAbsent(ctx, source, names[index])
				mu.Lock()
				absentByIndex[index], failures[index] = absent, err
				if err != nil && index < stopAfter {
					stopAfter = index
				}
				mu.Unlock()
				<-slots
			}
		}()
	}
	workers.Wait()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	var absent []string
	for i, name := range names {
		if failures[i] != nil {
			return nil, failures[i]
		}
		if absentByIndex[i] {
			absent = append(absent, name)
		}
	}
	return absent, nil
}

func (m *mover) proveSourceNameAbsent(ctx context.Context, source, name string) (bool, error) {
	response, err := m.sourceAbsenceRequest(ctx, http.MethodHead, source+"/metrics/"+url.PathEscape(name))
	if err != nil {
		return false, err
	}
	_ = response.Body.Close()
	switch response.StatusCode {
	case http.StatusOK:
		return false, nil
	case http.StatusNotFound:
		if err := m.confirmSourceAbsent(ctx, source, name); err != nil {
			return false, err
		}
		return true, nil
	default:
		return false, &apiError{response.StatusCode, fmt.Sprintf("source HEAD %s: HTTP %d", name, response.StatusCode)}
	}
}
