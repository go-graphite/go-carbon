package downsize

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
)

// Keep skip records separate from completed ranges: a later HTTP413 can split
// the original task without creating overlapping checkpoint ranges.
type absentSourceSkip struct {
	Source, Stream      string
	Start, End          int64
	BatchMetrics, Count int
	Names               []string
}

func (s absentSourceSkip) key() string {
	return s.Source + ":" + taskKey(task{Stream: s.Stream, Start: s.Start, End: s.End})
}

func (j *journal) absentError() error {
	total := 0
	for _, s := range j.absent {
		total += s.Count
		// Permit one churned name even in a small batch. Larger batches have
		// a strict 1% limit; the total limit spans retries and process restarts.
		if s.Count > max(1, s.BatchMetrics/100) {
			return fmt.Errorf("absent source bound exceeded: %d/%d names in batch %s", s.Count, s.BatchMetrics, s.key())
		}
	}
	if total > 10000 {
		return fmt.Errorf("absent source bound exceeded: %d total names (limit 10000)", total)
	}
	return nil
}

func (m *mover) sourceNames(t task) ([]string, error) {
	m.stateMu.Lock()
	defer m.stateMu.Unlock()
	if err := m.journal.absentError(); err != nil {
		return nil, err
	}
	skipped := map[string]bool{}
	for _, s := range m.journal.absent {
		if s.Source == t.Source && s.Stream == t.Stream {
			for _, name := range s.Names {
				skipped[name] = true
			}
		}
	}
	var names []string
	for _, name := range t.Names {
		if !skipped[name] {
			names = append(names, name)
		}
	}
	return names, nil
}

// A pull 404 need not mean absent data (it can be a wrong API route). Confirm
// each missing name through the donor's authenticated metadata/catalog HEAD.
// Other HEAD errors retain their normal fatal behavior.
func (m *mover) skipAbsentSource(ctx context.Context, batch task, names []string) error {
	var absent []string
	for _, name := range names {
		r, err := http.NewRequestWithContext(ctx, http.MethodHead, batch.Source+"/metrics/"+url.PathEscape(name), nil)
		if err != nil {
			return err
		}
		if token := os.Getenv(m.tokenEnv); token != "" {
			r.Header.Set("X-Buckyd-Authorization", token)
		}
		response, err := m.client.Do(r)
		if err != nil {
			return err
		}
		_ = response.Body.Close()
		switch response.StatusCode {
		case http.StatusOK:
		case http.StatusNotFound:
			absent = append(absent, name)
		default:
			return &apiError{response.StatusCode, fmt.Sprintf("source HEAD %s: HTTP %d", name, response.StatusCode)}
		}
	}
	if len(absent) == 0 {
		return errors.New("source export HTTP404 but no requested name is absent on donor")
	}
	m.stateMu.Lock()
	defer m.stateMu.Unlock()
	s := absentSourceSkip{Source: batch.Source, Stream: batch.Stream, Start: batch.Start, End: batch.End, BatchMetrics: len(batch.Names)}
	if old, ok := m.journal.absent[s.key()]; ok {
		s = old
	}
	seen := map[string]bool{}
	for _, name := range s.Names {
		seen[name] = true
	}
	for _, name := range absent {
		if !seen[name] {
			s.Names = append(s.Names, name)
			seen[name] = true
		}
	}
	s.Count = len(s.Names)
	data, err := json.Marshal(struct {
		Operation, Plan     string
		SkippedAbsentSource absentSourceSkip
	}{m.journal.operation, m.journal.plan, s})
	if err != nil {
		return err
	}
	if _, err := m.journal.file.Write(append(data, '\n')); err != nil {
		return err
	}
	if err := m.journal.file.Sync(); err != nil {
		return err
	}
	m.journal.absent[s.key()] = s
	total := 0
	for _, record := range m.journal.absent {
		total += record.Count
	}
	err = m.journal.absentError()
	event("skipped_absent_source", map[string]any{"source": s.Source, "stream": s.Stream, "start": s.Start, "end": s.End, "count": s.Count, "names": s.Names, "total": total, "bound_exceeded": err != nil})
	return err
}
