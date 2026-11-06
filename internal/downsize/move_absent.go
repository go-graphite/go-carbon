package downsize

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"unicode/utf8"
)

// Keep skip records separate from completed ranges: a later HTTP413 can split
// the original task without creating overlapping checkpoint ranges.
type absentSourceSkip struct {
	Source, Stream        string
	Start, End            int64
	BatchMetrics, Count   int
	Names                 []string
	CatalogConfirmedNames []string `json:",omitempty"`
}

func (s absentSourceSkip) key() string {
	return s.Source + ":" + taskKey(task{Stream: s.Stream, Start: s.Start, End: s.End})
}

func (j *journal) absentError() error {
	if j.maxSpeed {
		return nil // Exact HEAD+catalog proofs and durable name records remain mandatory.
	}
	total := 0
	for _, skip := range j.absent {
		total += skip.Count
		if skip.Count > max(1, skip.BatchMetrics/100) {
			return fmt.Errorf("absent source bound exceeded: %d/%d names in batch %s", skip.Count, skip.BatchMetrics, skip.key())
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

// A pull or metadata HEAD 404 can hide more than an absent metric. The legacy
// HEAD maps metadata read errors to 404 too, so require a fresh catalog proof
// before durably skipping a name. Other errors retain their fatal behavior.
func (m *mover) skipAbsentSource(ctx context.Context, batch task, names []string) error {
	var absent []string
	if m.journal.maxSpeed {
		var err error
		absent, err = m.parallelAbsentSourceProofs(ctx, batch.Source, names)
		if err != nil {
			return err
		}
	} else {
		for _, name := range names {
			response, err := m.sourceAbsenceRequest(ctx, http.MethodHead, batch.Source+"/metrics/"+url.PathEscape(name))
			if err != nil {
				return err
			}
			_ = response.Body.Close()
			switch response.StatusCode {
			case http.StatusOK:
			case http.StatusNotFound:
				if err := m.confirmSourceAbsent(ctx, batch.Source, name); err != nil {
					return err
				}
				absent = append(absent, name)
			default:
				return &apiError{response.StatusCode, fmt.Sprintf("source HEAD %s: HTTP %d", name, response.StatusCode)}
			}
		}
	}
	if len(absent) == 0 {
		return copyIntegrityError(errors.New("source export HTTP404 but no requested name is absent on donor"))
	}
	m.stateMu.Lock()
	defer m.stateMu.Unlock()
	s := absentSourceSkip{Source: batch.Source, Stream: batch.Stream, Start: batch.Start, End: batch.End, BatchMetrics: len(batch.Names)}
	if old, ok := m.journal.absent[s.key()]; ok {
		s = old
	}
	seen := map[string]bool{}
	confirmed := map[string]bool{}
	for _, name := range s.Names {
		seen[name] = true
	}
	for _, name := range s.CatalogConfirmedNames {
		confirmed[name] = true
	}
	for _, name := range absent {
		if !seen[name] {
			s.Names = append(s.Names, name)
			seen[name] = true
		}
		if !confirmed[name] {
			s.CatalogConfirmedNames = append(s.CatalogConfirmedNames, name)
			confirmed[name] = true
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

func (m *mover) sourceAbsenceRequest(ctx context.Context, method, endpoint string) (*http.Response, error) {
	r, err := http.NewRequestWithContext(ctx, method, endpoint, nil)
	if err != nil {
		return nil, err
	}
	token, err := m.authorizationToken()
	if err != nil {
		return nil, err
	}
	if token != "" {
		r.Header.Set("X-Buckyd-Authorization", token)
	}
	r.Header.Set("Cache-Control", "no-cache, no-store")
	// Reuse the authenticated transport, timeout and pool without allowing a
	// custom client's redirect policy to turn another endpoint into evidence.
	client := *m.client
	client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	response, err := client.Do(r)
	if err == nil && response.StatusCode == http.StatusUnauthorized {
		m.invalidateHostToken()
	}
	if err == nil && response.StatusCode == http.StatusTooManyRequests && m.tuner != nil && m.tuner.maxSpeed != nil {
		if parsed, parseErr := url.Parse(endpoint); parseErr == nil {
			m.tuner.backoff(parsed.Scheme + "://" + parsed.Host)
		}
	}
	return response, err
}

func (m *mover) confirmSourceAbsent(ctx context.Context, source, name string) error {
	query := url.Values{"prefix": {name}, "limit": {"1"}}
	response, err := m.sourceAbsenceRequest(ctx, http.MethodGet, source+"/v1/transfer/list?"+query.Encode())
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return &apiError{response.StatusCode, fmt.Sprintf("source catalog absence check %q: HTTP %d", name, response.StatusCode)}
	}
	const maxPageBytes = 1 << 20
	data, err := io.ReadAll(io.LimitReader(response.Body, maxPageBytes+1))
	if err != nil {
		return err
	}
	var page []string
	if len(data) > maxPageBytes || !utf8.Valid(data) || json.Unmarshal(data, &page) != nil || page == nil || len(page) > 1 {
		return copyIntegrityError(fmt.Errorf("source catalog absence check %q: invalid bounded JSON array", name))
	}
	if len(page) == 1 {
		if !strings.HasPrefix(page[0], name) || strings.ContainsAny(page[0], "\r\n\x00") {
			return copyIntegrityError(fmt.Errorf("source catalog absence check %q: response does not honor the exact prefix", name))
		}
		if page[0] == name {
			return copyIntegrityError(fmt.Errorf("source HEAD returned HTTP404 for %q but fresh catalog still lists that exact name; refusing absent-source skip", name))
		}
	}
	// The exact name sorts before every longer name sharing its prefix. An
	// empty page or a strictly longer match therefore proves current absence.
	return nil
}
