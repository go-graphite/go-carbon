package downsize

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"sort"
)

// fillIdentity binds an import to the exact verification observation that
// requested it. Round/source alone are reused when a frontier finds a new hole
// after an earlier successful run. Even identical names and ranges may need a
// new import after deletion, so the durable observation generation participates.
// Replaying the same verification rows after a crash keeps the identity stable.
func (s *syncer) fillIdentity(round, src int, names []string) (string, error) {
	identity, err := syncIdentity(s.o)
	if err != nil {
		return "", err
	}
	selected := append([]string(nil), names...)
	sort.Strings(selected)
	observations := make(map[string]string, len(selected))
	for _, name := range selected {
		observations[name] = ""
	}
	journalRound := round + s.verificationBase
	for _, row := range s.j.find(func(r syncRow) bool { return r.Kind == "verified" && r.Round == journalRound }) {
		for _, hole := range row.Holes {
			if _, ok := observations[hole.Name]; ok && hole.Source == s.o.from[src] {
				// Legacy rows have no generation; their durable timestamp still
				// separates observations. New rows also carry a random identity.
				observations[hole.Name] = row.Identity + "/" + row.Time
			}
		}
	}
	scope := ""
	if s.frontier != nil {
		scope = s.frontier.key
	}
	data, err := json.Marshal(struct {
		Identity, Scope, Source string
		Round                   int
		Names                   []string
		Windows                 syncRanges
		FullHistory             bool
		Observations            map[string]string
	}{identity, scope, s.o.from[src], journalRound, selected, s.copyWindows(), s.o.fullHistory, observations})
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%x", sha256.Sum256(data)), nil
}
