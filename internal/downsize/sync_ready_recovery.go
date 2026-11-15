package downsize

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// A recovery intent is durable before discovery starts. Its fixed windows and
// process boundary let resume reuse post-restart receipts without widening the
// window to the time of the next invocation. A later restart gets a new intent.
type readyRecovery struct {
	Identity    string                  `json:"identity"`
	Scope       string                  `json:"scope"`
	State       string                  `json:"state"`
	Windows     syncRanges              `json:"windows"`
	FullHistory bool                    `json:"full_history"`
	Processes   map[string]readyProcess `json:"processes"`
	Restarts    syncRanges              `json:"restarts"`
}

func (r readyRecovery) options(o syncOptions) syncOptions {
	o.state = filepath.Join(o.state, r.State)
	o.windows = append(syncRanges(nil), r.Windows...)
	o.fullHistory, o.sampleEvery, o.settle = r.FullHistory, 1, 0
	return o
}

func readReadyRecovery(path string, o syncOptions, scope string) (*readyRecovery, error) {
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var r readyRecovery
	if err = json.Unmarshal(data, &r); err != nil {
		return nil, fmt.Errorf("restart recovery checkpoint: %w", err)
	}
	identity, err := syncIdentity(o)
	if err != nil {
		return nil, err
	}
	if r.Identity != identity || r.Scope != scope || filepath.Base(r.State) != r.State || !strings.HasPrefix(r.State, "ready-recovery-") || len(r.Processes) != len(o.from)+1 || len(r.Windows) == 0 && !r.FullHistory {
		return nil, errors.New("restart recovery checkpoint does not match sync scope")
	}
	for _, store := range append(append([]string(nil), o.from...), o.to) {
		p := r.Processes[store]
		if p.Start <= 0 || p.Start > float64(o.now().Unix()+1) || p.Observed <= 0 || p.Observed > o.now().Unix() {
			return nil, errors.New("restart recovery checkpoint has invalid process boundary")
		}
	}
	for _, w := range append(append(syncRanges(nil), r.Windows...), r.Restarts...) {
		if w.From < 0 || w.Until <= w.From {
			return nil, errors.New("restart recovery checkpoint has invalid window")
		}
	}
	return &r, nil
}
