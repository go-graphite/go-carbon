package downsize

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"time"
)

func lockResumeState(path string) (*os.File, error) {
	f, err := os.OpenFile(path+".lock", os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	if err = syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		f.Close()
		return nil, fmt.Errorf("state %s is already locked: %w", path, err)
	}
	return f, nil
}

// A checkpoint is published only after both append files have reached stable
// storage. On recovery, bytes beyond these offsets are uncommitted and discarded.
type syncDiscoveryCheckpoint struct {
	Cursors                                  map[string]string `json:"cursors"`
	UnionBytes                               int64             `json:"union_bytes"`
	MissingBytes                             int64             `json:"missing_bytes"`
	Names, Missing, NotOwned, OnTarget, Both int
}

func cloneCursors(m map[string]string) map[string]string {
	out := make(map[string]string, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

func openDurableTSV(path string, offset int64) (*syncTSV, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}
	fail := func(err error) (*syncTSV, error) { f.Close(); return nil, err }
	st, err := f.Stat()
	if err != nil {
		return fail(err)
	}
	if st.Size() < offset {
		return fail(fmt.Errorf("%s: checkpoint needs %d bytes, have %d", path, offset, st.Size()))
	}
	if err = f.Truncate(offset); err != nil {
		return fail(err)
	}
	if _, err = f.Seek(offset, 0); err != nil {
		return fail(err)
	}
	return &syncTSV{file: f, w: bufio.NewWriterSize(f, 1<<20)}, nil
}
func (t *syncTSV) durableOffset() (int64, error) {
	if err := t.w.Flush(); err != nil {
		return 0, err
	}
	if err := t.file.Sync(); err != nil {
		return 0, err
	}
	return t.file.Seek(0, 1)
}

func atomicStateJSON(path string, value any) error {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	f, err := os.CreateTemp(filepath.Dir(path), ".state-*")
	if err != nil {
		return err
	}
	tmp := f.Name()
	defer os.Remove(tmp)
	if err = f.Chmod(0644); err != nil {
		f.Close()
		return err
	}
	_, werr := f.Write(append(data, '\n'))
	err = errors.Join(werr, f.Sync(), f.Close())
	if err != nil {
		return err
	}
	if err = os.Rename(tmp, path); err != nil {
		return err
	}
	return syncDirectory(filepath.Dir(path))
}

type progressSnapshot struct {
	Phase      string            `json:"phase"`
	Round      int               `json:"round"`
	Cursors    map[string]string `json:"cursors"`
	Discovered int64             `json:"names_discovered"`
	Verified   int64             `json:"names_verified"`
	Filled     int64             `json:"names_filled"`
	Copied     int64             `json:"names_copied"`
	Holes      int64             `json:"holes"` // cumulative hole point observations across rounds
	Rate       float64           `json:"names_per_second"`
	Updated    time.Time         `json:"last_update"`
	Status     string            `json:"status"`
	Error      string            `json:"error,omitempty"`
}

func (p *progressSnapshot) apply(r syncRow) {
	if r.Discovery != nil {
		p.Cursors = r.Discovery.Cursors
		p.Discovered = int64(r.Discovery.Names)
	}
	switch r.Kind {
	case "discovered":
		p.Discovered = int64(r.Names)
	case "verified":
		p.Verified += int64(r.Names)
		for _, h := range r.Holes {
			p.Holes += int64(h.Points)
		}
	case "copied":
		if r.Phase == "fill" {
			p.Filled += int64(r.Copied)
		} else {
			p.Copied += int64(r.Copied)
		}
	}
	if r.Round > p.Round && (r.Kind == "verified" || r.Phase == "fill") {
		p.Round = r.Round
	}
}

// The progress file is an observational snapshot; journals remain authoritative.
// Errors are returned on shutdown so monitoring never silently uses stale data.
func startProgressFile(ctx context.Context, path string, snapshot func() progressSnapshot, fail func(error)) func(error) error {
	done := make(chan struct{})
	stop := make(chan struct{})
	var writeErr error
	var mu sync.Mutex
	started := time.Now()
	initial := snapshot()
	base := initial.Discovered + initial.Verified + initial.Filled + initial.Copied
	write := func(status string, runErr error) {
		p := snapshot()
		p.Updated = time.Now()
		p.Status = status
		if runErr != nil {
			p.Error = runErr.Error()
		}
		p.Rate = float64(p.Discovered+p.Verified+p.Filled+p.Copied-base) / time.Since(started).Seconds()
		if err := atomicStateJSON(path, p); err != nil {
			mu.Lock()
			if writeErr == nil {
				writeErr = err
				event("progress_file_error", map[string]any{"level": "error", "path": path, "error": err.Error()})
				if fail != nil {
					fail(err)
				}
			}
			mu.Unlock()
		}
	}
	write("running", nil)
	go func() {
		defer close(done)
		tick := time.NewTicker(3 * time.Second)
		defer tick.Stop()
		for {
			select {
			case <-stop:
				return
			case <-ctx.Done():
				return
			case <-tick.C:
				write("running", nil)
			}
		}
	}()
	return func(err error) error {
		close(stop)
		<-done
		status := "complete"
		if err != nil {
			status = "incomplete"
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				status = "interrupted"
			}
		}
		write(status, err)
		mu.Lock()
		defer mu.Unlock()
		return writeErr
	}
}
