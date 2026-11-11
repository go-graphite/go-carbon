package downsize

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/go-graphite/go-carbon/internal/chunkstore"
)

func parseNativeWindow(from, until string) (*chunkstore.NativeWindow, error) {
	if from == "" && until == "" {
		return nil, nil
	}
	if from == "" || until == "" {
		return nil, errors.New("--from and --until must be supplied together")
	}
	parse := func(value string) (int64, error) {
		if n, err := strconv.ParseInt(value, 10, 64); err == nil {
			return n, nil
		}
		t, err := time.Parse(time.RFC3339, value)
		if err != nil || t.Nanosecond() != 0 {
			return 0, fmt.Errorf("time must be Unix seconds or RFC3339 with whole seconds: %s", value)
		}
		return t.Unix(), nil
	}
	a, err := parse(from)
	if err != nil {
		return nil, err
	}
	b, err := parse(until)
	if err != nil {
		return nil, err
	}
	w := &chunkstore.NativeWindow{From: a, Until: b}
	return w, w.Validate()
}

// The execution plan's domain records the window even when every range is
// complete. Resume inherits it before opening the journal; explicit changes
// are rejected by the journal's existing immutable-plan check.
func resolveNativeWindow(o *options, p *manifest) error {
	explicit, err := parseNativeWindow(o.from, o.until)
	if err != nil {
		return err
	}
	o.nativeWindow = p.Spec.NativeWindow
	if err := o.nativeWindow.Validate(); err != nil {
		return err
	}
	if explicit != nil {
		if o.nativeWindow != nil && !chunkstore.SameNativeWindow(o.nativeWindow, explicit) {
			return errors.New("time window differs from committed plan")
		}
		o.nativeWindow = explicit
	}
	if o.state == "" {
		return nil
	}
	f, err := os.Open(o.state)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	defer f.Close()
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 4096), 1<<20)
	if !scanner.Scan() {
		return scanner.Err()
	}
	var row journalRow
	if err := json.Unmarshal(scanner.Bytes(), &row); err != nil {
		return err
	}
	_, suffix, found := strings.Cut(row.Plan, ":window=")
	if !found {
		return nil
	}
	parts := strings.Split(suffix, ",")
	if len(parts) != 2 {
		return errors.New("invalid journal time window")
	}
	saved, err := parseNativeWindow(parts[0], parts[1])
	if err != nil {
		return err
	}
	if o.nativeWindow != nil && !chunkstore.SameNativeWindow(o.nativeWindow, saved) {
		return errors.New("time window differs from journal")
	}
	o.nativeWindow = saved
	return nil
}
