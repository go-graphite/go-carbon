// store-readonly inspects named metrics in a stopped chunkstore without writes.
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/internal/whisperio"
)

type window struct {
	From  int64 `json:"from"`
	Until int64 `json:"until"`
}
type request struct {
	Name   string   `json:"name"`
	Ranges []window `json:"ranges"`
}
type point struct {
	Timestamp int64  `json:"t"`
	Bits      string `json:"bits"`
}
type archive struct {
	Step    int     `json:"step"`
	Count   int     `json:"count"`
	Zero    int     `json:"zero"`
	Nonzero int     `json:"nonzero"`
	Last    *point  `json:"last"`
	Points  []point `json:"points,omitempty"`
}
type response struct {
	Name       string    `json:"name"`
	Error      string    `json:"error,omitempty"`
	LastUpdate int64     `json:"last_update,omitempty"`
	Last       *point    `json:"last"`
	Archives   []archive `json:"archives,omitempty"`
}

func inspect(directory string, input io.Reader, output io.Writer) (err error) {
	if !filepath.IsAbs(directory) {
		return errors.New("absolute store directory required")
	}
	marker, err := os.ReadFile(filepath.Join(directory, "CHUNKSTORE"))
	if err != nil {
		return err
	}
	version, parseErr := strconv.Atoi(strings.TrimPrefix(strings.TrimSpace(string(marker)), "go-carbon-chunks-v"))
	if parseErr != nil {
		return errors.New("invalid chunkstore format marker")
	}
	opts := store.Options{ReadOnly: true, ExactBlocks: version >= 4, CacheSize: 128 << 20, MetadataCacheSize: 1 << 20}
	// Preserve the database's existing source binding. Read-only Open verifies
	// the original identity and never creates a token or upgrades the marker.
	binding, bindingErr := os.ReadFile(filepath.Join(directory, "INCREMENTAL-FILE-MIGRATION"))
	if bindingErr == nil {
		parts := strings.Split(string(binding), "\n")
		if len(parts) != 4 || (parts[0] != "go-carbon-incremental-v1" && parts[0] != "go-carbon-incremental-v2") || !filepath.IsAbs(parts[1]) {
			return errors.New("invalid incremental source binding")
		}
		opts.IncrementalSource = parts[1]
	} else if !errors.Is(bindingErr, os.ErrNotExist) {
		return bindingErr
	}
	if version >= 6 {
		opts.Continuation = &whisperio.ContinuationCodec{}
		opts.TimeBuckets = true
	}
	db, err := store.Open(directory, opts)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, db.Close()) }()
	if !db.ReadOnly() {
		return errors.New("store did not open read-only")
	}
	scanner := bufio.NewScanner(input)
	scanner.Buffer(make([]byte, 4096), 2<<20)
	encoder := json.NewEncoder(output)
	for scanner.Scan() {
		var q request
		if err := json.Unmarshal(scanner.Bytes(), &q); err != nil {
			return err
		}
		if q.Name == "" {
			return errors.New("metric name required")
		}
		for _, w := range q.Ranges {
			if w.From < 0 || w.Until <= w.From {
				return errors.New("invalid half-open range")
			}
		}
		row := response{Name: q.Name}
		snapshot, e := db.Snapshot(context.Background(), q.Name)
		if e != nil {
			if !errors.Is(e, store.ErrNotFound) {
				return fmt.Errorf("%s: %w", q.Name, e)
			}
			row.Error = "not found"
		} else {
			if !snapshot.Metadata.LastUpdate.IsZero() {
				row.LastUpdate = snapshot.Metadata.LastUpdate.Unix()
			}
			for _, a := range snapshot.Archives {
				out := archive{Step: a.Retention.Step, Count: a.Retention.Count}
				for _, p := range a.Points {
					if p.Timestamp <= 0 {
						continue
					}
					v := point{Timestamp: p.Timestamp, Bits: fmt.Sprintf("%016x", math.Float64bits(p.Value))}
					if p.Value == 0 {
						out.Zero++
					} else {
						out.Nonzero++
					}
					if out.Last == nil || v.Timestamp > out.Last.Timestamp {
						copy := v
						out.Last = &copy
					}
					if row.Last == nil || v.Timestamp > row.Last.Timestamp {
						copy := v
						row.Last = &copy
					}
					for _, w := range q.Ranges {
						if w.From <= p.Timestamp && p.Timestamp < w.Until {
							out.Points = append(out.Points, v)
							break
						}
					}
				}
				row.Archives = append(row.Archives, out)
			}
		}
		if err := encoder.Encode(row); err != nil {
			return err
		}
	}
	return scanner.Err()
}

func main() {
	directory := flag.String("directory", "", "absolute path to an existing stopped chunkstore")
	flag.Parse()
	if err := inspect(*directory, os.Stdin, os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
