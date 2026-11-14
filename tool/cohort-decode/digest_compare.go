package main

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
)

type digestInput struct {
	Host, Path, SHA256 string
	Bytes              int64
	Windows            []window
	Names, AbsentNames int64
	RequireCounts      bool
}
type digestJoinSpec struct {
	Survivor digestInput
	Donors   []digestInput
}
type digestRow struct {
	Archives        [][2]int              `json:"archives"`
	Buckets         [][]any               `json:"buckets"`
	Ambiguous       [][3]int64            `json:"ambiguous"`
	Windows         map[string]*digestRow `json:"windows"`
	WindowsComplete bool                  `json:"windows_complete"`
}
type digestCursor struct {
	input          digestInput
	file           *os.File
	reader         *bufio.Reader
	hash           io.Writer
	sum            interface{ Sum([]byte) []byte }
	bytes          int64
	name, previous string
	row            *digestRow
	eof            bool
	windowsOnly    bool
	rows, absent   int64
}
type countedReader struct {
	reader io.Reader
	count  *int64
}

func (r countedReader) Read(p []byte) (int, error) {
	n, e := r.reader.Read(p)
	*r.count += int64(n)
	return n, e
}
func openDigest(input digestInput) (*digestCursor, error) {
	f, e := os.Open(input.Path)
	if e != nil {
		return nil, e
	}
	h := sha256.New()
	c := &digestCursor{input: input, file: f, hash: h, sum: h}
	z, e := gzip.NewReader(io.TeeReader(countedReader{f, &c.bytes}, h))
	if e != nil {
		f.Close()
		return nil, e
	}
	c.reader = bufio.NewReaderSize(z, 1<<20)
	return c, nil
}
func (c *digestCursor) next() error {
	line, e := c.reader.ReadBytes('\n')
	if e == io.EOF && len(line) == 0 {
		c.eof = true
		if c.bytes != c.input.Bytes || hex.EncodeToString(c.sum.Sum(nil)) != c.input.SHA256 {
			return fmt.Errorf("digest stream size/SHA differs: %s", c.input.Host)
		}
		if c.input.RequireCounts && (c.rows != c.input.Names || c.absent != c.input.AbsentNames) {
			return fmt.Errorf("digest name/explicit absence counts differ: %s", c.input.Host)
		}
		return nil
	}
	if e != nil {
		return e
	}
	if len(line) > 128<<20 {
		return errors.New("digest row exceeds cap")
	}
	var pair []json.RawMessage
	if e = json.Unmarshal(line, &pair); e != nil {
		return e
	}
	if len(pair) != 2 {
		return errors.New("digest row must be a name/value pair")
	}
	if e = json.Unmarshal(pair[0], &c.name); e != nil {
		return e
	}
	if c.previous != "" && c.name <= c.previous {
		return errors.New("digest names are not strictly sorted")
	}
	c.previous = c.name
	c.rows++
	if bytes.Equal(bytes.TrimSpace(pair[1]), []byte("null")) {
		c.absent++
	}
	if c.windowsOnly {
		var light *struct {
			Archives        [][2]int              `json:"archives"`
			Windows         map[string]*digestRow `json:"windows"`
			WindowsComplete bool                  `json:"windows_complete"`
		}
		if err := json.Unmarshal(pair[1], &light); err != nil {
			return err
		}
		c.row = nil
		if light != nil {
			c.row = &digestRow{Archives: light.Archives, Windows: light.Windows, WindowsComplete: light.WindowsComplete}
		}
		return nil
	}
	return json.Unmarshal(pair[1], &c.row)
}

type digestJoinCounts struct {
	Equal               int64                     `json:"equal"`
	Drilled             int64                     `json:"drilled"`
	DonorNames          int64                     `json:"donor_names"`
	UnavailableDonor    int64                     `json:"unavailable_donor"`
	UnavailableSurvivor int64                     `json:"unavailable_survivor"`
	PolicyMismatch      int64                     `json:"policy_mismatch"`
	ByWindow            map[int]*digestJoinCounts `json:"by_window,omitempty"`
}

func bucketKey(b []any) ([2]int64, error) {
	if len(b) != 6 {
		return [2]int64{}, errors.New("six digest bucket fields required")
	}
	a, aok := b[0].(float64)
	d, dok := b[1].(float64)
	if !aok || !dok || a != float64(int64(a)) || d != float64(int64(d)) {
		return [2]int64{}, errors.New("invalid archive/day")
	}
	return [2]int64{int64(a), int64(d)}, nil
}

// joinDigests consumes every compressed input to its SHA-verified EOF. It emits
// only unequal buckets; no metric points or production APIs are involved.
func joinDigests(spec digestJoinSpec, output io.Writer) (digestJoinCounts, error) {
	var count digestJoinCounts
	inputs := append([]digestInput{spec.Survivor}, spec.Donors...)
	cursors := make([]*digestCursor, 0, len(inputs))
	defer func() {
		for _, c := range cursors {
			c.file.Close()
		}
	}()
	for _, input := range inputs {
		c, e := openDigest(input)
		if e != nil {
			return count, e
		}
		cursors = append(cursors, c)
		if e = c.next(); e != nil {
			return count, e
		}
	}
	target := cursors[0]
	enc := json.NewEncoder(output)
	for {
		var source *digestCursor
		for _, c := range cursors[1:] {
			if !c.eof && (source == nil || c.name < source.name) {
				source = c
			}
		}
		if source == nil {
			break
		}
		count.DonorNames++
		for !target.eof && target.name < source.name {
			if e := target.next(); e != nil {
				return count, e
			}
		}
		var actual *digestRow
		if !target.eof && target.name == source.name {
			actual = target.row
		}
		if source.row == nil {
			count.UnavailableDonor++
		} else {
			if actual == nil {
				count.UnavailableSurvivor++
			}
			wanted, e := unequalBuckets(source.row, actual, &count)
			if e != nil {
				return count, e
			}

			if len(wanted) > 0 {
				if e := enc.Encode(map[string]any{"name": source.name, "donor": source.input.Host, "buckets": wanted, "survivor_native_present": actual != nil}); e != nil {
					return count, e
				}
			}
		}
		if e := source.next(); e != nil {
			return count, e
		}
	}
	for !target.eof {
		if e := target.next(); e != nil {
			return count, e
		}
	}
	return count, nil
}

// unequalBuckets compares donor buckets; survivor-only buckets are allowed.
func unequalBuckets(source, actual *digestRow, count *digestJoinCounts) ([]any, error) {
	policies := make(map[[2]int]int)
	buckets := make(map[[2]int64][]any)
	ambiguous := make(map[[2]int64]bool)
	if actual != nil {
		for i, a := range actual.Archives {
			policies[a] = i
		}
		for _, b := range actual.Buckets {
			k, e := bucketKey(b)
			if e != nil {
				return nil, e
			}
			buckets[k] = b
		}
		for _, a := range actual.Ambiguous {
			ambiguous[[2]int64{a[0], a[1] / 86400}] = true
		}
	}
	ownAmbiguous := make(map[[2]int64]bool)
	for _, a := range source.Ambiguous {
		ownAmbiguous[[2]int64{a[0], a[1] / 86400}] = true
	}
	wanted := []any{}
	for _, b := range source.Buckets {
		k, e := bucketKey(b)
		if e != nil {
			return nil, e
		}
		if k[0] < 0 || int(k[0]) >= len(source.Archives) {
			return nil, errors.New("invalid source archive index")
		}
		policy := source.Archives[k[0]]
		ti, ok := policies[policy]
		var observed []any
		if ok {
			observed = buckets[[2]int64{int64(ti), k[1]}]
		}
		if observed != nil && observed[2] == b[2] && observed[3] == b[3] && !ownAmbiguous[k] && !ambiguous[[2]int64{int64(ti), k[1]}] {
			count.Equal++
			continue
		}
		count.Drilled++
		if !ok {
			count.PolicyMismatch++
		}
		wanted = append(wanted, map[string]any{"archive": k[0], "policy": policy, "day": k[1], "donor": b, "target": observed})
	}
	return wanted, nil
}
