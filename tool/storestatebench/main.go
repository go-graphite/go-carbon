// storestatebench compares offline state representations. It never connects to
// transfer endpoints and does not change the input directory.
package main

import (
	"bufio"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"time"

	"github.com/go-graphite/go-carbon/internal/statestore"
)

func check(err error) {
	if err != nil {
		panic(err)
	}
}
func emit(v any) { check(json.NewEncoder(os.Stdout).Encode(v)) }
func main() {
	mode := flag.String("mode", "normalize", "normalize, import, query, update, crash-child, crash-check, legacy-scan")
	input := flag.String("input", "", "read-only legacy state or normalized spool")
	output := flag.String("output", "", "new spool or database path")
	backend := flag.String("backend", "sqlite", "sqlite or pebble")
	bucket := flag.String("bucket", "progress.json", "query namespace")
	id := flag.Uint64("id", 0, "query record ID")
	n := flag.Int("names", 1<<20, "update workload size")
	cut := flag.String("cut", "before", "crash injection: before or after commit")
	flag.Parse()
	start := time.Now()
	switch *mode {
	case "normalize":
		check(normalize(*input, *output))
	case "import":
		check(importSpool(*backend, *input, *output))
	case "query":
		s, err := statestore.Open(*backend, *output)
		check(err)
		b, err := s.Get(*bucket, *id)
		check(err)
		emit(map[string]any{"query": *bucket, "id": *id, "payload": json.RawMessage(b), "seconds": time.Since(start).Seconds()})
		check(s.Close())
	case "update":
		check(update(*backend, *output, *n))
	case "crash-child":
		check(crashChild(*backend, *output, *cut))
	case "crash-check":
		check(crashCheck(*backend, *output))
	case "legacy-scan":
		check(legacyScan(*input))
	default:
		panic("unknown mode")
	}
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	var u syscall.Rusage
	check(syscall.Getrusage(syscall.RUSAGE_SELF, &u))
	emit(map[string]any{"mode": *mode, "backend": *backend, "wall_seconds": time.Since(start).Seconds(), "user_seconds": float64(u.Utime.Sec) + float64(u.Utime.Usec)/1e6, "system_seconds": float64(u.Stime.Sec) + float64(u.Stime.Usec)/1e6, "maxrss_platform_units": u.Maxrss, "heap_alloc_bytes": m.HeapAlloc})
}

// Spool frames are canonical JSON Records, length-prefixed so payloads are not
// newline constrained. Both engines consume the exact same ordered bytes.
func writeRecord(w io.Writer, r statestore.Record) error {
	b, err := json.Marshal(r)
	if err != nil {
		return err
	}
	var h [8]byte
	binary.LittleEndian.PutUint64(h[:], uint64(len(b)))
	if _, err = w.Write(h[:]); err != nil {
		return err
	}
	_, err = w.Write(b)
	return err
}
func readRecord(r io.Reader) (statestore.Record, error) {
	var h [8]byte
	_, err := io.ReadFull(r, h[:])
	if err != nil {
		return statestore.Record{}, err
	}
	n := binary.LittleEndian.Uint64(h[:])
	if n > 256<<20 {
		return statestore.Record{}, errors.New("oversized frame")
	}
	b := make([]byte, n)
	if _, err = io.ReadFull(r, b); err != nil {
		return statestore.Record{}, err
	}
	var v statestore.Record
	err = json.Unmarshal(b, &v)
	return v, err
}
func hashRecord(sum *[32]byte, r statestore.Record) {
	b, err := json.Marshal(r)
	check(err)
	h := sha256.Sum256(b)
	for i := range sum {
		sum[i] ^= h[i]
	}
}
func scanner(f *os.File) *bufio.Scanner {
	s := bufio.NewScanner(f)
	s.Buffer(make([]byte, 1<<20), 64<<20)
	return s
}
func decode(b []byte) (any, error) {
	d := json.NewDecoder(strings.NewReader(string(b)))
	d.UseNumber()
	var v any
	err := d.Decode(&v)
	return v, err
}

func normalize(input, output string) error {
	d, err := newDictionary()
	if err != nil {
		return err
	}
	defer d.close()
	out, err := os.OpenFile(output, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	defer out.Close()
	w := bufio.NewWriterSize(out, 8<<20)
	var digest [32]byte
	var records uint64
	put := func(r statestore.Record) error { hashRecord(&digest, r); records++; return writeRecord(w, r) }
	f, err := os.Open(filepath.Join(input, "union.tsv"))
	if err != nil {
		return err
	}
	s := scanner(f)
	names := make([]catalogName, 0, pageSize)
	var prev string
	start := time.Now()
	flush := func() error {
		if len(names) == 0 {
			return nil
		}
		p := d.add(names)
		err := put(statestore.Record{Bucket: "catalog", ID: p.FirstID, Label: p.Last, Payload: p.Data})
		names = names[:0]
		return err
	}
	for s.Scan() {
		n, e := parseCatalog(s.Text())
		if e != nil {
			f.Close()
			return e
		}
		if n.Name <= prev {
			f.Close()
			return errors.New("catalog must be strictly sorted")
		}
		prev = n.Name
		names = append(names, n)
		if len(names) == pageSize {
			if e = flush(); e != nil {
				f.Close()
				return e
			}
		}
	}
	err = s.Err()
	f.Close()
	if err != nil {
		return err
	}
	if err = flush(); err != nil {
		return err
	}
	emit(map[string]any{"stage": "dictionary", "names": d.count, "pages": len(d.pages), "seconds": time.Since(start).Seconds()})
	// Missing membership is preserved independently for comparison even though it
	// is derivable from the dictionary's source/target flags.
	for _, file := range []string{"missing.tsv", "sync.jsonl", "pending-selections.json", "residual.json", "config.json", "progress.json", "resolved.json"} {
		start = time.Now()
		f, err = os.Open(filepath.Join(input, file))
		if err != nil {
			return err
		}
		var row uint64
		var chunk []any
		var chunkID uint64
		kinds := map[string]uint64{}
		var bytesIn int64
		emitChunk := func() error {
			if len(chunk) == 0 {
				return nil
			}
			b, e := json.Marshal(chunk)
			if e != nil {
				return e
			}
			err := put(statestore.Record{Bucket: file, ID: chunkID, Payload: d.enc.EncodeAll(b, nil)})
			chunkID++
			chunk = chunk[:0]
			return err
		}
		add := func(v any) error {
			b, e := d.canonical(v)
			if e != nil {
				return e
			}
			chunk = append(chunk, json.RawMessage(b))
			row++
			if len(chunk) == pageSize {
				return emitChunk()
			}
			return nil
		}
		if file == "missing.tsv" || file == "sync.jsonl" {
			s = scanner(f)
			for s.Scan() {
				line := s.Bytes()
				bytesIn += int64(len(line) + 1)
				if file == "missing.tsv" {
					v := strings.Split(string(line), "\t")
					if len(v) != 2 {
						f.Close()
						return errors.New("invalid missing row")
					}
					if err = add(v); err != nil {
						f.Close()
						return err
					}
				} else {
					v, e := decode(line)
					if e != nil {
						f.Close()
						return e
					}
					m, ok := v.(map[string]any)
					if !ok {
						return errors.New("journal row is not an object")
					}
					kind, _ := m["kind"].(string)
					kinds[kind]++
					b, e := d.canonical(v)
					if e != nil {
						return e
					}
					if e = put(statestore.Record{Bucket: file, ID: row, Label: kind, Payload: d.enc.EncodeAll(b, nil)}); e != nil {
						return e
					}
					row++
				}
			}
			err = s.Err()
		} else if file == "pending-selections.json" || file == "residual.json" {
			dec := json.NewDecoder(bufio.NewReaderSize(f, 1<<20))
			dec.UseNumber()
			tok, e := dec.Token()
			if e != nil {
				return e
			}
			if tok != json.Delim('[') {
				return errors.New("expected residual array")
			}
			for dec.More() {
				var v any
				if e = dec.Decode(&v); e != nil {
					return e
				}
				if e = add(v); e != nil {
					return e
				}
			}
			_, err = dec.Token()
			bytesIn = dec.InputOffset()
		} else {
			b, e := io.ReadAll(io.LimitReader(f, 16<<20))
			if e != nil {
				return e
			}
			bytesIn = int64(len(b))
			var v any
			v, e = decode(b)
			if e != nil {
				return e
			}
			b, e = json.Marshal(v)
			if e != nil {
				return e
			}
			err = put(statestore.Record{Bucket: file, ID: 0, Payload: b})
			row = 1
		}
		f.Close()
		if err != nil {
			return err
		}
		if err = emitChunk(); err != nil {
			return err
		}
		emit(map[string]any{"stage": "normalize", "file": file, "rows": row, "bytes": bytesIn, "bytes_per_row": float64(bytesIn) / float64(max(row, 1)), "kinds": kinds, "seconds": time.Since(start).Seconds()})
	}
	if err = w.Flush(); err != nil {
		return err
	}
	if err = out.Sync(); err != nil {
		return err
	}
	emit(map[string]any{"stage": "normalized", "records": records, "xor_sha256": fmt.Sprintf("%x", digest), "catalog_names": d.count})
	return nil
}
func importSpool(backend, input, output string) error {
	s, err := statestore.Open(backend, output)
	if err != nil {
		return err
	}
	defer s.Close()
	f, err := os.Open(input)
	if err != nil {
		return err
	}
	defer f.Close()
	r := bufio.NewReaderSize(f, 8<<20)
	var expected [32]byte
	var count uint64
	var batch []statestore.Record
	var bytes int
	start := time.Now()
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		e := s.Apply(batch)
		batch = batch[:0]
		bytes = 0
		return e
	}
	for {
		v, e := readRecord(r)
		if errors.Is(e, io.EOF) {
			break
		}
		if e != nil {
			return e
		}
		hashRecord(&expected, v)
		count++
		batch = append(batch, v)
		bytes += len(v.Payload)
		if bytes >= 8<<20 || len(batch) >= 4096 {
			if e = flush(); e != nil {
				return e
			}
		}
	}
	if err = flush(); err != nil {
		return err
	}
	emit(map[string]any{"stage": "import", "records": count, "seconds": time.Since(start).Seconds(), "xor_sha256": fmt.Sprintf("%x", expected)})
	var actual [32]byte
	var n uint64
	start = time.Now()
	err = s.Scan(func(r statestore.Record) error { hashRecord(&actual, r); n++; return nil })
	if err != nil {
		return err
	}
	if actual != expected || n != count {
		return errors.New("engine parity mismatch")
	}
	emit(map[string]any{"stage": "full_engine_parity", "records": n, "seconds": time.Since(start).Seconds(), "xor_sha256": fmt.Sprintf("%x", actual)})
	return nil
}
func update(backend, path string, n int) error {
	s, err := statestore.Open(backend, path)
	if err != nil {
		return err
	}
	defer s.Close()
	start := time.Now()
	var added uint64
	for off := 0; off < n; off += 8192 {
		ids := make([]uint64, min(8192, n-off))
		for i := range ids {
			ids[i] = uint64(off + i + 1)
		}
		k, e := s.CommitWork(1, uint64(off/8192), ids, nil)
		if e != nil {
			return e
		}
		added += k
	}
	elapsed := time.Since(start).Seconds()
	emit(map[string]any{"stage": "updates", "requested_names": n, "new_names": added, "seconds": elapsed, "names_per_second": float64(added) / elapsed})
	return nil
}
func crashChild(backend, path, cut string) error {
	s, err := statestore.Open(backend, path)
	if err != nil {
		return err
	}
	defer s.Close()
	ids := make([]uint64, 8192)
	for i := range ids {
		ids[i] = uint64(i + 1)
	}
	hook := func() { check(syscall.Kill(os.Getpid(), syscall.SIGKILL)) }
	if cut == "after" {
		hook = nil
	}
	_, err = s.CommitWork(99, 1, ids, hook)
	if err != nil {
		return err
	}
	return syscall.Kill(os.Getpid(), syscall.SIGKILL)
}
func crashCheck(backend, path string) error {
	for _, cut := range []string{"before", "after"} {
		p := path + "-" + cut
		cmd := exec.Command(os.Args[0], "-mode=crash-child", "-backend="+backend, "-output="+p, "-cut="+cut)
		err := cmd.Run()
		var exit *exec.ExitError
		if !errors.As(err, &exit) || exit.ProcessState.Sys().(syscall.WaitStatus).Signal() != syscall.SIGKILL {
			return fmt.Errorf("child did not die at %s: %v", cut, err)
		}
		s, e := statestore.Open(backend, p)
		if e != nil {
			return e
		}
		b, e := s.Get("window", 99)
		if cut == "before" {
			if !errors.Is(e, statestore.ErrNotFound) {
				return fmt.Errorf("precommit survived: %s %v", b, e)
			}
		} else {
			if e != nil || string(b) != "8192" {
				return fmt.Errorf("postcommit lost: %s %v", b, e)
			}
		}
		ids := make([]uint64, 8192)
		for i := range ids {
			ids[i] = uint64(i + 1)
		}
		added, e := s.CommitWork(99, 1, ids, nil)
		if e != nil {
			return e
		}
		want := uint64(0)
		if cut == "before" {
			want = 8192
		}
		if added != want {
			return fmt.Errorf("replay counted %d want %d", added, want)
		}
		b, e = s.Get("window", 99)
		if e != nil || string(b) != "8192" {
			return fmt.Errorf("wrong replay count %s %v", b, e)
		}
		s.Close()
		emit(map[string]any{"stage": "crash_check", "cut": cut, "pass": true, "replay_added": added})
	}
	return nil
}
func legacyScan(path string) error {
	start := time.Now()
	f, err := os.Open(filepath.Join(path, "sync.jsonl"))
	if err != nil {
		return err
	}
	defer f.Close()
	s := scanner(f)
	var rows uint64
	var bytes uint64
	kinds := map[string]uint64{}
	for s.Scan() {
		var v map[string]json.RawMessage
		if err = json.Unmarshal(s.Bytes(), &v); err != nil {
			return err
		}
		var kind string
		if err = json.Unmarshal(v["kind"], &kind); err != nil {
			return err
		}
		kinds[kind]++
		rows++
		bytes += uint64(len(s.Bytes()) + 1)
	}
	if err = s.Err(); err != nil {
		return err
	}
	emit(map[string]any{"stage": "legacy_journal_parse_only", "rows": rows, "bytes": bytes, "kinds": kinds, "seconds": time.Since(start).Seconds(), "note": "lower bound: does not retain/reduce journal or issue RPCs"})
	return nil
}
