package ringplan

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

type inventory struct {
	page         []string
	pageAt       int
	paged        func(string) ([]string, error)
	done         bool
	file         *os.File
	body         io.ReadCloser
	scanner      *bufio.Scanner
	decoder      *json.Decoder
	format       string
	defaultBytes int64
	offset       int64
	previous     string
	info         os.FileInfo
}

func openInventory(ctx context.Context, in input, offset int64, previous string, timeout time.Duration) (*inventory, error) {
	v := &inventory{format: in.Format, defaultBytes: in.EstimatedBytes, offset: offset, previous: previous}
	if in.Format == "transfer" {
		client := &http.Client{Timeout: timeout, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
		v.paged = func(after string) ([]string, error) {
			u, err := url.Parse(in.URL)
			if err != nil {
				return nil, err
			}
			query := u.Query()
			query.Set("after", after)
			query.Set("limit", "10000")
			u.RawQuery = query.Encode()
			req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
			if err != nil {
				return nil, err
			}
			if in.TokenEnv != "" {
				token := os.Getenv(in.TokenEnv)
				if token == "" {
					return nil, errors.New("empty inventory token")
				}
				req.Header.Set("X-Buckyd-Authorization", token)
			}
			resp, err := client.Do(req)
			if err != nil {
				return nil, err
			}
			defer resp.Body.Close()
			if resp.StatusCode != 200 {
				return nil, fmt.Errorf("inventory HTTP status %d", resp.StatusCode)
			}
			var page []string
			d := json.NewDecoder(io.LimitReader(resp.Body, 64<<20))
			if err := d.Decode(&page); err != nil {
				return nil, err
			}
			var extra any
			if err := d.Decode(&extra); err != io.EOF {
				return nil, errors.New("trailing inventory page")
			}
			if len(page) > 10000 {
				return nil, errors.New("oversize inventory page")
			}
			return page, nil
		}
		v.body = io.NopCloser(strings.NewReader(""))
		return v, nil
	}
	if in.Format == "buckyd" {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, in.URL, nil)
		if err != nil {
			return nil, err
		}
		if in.TokenEnv != "" {
			token := os.Getenv(in.TokenEnv)
			if token == "" {
				return nil, fmt.Errorf("empty token environment variable %s", in.TokenEnv)
			}
			req.Header.Set("X-Buckyd-Authorization", token)
		}
		client := &http.Client{Timeout: timeout, CheckRedirect: func(_ *http.Request, _ []*http.Request) error { return errors.New("inventory redirects refused") }}
		resp, err := client.Do(req)
		if err != nil {
			return nil, err
		}
		if resp.StatusCode != http.StatusOK {
			resp.Body.Close()
			return nil, fmt.Errorf("inventory HTTP status %d", resp.StatusCode)
		}
		v.body = resp.Body
		v.decoder = json.NewDecoder(bufio.NewReaderSize(resp.Body, 256<<10))
		token, err := v.decoder.Token()
		if err != nil || token != json.Delim('[') {
			resp.Body.Close()
			return nil, errors.New("buckyd inventory must be a JSON array")
		}
		return v, nil
	}
	f, err := os.Open(in.Path)
	if err != nil {
		return nil, err
	}
	v.file, v.body = f, f
	v.info, err = f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	if !v.info.Mode().IsRegular() || offset > v.info.Size() {
		f.Close()
		return nil, errors.New("inventory must be a regular immutable file")
	}
	if _, err := f.Seek(offset, io.SeekStart); err != nil {
		f.Close()
		return nil, err
	}
	v.scanner = bufio.NewScanner(f)
	v.scanner.Buffer(make([]byte, 256<<10), 1<<20)
	v.scanner.Split(func(data []byte, atEOF bool) (int, []byte, error) {
		advance, token, err := bufio.ScanLines(data, atEOF)
		v.offset += int64(advance)
		return advance, token, err
	})
	return v, nil
}

func (v *inventory) next() (string, int64, error) {
	if v.paged != nil {
		if v.done {
			return "", 0, io.EOF
		}
		if v.pageAt == len(v.page) {
			page, err := v.paged(v.previous)
			if err != nil {
				return "", 0, err
			}
			v.page, v.pageAt = page, 0
			if len(page) == 0 {
				v.done = true
				return "", 0, io.EOF
			}
		}
		name := v.page[v.pageAt]
		v.pageAt++
		return v.check(name, v.defaultBytes)
	}
	if v.decoder != nil {
		return v.nextHTTP()
	}
	var name string
	size := v.defaultBytes
	if !v.scanner.Scan() {
		if err := v.scanner.Err(); err != nil {
			return "", 0, err
		}
		return "", 0, io.EOF
	}
	line := v.scanner.Text()
	switch v.format {
	case "lines":
		name = line
	case "tsv":
		var raw string
		var ok bool
		name, raw, ok = strings.Cut(line, "\t")
		if !ok {
			return "", 0, errors.New("TSV needs metric and byte estimate")
		}
		var err error
		size, err = strconv.ParseInt(raw, 10, 64)
		if err != nil {
			return "", 0, err
		}
	case "jsonl":
		var rec struct {
			Metric         string `json:"metric"`
			EstimatedBytes int64  `json:"estimated_bytes"`
		}
		if err := json.Unmarshal([]byte(line), &rec); err != nil {
			return "", 0, err
		}
		name, size = rec.Metric, rec.EstimatedBytes
	}
	return v.check(name, size)
}

func (v *inventory) nextHTTP() (string, int64, error) {
	if v.done {
		return "", 0, io.EOF
	}
	if !v.decoder.More() {
		token, err := v.decoder.Token()
		if errors.Is(err, io.EOF) {
			return "", 0, io.ErrUnexpectedEOF
		}
		if err != nil {
			return "", 0, err
		}
		if token != json.Delim(']') {
			return "", 0, errors.New("buckyd inventory must end with a JSON array delimiter")
		}
		var extra any
		if err := v.decoder.Decode(&extra); err != io.EOF {
			return "", 0, errors.New("trailing buckyd inventory data")
		}
		v.done = true
		return "", 0, io.EOF
	}
	var name string
	if err := v.decoder.Decode(&name); err != nil {
		if errors.Is(err, io.EOF) {
			err = io.ErrUnexpectedEOF
		}
		return "", 0, err
	}
	return v.check(name, v.defaultBytes)
}

func (v *inventory) check(name string, size int64) (string, int64, error) {
	if size < 0 {
		return "", 0, errors.New("negative byte estimate")
	}
	if err := validMetric(name); err != nil {
		return "", 0, err
	}
	if v.previous != "" && name <= v.previous {
		return "", 0, fmt.Errorf("inventory not strictly sorted: %q after %q", name, v.previous)
	}
	v.previous = name
	return name, size, nil
}

func validMetric(name string) error {
	if len(name) == 0 || len(name) > 65535 || !utf8.ValidString(name) {
		return errors.New("metric must be 1..65535 UTF-8 bytes")
	}
	for i := 0; i < len(name); i++ {
		if name[i] <= ' ' || name[i] == 127 {
			return fmt.Errorf("metric contains whitespace/control at byte %d", i)
		}
	}
	return nil
}

func (v *inventory) unchanged() error {
	if v.file == nil {
		return nil
	}
	now, err := v.file.Stat()
	if err != nil {
		return err
	}
	if now.Size() != v.info.Size() || !now.ModTime().Equal(v.info.ModTime()) {
		return errors.New("inventory changed while planning")
	}
	return nil
}

func (v *inventory) close() { _ = v.body.Close() }
