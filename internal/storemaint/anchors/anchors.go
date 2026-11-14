// Package anchors repairs archive anchors through the daemon's maintenance API.
package anchors

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strings"
	"syscall"
	"time"

	"github.com/go-graphite/go-carbon/internal/storemaint/remote"
)

type identity struct {
	Endpoint string `json:"endpoint"`
	Mode     string `json:"mode"`
	Cohort   string `json:"cohort"`
	Plan     string `json:"plan_sha256,omitempty"`
}
type archive struct {
	Changed bool `json:"changed"`
	Blocked bool `json:"blocked"`
}
type result struct {
	Name     string    `json:"name"`
	Applied  bool      `json:"applied"`
	Error    string    `json:"error"`
	Archives []archive `json:"archives"`
}

func (r result) blocked() bool {
	for _, a := range r.Archives {
		if a.Blocked {
			return true
		}
	}
	return false
}

type row struct {
	Identity identity        `json:"identity"`
	At       int64           `json:"at"`
	Result   json.RawMessage `json:"result"`
}

func digest(b []byte) string { s := sha256.Sum256(b); return hex.EncodeToString(s[:]) }

func Main(args []string) error {
	fs := flag.NewFlagSet("storectl maintain anchors", flag.ContinueOnError)
	endpoint := fs.String("endpoint", "http://127.0.0.1:4242", "buckyd endpoint")
	secret := fs.String("secret-file", "", "auth secret file (never journaled)")
	names := fs.String("names", "", "newline-separated names; otherwise enumerate catalog")
	journal := fs.String("journal", "", "fsynced per-name resume journal")
	apply := fs.Bool("apply", false, "apply repairs; requires plan and WRITE-OK")
	plan := fs.String("plan", "", "reviewed repair plan")
	approval := fs.String("write-ok", "", "approval containing plan SHA256")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 0 || *secret == "" || *journal == "" {
		return errors.New("anchors requires -secret-file and -journal")
	}
	id := identity{Endpoint: strings.TrimRight(*endpoint, "/"), Mode: "dry-run", Cohort: "catalog"}
	if *apply {
		if *plan == "" || *approval == "" {
			return errors.New("apply needs -plan and -write-ok")
		}
		b, err := os.ReadFile(*plan)
		if err != nil {
			return err
		}
		id.Plan = digest(b)
		b, err = os.ReadFile(*approval)
		if err != nil {
			return err
		}
		if !strings.Contains(string(b), id.Plan) {
			return errors.New("WRITE-OK does not bind this plan SHA256")
		}
		id.Mode = "apply"
	}
	var cohort []string
	if *names != "" {
		b, err := os.ReadFile(*names)
		if err != nil {
			return err
		}
		id.Cohort = digest(b)
		for _, n := range strings.Split(string(b), "\n") {
			if n = strings.TrimSpace(n); n != "" {
				cohort = append(cohort, n)
			}
		}
	}
	c, err := remote.New(id.Endpoint, *secret, *apply)
	if err != nil {
		return err
	}
	return run(c, id, *journal, cohort, *names != "", os.Stdout)
}
func run(c *remote.Client, id identity, path string, names []string, explicit bool, out io.Writer) error {
	log, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return err
	}
	defer log.Close()
	if err = syscall.Flock(int(log.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		return err
	}
	seen := map[string]bool{}
	reader := bufio.NewReader(log)
	var offset int64
	for {
		line, e := reader.ReadBytes('\n')
		if e == io.EOF {
			if len(line) > 0 {
				if err = log.Truncate(offset); err != nil {
					return err
				}
				if err = log.Sync(); err != nil {
					return err
				}
			}
			break
		}
		if e != nil {
			return e
		}
		var r row
		if err = json.Unmarshal(line, &r); err != nil {
			return err
		}
		if r.Identity != id {
			return errors.New("journal identity differs; use a distinct journal")
		}
		var res result
		if err = json.Unmarshal(r.Result, &res); err != nil {
			return err
		}
		if res.Error == "" && !res.blocked() {
			seen[res.Name] = true
		}
		offset += int64(len(line))
	}
	if _, err = log.Seek(0, io.SeekEnd); err != nil {
		return err
	}
	counts := map[string]int{"names": 0, "changed": 0, "applied": 0, "blocked": 0, "errors": 0}
	process := func(names []string) error {
		for _, name := range names {
			if seen[name] {
				continue
			}
			var rows []json.RawMessage
			if err := c.Do(http.MethodPost, "/v1/maintenance/archive-anchors", map[string]any{"names": []string{name}, "apply": c.Apply}, &rows); err != nil {
				return err
			}
			if len(rows) != 1 {
				return errors.New("maintenance response identity mismatch")
			}
			var res result
			if err := json.Unmarshal(rows[0], &res); err != nil {
				return err
			}
			if res.Name != name {
				return errors.New("maintenance response identity mismatch")
			}
			b, err := json.Marshal(row{Identity: id, At: time.Now().Unix(), Result: rows[0]})
			if err != nil {
				return err
			}
			if _, err = log.Write(append(b, '\n')); err != nil {
				return err
			}
			if err = log.Sync(); err != nil {
				return err
			}
			counts["names"]++
			for _, a := range res.Archives {
				if a.Changed {
					counts["changed"]++
					break
				}
			}
			if res.Applied {
				counts["applied"]++
			}
			if res.blocked() {
				counts["blocked"]++
			}
			if res.Error != "" {
				counts["errors"]++
			}
			if res.Error == "" && !res.blocked() {
				seen[name] = true
			}
			if err = json.NewEncoder(out).Encode(map[string]any{"mode": id.Mode, "counts": counts, "last": name}); err != nil {
				return err
			}
		}
		return nil
	}
	if explicit {
		if err = process(names); err != nil {
			return err
		}
	} else {
		after := ""
		for {
			var page []string
			if err = c.Do(http.MethodGet, "/v1/transfer/list?"+url.Values{"after": {after}, "limit": {"1000"}}.Encode(), nil, &page); err != nil {
				return err
			}
			if len(page) == 0 {
				break
			}
			if len(page) > 1000 || !sort.StringsAreSorted(page) || page[0] <= after {
				return errors.New("catalog pagination did not advance")
			}
			for i := 1; i < len(page); i++ {
				if page[i] == page[i-1] {
					return errors.New("duplicate catalog entry")
				}
			}
			if err = process(page); err != nil {
				return err
			}
			after = page[len(page)-1]
		}
	}
	if err = json.NewEncoder(out).Encode(map[string]any{"mode": id.Mode, "counts": counts, "complete": true}); err != nil {
		return err
	}
	if counts["blocked"]+counts["errors"] > 0 {
		return fmt.Errorf("anchors: %d blocked, %d errors", counts["blocked"], counts["errors"])
	}
	return nil
}
