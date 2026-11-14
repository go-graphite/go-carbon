package downsize

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
)

// syncBlock is generic input data. Paths and URLs are supplied by the operator;
// the tool does not resolve fleet names, run SSH or launch a remote client.
type syncBlock struct {
	Target string    `json:"target"`
	Donors []string  `json:"donors"`
	Peers  []string  `json:"peers"`
	Ring   string    `json:"ring"`
	Period syncRange `json:"period"`
	Gaps   []struct {
		Name  string `json:"name"`
		From  int64  `json:"from"`
		Until int64  `json:"until"`
		Open  bool   `json:"open"`
	} `json:"gaps"`
	GapLedgerComplete bool   `json:"gap_ledger_complete"`
	CarbonAPIURL      string `json:"carbonapi_url"`
	State             string `json:"state"`
	Names             string `json:"names"`
}

func expandSyncBlock(args []string) ([]string, error) {
	path := ""
	var rest []string
	for i := 0; i < len(args); i++ {
		value := args[i]
		if value == "--block" {
			if i+1 == len(args) {
				return nil, fmt.Errorf("--block requires a file")
			}
			i++
			value = "--block=" + args[i]
		}
		if strings.HasPrefix(value, "--block=") {
			if path != "" {
				return nil, fmt.Errorf("only one --block allowed")
			}
			path = strings.TrimPrefix(value, "--block=")
		} else {
			rest = append(rest, value)
		}
	}
	if path == "" {
		return args, nil
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var b syncBlock
	d := json.NewDecoder(bytes.NewReader(data))
	d.DisallowUnknownFields()
	if err = d.Decode(&b); err != nil {
		return nil, fmt.Errorf("block: %w", err)
	}
	if b.Target == "" || len(b.Donors) == 0 || b.State == "" || b.CarbonAPIURL == "" {
		return nil, fmt.Errorf("block requires target, donors, state and carbonapi_url")
	}
	var extra any
	if err = d.Decode(&extra); err != io.EOF {
		return nil, fmt.Errorf("block has trailing JSON")
	}
	// Scope is authoritative in a block file. Operational limits and opt-outs
	// remain command-line flags; overriding scope would make the attestation lie.
	for _, v := range rest {
		key := strings.SplitN(strings.TrimLeft(v, "-"), "=", 2)[0]
		switch key {
		case "to", "from", "source", "ring", "period", "window", "names", "prefix", "state", "carbonapi-url", "gap-ledger-complete":
			return nil, fmt.Errorf("--%s cannot override --block scope", key)
		}
	}
	out := []string{"--to", b.Target, "--state", b.State, "--carbonapi-url", b.CarbonAPIURL, "--period", fmt.Sprintf("%d:%d", b.Period.From, b.Period.Until)}
	if b.GapLedgerComplete {
		out = append(out, "--gap-ledger-complete")
	}
	for _, s := range append(append([]string(nil), b.Donors...), b.Peers...) {
		out = append(out, "--from", s)
	}
	if b.Ring != "" {
		out = append(out, "--ring", b.Ring)
	}
	if b.Names != "" {
		out = append(out, "--names", b.Names)
	}
	// Period parity is checked for all names in the same sync, alongside each gap.
	periodName := b.Period.Name
	if periodName == "" {
		periodName = "period"
	}
	if strings.ContainsAny(periodName, ": ,") {
		return nil, fmt.Errorf("invalid period name")
	}
	out = append(out, "--window", fmt.Sprintf("%s:%d:%d", periodName, b.Period.From, b.Period.Until))
	seen := map[string]bool{periodName: true}
	for _, w := range b.Gaps {
		if w.Open {
			return nil, fmt.Errorf("gap still open: %s", w.Name)
		}
		if w.Name == "" || seen[w.Name] || strings.ContainsAny(w.Name, ": ,") {
			return nil, fmt.Errorf("invalid gap name")
		}
		seen[w.Name] = true
		out = append(out, "--window", w.Name+":"+strconv.FormatInt(w.From, 10)+":"+strconv.FormatInt(w.Until, 10))
	}
	out = append(out, "--block="+path)
	return append(out, rest...), nil
}
