package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/go-graphite/go-carbon/internal/v2placement"
)

// placement selects exact names under the same frozen contract as store intake.
// This is read-only planning: it never treats a selection as a move certificate.
func placement(args []string, out io.Writer) error {
	fs := flag.NewFlagSet("placement", flag.ContinueOnError)
	file := fs.String("map", "", "placement JSON document")
	name := fs.String("name", "", "exact post-rewrite metric name")
	replica := fs.String("replica", "", "replica domain")
	expected := fs.String("expect-digest", "", "required map digest")
	epoch := fs.Uint64("expect-epoch", 0, "required write epoch")
	if e := fs.Parse(args); e != nil {
		return e
	}
	if fs.NArg() != 0 || *file == "" || *name == "" || *replica == "" || *expected == "" || *epoch == 0 {
		return fmt.Errorf("placement requires --map, --name, --replica, --expect-digest and --expect-epoch")
	}
	f, e := os.Open(*file)
	if e != nil {
		return e
	}
	defer f.Close()
	d, e := v2placement.Decode(f)
	if e != nil {
		return e
	}
	selection, e := d.Select(*name, *replica)
	if e != nil {
		return e
	}
	if selection.MapDigest != *expected || selection.Replica.Epoch != *epoch {
		return v2placement.ErrStaleEpoch
	}
	return json.NewEncoder(out).Encode(selection)
}
