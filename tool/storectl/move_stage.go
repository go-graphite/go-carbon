package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"io"
	"os"
	"path/filepath"

	"github.com/cockroachdb/pebble"
	"github.com/go-graphite/go-carbon/internal/v2core"
	"github.com/go-graphite/go-carbon/internal/v2part"
)

// move-stage owns an independent read catalog, never a daemon owner directory.
// Bytes have already arrived through sync --parts. No routing/read switch is
// certified: live catalog and reducer adoption remain separate prerequisites.
func moveStage(args []string, out io.Writer) error {
	fs := flag.NewFlagSet("move-stage", flag.ContinueOnError)
	root := fs.String("catalog-dir", "", "independent offline move read catalog")
	part := fs.String("part", "", "verified source SST (retained, never consumed)")
	descriptor := fs.String("descriptor", "", "canonical descriptor for this part")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 0 || *root == "" || *part == "" || *descriptor == "" {
		return errors.New("move-stage requires --catalog-dir, --part and --descriptor")
	}
	b, err := os.ReadFile(*descriptor)
	if err != nil {
		return err
	}
	d, err := v2part.ParseDescriptor(b)
	if err != nil {
		return err
	}
	if err = os.MkdirAll(*root, 0700); err != nil {
		return err
	}
	marker := filepath.Join(*root, "MOVECATALOG")
	const identity = "go-carbon-offline-move-catalog-v1\n"
	if b, err = os.ReadFile(marker); err == nil {
		if string(b) != identity {
			return errors.New("invalid offline move catalog identity")
		}
	} else if errors.Is(err, os.ErrNotExist) {
		entries, e := os.ReadDir(*root)
		if e != nil {
			return e
		}
		if len(entries) != 0 {
			return errors.New("refusing nonempty directory without MOVECATALOG identity")
		}
		f, e := os.OpenFile(marker, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		if e != nil {
			return e
		}
		_, e = f.WriteString(identity)
		if e == nil {
			e = f.Sync()
		}
		e = errors.Join(e, f.Close())
		if e != nil {
			return e
		}
		if e = v2core.SyncPartDirectory(*root); e != nil {
			return e
		}
	} else {
		return err
	}
	db, err := pebble.Open(filepath.Join(*root, "read-index"), &pebble.Options{})
	if err != nil {
		return err
	}
	defer db.Close()
	installed, err := v2core.InstallCopiedPart(context.Background(), db, *root, *part, d)
	if err != nil {
		return err
	}
	return json.NewEncoder(out).Encode(struct {
		Installed bool
		Class     uint8
		SHA256    v2part.Digest
		Ready     bool
	}{installed, v2core.CopiedPart, d.SHA256, false})
}
