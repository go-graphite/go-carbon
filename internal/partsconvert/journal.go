package partsconvert

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/go-graphite/go-carbon/internal/partsformat"
)

// Canonical names and placement labels may contain arbitrary bytes. JSON
// strings would replace invalid UTF-8, so cursor boundaries use base64 bytes.
func (u Unit) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Group        []byte
		VShard       uint16
		Lower, Upper []byte
		Final        bool
	}{[]byte(u.Group), u.VShard, []byte(u.Lower), []byte(u.Upper), u.Final})
}
func (u *Unit) UnmarshalJSON(b []byte) error {
	var wire struct {
		Group        []byte
		VShard       uint16
		Lower, Upper []byte
		Final        bool
	}
	d := json.NewDecoder(bytes.NewReader(b))
	d.DisallowUnknownFields()
	if err := d.Decode(&wire); err != nil {
		return err
	}
	*u = Unit{Group: string(wire.Group), VShard: wire.VShard, Lower: string(wire.Lower), Upper: string(wire.Upper), Final: wire.Final}
	return nil
}

func unitName(u Unit) string {
	identity, _ := json.Marshal(u)
	sum := sha256.Sum256(identity)
	return fmt.Sprintf("%x-%04d", sum[:16], u.VShard)
}

// A synced cursor is insufficient if a power loss can discard its parent
// directory entry. Make each newly created directory durable before ingest.
func makeDurableDirectory(dir string) error {
	info, err := os.Stat(dir)
	if err == nil {
		if !info.IsDir() {
			return fmt.Errorf("conversion directory is not a directory: %s", dir)
		}
		return nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	parent := filepath.Dir(dir)
	if parent == dir {
		return err
	}
	if err = makeDurableDirectory(parent); err != nil {
		return err
	}
	if err = os.Mkdir(dir, 0750); err != nil && !errors.Is(err, os.ErrExist) {
		return err
	}
	f, err := os.Open(parent)
	if err != nil {
		return err
	}
	return errors.Join(f.Sync(), f.Close())
}

func readReceipt(dir string) (Receipt, error) {
	var r Receipt
	f, err := os.Open(filepath.Join(dir, "cursor.json"))
	if err != nil {
		return r, err
	}
	defer f.Close()
	dec := json.NewDecoder(io.LimitReader(f, 64<<20))
	dec.DisallowUnknownFields()
	if err = dec.Decode(&r); err != nil {
		return r, err
	}
	var extra any
	if err = dec.Decode(&extra); err != io.EOF {
		return r, errors.New("invalid trailing conversion cursor")
	}
	return r, nil
}
func writeReceipt(dir string, r Receipt) error {
	b, err := json.Marshal(r)
	if err != nil {
		return err
	}
	if len(b) >= 64<<20 {
		return errors.New("conversion cursor exceeds 64MiB")
	}
	f, err := os.OpenFile(filepath.Join(dir, "cursor.tmp"), os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		return err
	}
	_, err = f.Write(b)
	err = errors.Join(err, f.Sync(), f.Close())
	if err != nil {
		return err
	}
	if err = os.Rename(filepath.Join(dir, "cursor.tmp"), filepath.Join(dir, "cursor.json")); err != nil {
		return err
	}
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	return errors.Join(d.Sync(), d.Close())
}
func validateReceipt(r Receipt, dir string) error {
	if token, err := hex.DecodeString(r.Token); err != nil || len(token) != 16 {
		return errors.New("invalid conversion token")
	}
	switch r.State {
	case "building", "prepared", "ingested", "ready", "published", "reclaimed":
	default:
		return errors.New("invalid conversion cursor state")
	}
	for i, t := range r.Tables {
		if t.Path != filepath.Join(dir, fmt.Sprintf("part-%08d.sst", i)) || t.Records == 0 {
			return errors.New("invalid conversion table path/count")
		}
		lo, err := partsformat.Decode(t.Smallest)
		if err != nil {
			return err
		}
		hi, err := partsformat.Decode(t.Largest)
		if err != nil {
			return err
		}
		if lo.VShard != r.Unit.VShard || hi.VShard != r.Unit.VShard || string(lo.Group) != r.Unit.Group || string(hi.Group) != r.Unit.Group || lo.Part != hi.Part || lo.Partition != hi.Partition || bytes.Compare(t.Smallest, t.Largest) > 0 {
			return errors.New("conversion table crosses ownership/partition")
		}
		prefix, err := partsformat.PartitionPrefixForGroup(lo.Group, lo.VShard, lo.Partition)
		if err != nil {
			return err
		}
		if !bytes.Equal(t.Prefix, append(prefix, lo.Part[:]...)) {
			return errors.New("invalid conversion part prefix")
		}
		if sum, err := hex.DecodeString(t.Digest); err != nil || len(sum) != 32 {
			return errors.New("invalid conversion digest")
		}
	}
	return nil
}
func cleanStage(dir string) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), "part-") && (strings.HasSuffix(e.Name(), ".sst") || strings.HasSuffix(e.Name(), ".sst.ingest")) {
			if err = os.Remove(filepath.Join(dir, e.Name())); err != nil {
				return err
			}
		}
	}
	return nil
}

// Ready reads a verified receipt for the mixed-reader owner. Readers must still
// pin the target snapshot and apply the stored cutover/legacy precedence rules.
// It is not permission to reclaim an old source or discard its reader pins.
func Ready(directory string, u Unit, source, destination string) (Receipt, error) {
	dir := filepath.Join(directory, unitName(u))
	r, err := readReceipt(dir)
	if err != nil {
		return r, err
	}
	if err = validateReceipt(r, dir); err != nil {
		return r, err
	}
	if r.Source != source || r.Destination != destination || r.Unit != u || r.Version != 1 || (r.State != "ready" && r.State != "published" && r.State != "reclaimed") {
		return r, errors.New("vshard conversion is not verified for this source/destination")
	}
	return r, nil
}
