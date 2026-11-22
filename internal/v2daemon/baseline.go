package v2daemon

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"github.com/go-graphite/go-carbon/internal/partlog"
	"github.com/go-graphite/go-carbon/internal/v2convert"
	"github.com/go-graphite/go-carbon/internal/v2core"
	"github.com/go-graphite/go-carbon/internal/v2part"
	"github.com/go-graphite/go-carbon/internal/v2recv"
)

// installBaseline runs under the daemon root lock. The placement-group rename
// publishes catalogs, original archive occupancy, policy and ordering authority
// together; no engine or reader exists until that rename has been synced.
// Conversion output is an exclusively owned, fenced checkpoint, never a live
// classic path. Once installed, restart needs only this root and its journal.
func installBaseline(root string, c Config) (uint64, error) {
	group := filepath.Join(root, "placement-group-00000001")
	marker := filepath.Join(group, "BASELINE.json")
	var installed v2convert.NativeBaseline
	if b, err := os.ReadFile(marker); err == nil {
		if json.Unmarshal(b, &installed) != nil || installed.Validate() != nil {
			return 0, errors.New("invalid installed baseline authority")
		}
		return installed.Order, nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return 0, err
	}
	if c.BaselinePath == "" {
		return 0, nil
	}
	if c.Layer != 3 {
		return 0, errors.New("baseline requires the rollup layer")
	}
	if _, err := os.Stat(group); !errors.Is(err, os.ErrNotExist) {
		return 0, errors.New("baseline must be installed before first admission")
	}
	data, err := os.ReadFile(filepath.Join(c.BaselinePath, "receipt.json"))
	if err != nil {
		return 0, err
	}
	var receipt v2convert.NativeReceipt
	if json.Unmarshal(data, &receipt) != nil || receipt.Version != 1 || receipt.Adapter != "native-baseline-v1" || receipt.Baseline.Validate() != nil || receipt.Baseline.Group != "local" || receipt.SourceSHA != hex.EncodeToString(receipt.Baseline.Snapshot[:]) || len(receipt.Names) == 0 || len(receipt.Files) == 0 || receipt.Entries == 0 || receipt.Writer != (v2convert.CoreWriter{Source: receipt.Baseline.Source}).Format() {
		return 0, errors.New("invalid converted baseline receipt")
	}
	if err := v2convert.Verify(context.Background(), c.BaselinePath, receipt.Receipt); err != nil {
		return 0, err
	}
	// Materialization has a fixed startup bound, independent of incoming traffic.
	const maxBaselineBytes = 256 << 20
	used := 0
	var rows []v2core.KV
	// A metric's envelope repeats once per month partition it covers.
	metadata := map[string][]int{}
	envelopes := map[struct {
		name      string
		partition int64
	}]bool{}
	listed := map[string]bool{}
	for _, name := range receipt.Names {
		if name == "" || listed[name] {
			return 0, errors.New("duplicate/empty baseline name")
		}
		listed[name] = true
	}
	files := map[string]bool{}
	for _, f := range receipt.Files {
		files[f.Name] = true
	}
	for _, f := range receipt.Files {
		if strings.HasSuffix(f.Name, ".descriptor") {
			continue
		}
		if !strings.HasSuffix(f.Name, ".sst") || !files[f.Name+".descriptor"] {
			return 0, errors.New("baseline part lacks descriptor")
		}
		descBytes, err := os.ReadFile(filepath.Join(c.BaselinePath, f.Name+".descriptor"))
		if err != nil {
			return 0, err
		}
		desc, err := v2part.ParseDescriptor(descBytes)
		if err != nil {
			return 0, err
		}
		if desc.Source.Incarnation != receipt.Baseline.Source.Incarnation || desc.Source.Stream != receipt.Baseline.Source.Stream || desc.Scope.Group != receipt.Baseline.Group || desc.Scope.Part != receipt.Baseline.Part {
			return 0, errors.New("baseline part authority mismatch")
		}
		err = (v2convert.CoreWriter{}).Read(context.Background(), filepath.Join(c.BaselinePath, f.Name), func(row v2convert.Record) error {
			if len(row.Key)+len(row.Value)+48 > maxBaselineBytes-used {
				return errors.New("baseline exceeds startup memory bound")
			}
			used += len(row.Key) + len(row.Value) + 48
			k, err := v2part.Decode(row.Key)
			if err != nil {
				return err
			}
			if !listed[string(k.Metric)] || k.Group != receipt.Baseline.Group || k.Part != receipt.Baseline.Part {
				return errors.New("baseline record outside certified selection")
			}
			if k.Archive == v2part.MetadataArchive {
				id := struct {
					name      string
					partition int64
				}{string(k.Metric), k.Partition}
				if envelopes[id] {
					return errors.New("duplicate baseline metadata")
				}
				envelopes[id] = true
				metadata[string(k.Metric)] = append(metadata[string(k.Metric)], len(rows))
			}
			rows = append(rows, v2core.KV{Key: bytes.Clone(row.Key), Value: bytes.Clone(row.Value)})
			return nil
		})
		if err != nil {
			return 0, err
		}
	}
	if len(metadata) != len(listed) || uint64(len(rows)) != receipt.Entries {
		return 0, errors.New("incomplete baseline selection")
	}
	stage, err := os.MkdirTemp(root, ".baseline-building-")
	if err != nil {
		return 0, err
	}
	defer os.RemoveAll(stage)
	catalogs := make([]*partlog.NameCatalog, c.Producers)
	defer func() {
		for _, catalog := range catalogs {
			if catalog != nil {
				_ = catalog.Close()
			}
		}
	}()
	for i := range catalogs {
		catalogs[i], err = partlog.OpenNameCatalog(filepath.Join(stage, "journal", fmt.Sprintf("producer-%03d", i), "catalog"))
		if err != nil {
			return 0, err
		}
	}
	names := slices.Clone(receipt.Names)
	slices.Sort(names)
	for _, name := range names {
		indexes := metadata[name]
		for _, index := range indexes[1:] {
			if !bytes.Equal(rows[index].Value, rows[indexes[0]].Value) {
				return 0, errors.New("baseline envelope copies differ")
			}
		}
		m, err := v2core.DecodeRollupMetadata(v2part.ID(name), receipt.Baseline.Group, rows[indexes[0]].Value)
		if err != nil {
			return 0, err
		}
		var p v2core.RollupPolicy
		if json.Unmarshal(m.Policy, &p) != nil || string(p.Name) != name || p.Group != receipt.Baseline.Group || m.Frontier.Order != receipt.Baseline.Order || m.Expired || m.ResetOrder != 0 {
			return 0, errors.New("baseline policy/frontier mismatch")
		}
		canonical, err := v2core.EncodeRollupPolicy(p)
		if err != nil || !bytes.Equal(canonical, m.Policy) {
			return 0, errors.New("invalid baseline policy")
		}
		producer := v2recv.OwnerFor(name, c.Producers)
		id, err := catalogs[producer].Intern(name, m.Policy)
		if err != nil {
			return 0, err
		}
		m.ID = NumericID(id, producer, c.Producers)
		m.BaselineOrder = receipt.Baseline.Order
		value := v2core.AppendRollupMetadata(nil, m)
		for _, index := range indexes {
			rows[index].Value = value
		}
	}
	for i, catalog := range catalogs {
		if err := catalog.Sync(); err != nil {
			return 0, err
		}
		if err := catalog.Close(); err != nil {
			return 0, err
		}
		catalogs[i] = nil
	}
	if err := v2core.BootstrapRollups(filepath.Join(stage, "core"), coreConfig(c), rows, receipt.Baseline.Source, c.Hooks.Stage); err != nil {
		return 0, err
	}
	markerBytes, _ := json.Marshal(receipt.Baseline)
	f, err := os.OpenFile(filepath.Join(stage, "BASELINE.json"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return 0, err
	}
	_, err = f.Write(markerBytes)
	if err = errors.Join(err, f.Sync(), f.Close()); err != nil {
		return 0, err
	}
	// Sync ancestor directory entries too: catalogs and owner manifests must all
	// survive before the single visibility boundary is published.
	var directories []string
	err = filepath.WalkDir(stage, func(path string, d os.DirEntry, e error) error {
		if e == nil && d.IsDir() {
			directories = append(directories, path)
		}
		return e
	})
	if err != nil {
		return 0, err
	}
	for i := len(directories) - 1; i >= 0; i-- {
		if err := syncDir(directories[i]); err != nil {
			return 0, err
		}
	}
	if c.Hooks.Stage != nil {
		c.Hooks.Stage("baseline-ready")
	}
	if err := os.Rename(stage, group); err != nil {
		return 0, err
	}
	if err := syncDir(root); err != nil {
		return 0, err
	}
	if c.Hooks.Stage != nil {
		c.Hooks.Stage("baseline-installed")
	}
	return receipt.Baseline.Order, nil
}
