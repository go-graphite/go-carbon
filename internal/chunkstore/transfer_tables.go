package chunkstore

import (
	"fmt"
	"strings"

	"github.com/cockroachdb/pebble/objstorage/objstorageprovider"
	"github.com/cockroachdb/pebble/sstable"
)

func (s *Store) writeTransferTables(first uint64, records []TransferMetric, metas []Metadata) ([]string, error) {
	base := s.fs.PathJoin(s.dir, fmt.Sprintf("transfer-%020d", first))
	if records[0].Continuation {
		entries := make([]*fileIngestEntry, len(records))
		for i, r := range records {
			e := &fileIngestEntry{m: metas[i]}
			for _, kv := range r.Keys {
				e.keys = append(e.keys, kv.Key)
				e.values = append(e.values, kv.Value)
			}
			entries[i] = e
		}
		paths, err := s.writeFileStageTables(base, entries)
		if err != nil {
			if names, listErr := s.fs.List(s.dir); listErr == nil {
				for _, name := range names {
					if strings.HasPrefix(name, s.fs.PathBase(base)) {
						_ = s.fs.Remove(s.fs.PathJoin(s.dir, name))
					}
				}
			}
		}
		return paths, err
	}
	path := base + ".sst"
	f, err := s.fs.Create(path)
	if err != nil {
		return nil, err
	}
	paths := []string{path}
	w := sstable.NewWriter(objstorageprovider.NewFileWritable(f), s.pebbleOptions.MakeWriterOptions(numLevels-1, s.db.FormatMajorVersion().MaxTableFormat()))
	entries := 0
	for i, r := range records {
		prefix := chunkPrefix(metas[i].ID, metas[i].Generation)
		for _, kv := range r.Keys {
			key := append(append([]byte(nil), prefix...), kv.Key...)
			if err := w.Set(key, kv.Value); err != nil {
				_ = w.Close()
				return paths, err
			}
			entries++
		}
	}
	if err := w.Close(); err != nil {
		return paths, err
	}
	if entries == 0 {
		_ = s.fs.Remove(path)
		return nil, nil
	}
	return paths, nil
}
