package chunkstore

import (
	"errors"
	"fmt"

	"github.com/lomik/zapwriter"
	"go.uber.org/zap"
)

func (s *Store) nativeSidecarSummary(record TransferMetric) (summary SidecarSummary, err error) {
	defer func() {
		if err != nil {
			err = fmt.Errorf("native sidecar summary metric %q key %q: %w", record.Config.Name, SidecarSummaryKey, err)
			// The import caller also increments the native errors counter.
			zapwriter.Logger("storage").Error("native sidecar summary failed", zap.String("metric", record.Config.Name), zap.String("key", string(SidecarSummaryKey)), zap.Error(err))
		}
	}()
	kv := transferRecordIO(record)
	value, err := kv.Get(SidecarSummaryKey)
	if err == nil {
		return DecodeSidecarSummary(value)
	}
	if !errors.Is(err, ErrNotFound) {
		return summary, err
	}
	if reader, ok := s.continuation.(ContinuationSidecarReader); ok {
		return reader.ReadSidecar(kv)
	}
	if inspector, ok := s.continuation.(ContinuationSidecars); ok {
		return inspector.InspectSidecar(kv)
	}
	return summary, nil
}
