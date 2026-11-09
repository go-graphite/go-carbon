package chunkstore

import (
	"encoding/json"
	"time"
)

// Metadata's public JSON deliberately hides ownership, revision and storage
// interpretation. Durable combine state needs every one of those fields.
// Missing fields fail closed instead of treating a continuation as chunks.
type nativeCombineMetadata Metadata

type nativeCombineMetadataWire struct {
	Metadata
	Revision      *uint64
	LastUpdate    *time.Time
	FileMigration *FileMigrationState
	Continuation  *bool
}

func (m nativeCombineMetadata) MarshalJSON() ([]byte, error) {
	v := Metadata(m)
	return json.Marshal(nativeCombineMetadataWire{Metadata: v, Revision: &v.Revision, LastUpdate: &v.LastUpdate, FileMigration: &v.FileMigration, Continuation: &v.Continuation})
}

func (m *nativeCombineMetadata) UnmarshalJSON(data []byte) error {
	var v nativeCombineMetadataWire
	if err := json.Unmarshal(data, &v); err != nil {
		return err
	}
	if v.Revision == nil || v.LastUpdate == nil || v.FileMigration == nil || v.Continuation == nil {
		return ErrFormat
	}
	v.Metadata.Revision, v.Metadata.LastUpdate = *v.Revision, *v.LastUpdate
	v.Metadata.FileMigration, v.Metadata.Continuation = *v.FileMigration, *v.Continuation
	*m = nativeCombineMetadata(v.Metadata)
	return nil
}
