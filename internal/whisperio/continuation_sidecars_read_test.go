package whisperio

import (
	"bytes"
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/cockroachdb/pebble/vfs"
	store "github.com/go-graphite/go-carbon/internal/chunkstore"
)

type immutableSidecarIO struct{ store.ContinuationIO }

func (immutableSidecarIO) Set([]byte, []byte) error { panic("sidecar inspection wrote to snapshot") }
func (immutableSidecarIO) Delete([]byte) error      { panic("sidecar inspection deleted from snapshot") }
func (immutableSidecarIO) DeletePrefix([]byte) error {
	panic("sidecar inspection deleted snapshot prefix")
}

func TestReadSidecarLegacyMetadata(t *testing.T) {
	codec := &ContinuationCodec{}
	opts := coldOptions(codec)
	db, err := store.Open("/db", opts.WithFS(vfs.NewMem()))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	seedSidecarHistory(t, db, "legacy", opts.Now().Unix(), 2000)
	if err := db.UpdateMany(context.Background(), "legacy", []store.Point{{Timestamp: opts.Now().Unix() - 1500, Value: 123.25}}); err != nil {
		t.Fatal(err)
	}
	if db.Sidecars().Count != 1 {
		t.Fatalf("fixture inventory: %+v", db.Sidecars())
	}
	records, err := db.ExportTransfer(context.Background(), []string{"legacy"})
	if err != nil {
		t.Fatal(err)
	}
	kv := mapContinuationIO{}
	for _, item := range records[0].Keys {
		if bytes.Equal(item.Key, store.SidecarSummaryKey) || len(item.Key) >= 10 && item.Key[0] == 'f' && strings.ContainsRune("qrs", rune(item.Key[9])) {
			continue
		}
		kv[string(item.Key)] = bytes.Clone(item.Value)
	}
	before := mapContinuationIO{}
	for k, v := range kv {
		before[k] = bytes.Clone(v)
	}
	summary, err := codec.ReadSidecar(immutableSidecarIO{kv})
	if err != nil || !summary.Present || summary.Points == 0 || summary.Oldest == 0 {
		t.Fatal("physical inventory", summary, err)
	}
	if !reflect.DeepEqual(before, kv) {
		t.Fatal("read-only inventory mutated physical keys")
	}
	persisted, err := codec.InspectSidecar(kv)
	if err != nil || persisted != summary {
		t.Fatal("writable inventory differs", persisted, summary, err)
	}
	value, err := kv.Get(store.SidecarSummaryKey)
	if err != nil {
		t.Fatal("writable inventory did not persist summary", err)
	}
	decoded, err := store.DecodeSidecarSummary(value)
	if err != nil || decoded != summary {
		t.Fatal("persisted inventory differs", decoded, summary, err)
	}
	// Corruption is propagated instead of interpreting a damaged sidecar as empty.
	for key := range before {
		if len(key) >= 10 && key[0] == 'f' && key[9] == 'p' {
			before[key] = []byte("corrupt")
			if _, err := codec.ReadSidecar(immutableSidecarIO{before}); err == nil {
				t.Fatal("corrupt physical sidecar accepted")
			}
			break
		}
	}
	empty, err := codec.ReadSidecar(immutableSidecarIO{mapContinuationIO{}})
	if err != nil || empty != (store.SidecarSummary{}) {
		t.Fatal("missing physical sidecar", empty, err)
	}
	if _, err := before.Get(store.SidecarSummaryKey); !errors.Is(err, store.ErrNotFound) {
		t.Fatal("read-only inventory persisted summary", err)
	}
}
