package carbon

import (
	"testing"

	"github.com/BurntSushi/toml"
)

func TestFetchSharedIteratorDefaultAndOptOut(t *testing.T) {
	cfg := NewConfig()
	if !cfg.Whisper.StoreFetchSharedIterator {
		t.Fatal("range reads must reuse their shared iterator by default")
	}
	if _, err := toml.Decode("[whisper]\nstore-fetch-shared-iterator = false\n", cfg); err != nil {
		t.Fatal(err)
	}
	if cfg.Whisper.StoreFetchSharedIterator {
		t.Fatal("explicit opt-out was ignored")
	}
}
