package carbon

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/go-graphite/go-carbon/internal/recovery"
)

func TestStartupGenerationDoesNotDependOnOptionalCheckpoint(t *testing.T) {
	for _, format := range []recovery.Format{recovery.FormatLegacy, recovery.FormatCompact, recovery.FormatBlocks} {
		t.Run(format.String(), func(t *testing.T) {
			dir := t.TempDir()
			const suffix = "123.1794277191185205717.bin"
			if err := publishStartupGeneration(dir, format, suffix); err != nil {
				t.Fatal(err)
			}
			// A manifest destination that cannot be replaced reproduces a late
			// publication failure without breaking the authoritative dump/WAL.
			if err := os.Mkdir(filepath.Join(dir, recovery.ManifestName), 0700); err != nil {
				t.Fatal(err)
			}
			wal := recovery.InputName(recovery.FormatCompact, suffix)
			if err := os.WriteFile(filepath.Join(dir, wal), nil, 0600); err != nil {
				t.Fatal(err)
			}
			m, ok, err := startupGenerationManifest(dir)
			if err != nil || !ok || m.Cache.Name != recovery.CacheName(format, suffix) || m.WAL.Name != wal || m.ReadIndexID != "" {
				t.Fatalf("intake routing depended on read checkpoint: %+v %v %v", m, ok, err)
			}
			if err := os.WriteFile(filepath.Join(dir, recovery.InputName(recovery.FormatLegacy, suffix)), nil, 0600); err != nil {
				t.Fatal(err)
			}
			if _, _, err := startupGenerationManifest(dir); err == nil {
				t.Fatal("two possible WAL sources were silently accepted")
			}
		})
	}
}

func TestStartupGenerationRejectsMalformedOrEscapingNames(t *testing.T) {
	dir := t.TempDir()
	for _, body := range []string{
		`{"Version":2,"Format":0,"Suffix":"1.2.bin"}`,
		`{"Version":1,"Format":99,"Suffix":"1.2.bin"}`,
		`{"Version":1,"Format":0,"Suffix":"../1.2.bin"}`,
		`{"Version":1,"Format":0,"Suffix":"1.-2.bin"}`,
		`{"Version":1,"Format":0,"Suffix":"1.02.bin"}`,
		`{"Version":1,"Format":0,"Suffix":"1.2.bin","other":1}`,
		`{"Version":1,"Format":0,"Suffix":"1.2.bin"} {}`,
		strings.Repeat(" ", 4097),
	} {
		if err := os.WriteFile(filepath.Join(dir, startupGenerationName), []byte(body), 0600); err != nil {
			t.Fatal(err)
		}
		if _, _, err := startupGenerationManifest(dir); err == nil {
			t.Fatal("accepted an invalid intake generation", body[:min(len(body), 100)])
		}
	}
}
