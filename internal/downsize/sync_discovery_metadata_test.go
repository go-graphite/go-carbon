package downsize

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

type metadataDiscovery struct {
	*fakeSync
	err   error
	calls int
}

func (b *metadataDiscovery) Info(ctx context.Context, store string, names []string) (map[string][]syncArchive, error) {
	b.calls++
	if b.err != nil {
		return nil, b.err
	}
	return b.fakeSync.Info(ctx, store, names)
}
func (b *metadataDiscovery) Digest(context.Context, string, []string, int64, int64) (map[string][]syncDigest, error) {
	panic("discovery must not read points")
}

// Compare the emitted prefix, ordering and absent-name filtering with the
// catalog oracle across multiple batches. Empty archives still mean present.
func TestScopedDiscoveryUsesMetadata(t *testing.T) {
	for _, after := range []string{"", "scope.0500"} {
		t.Run(after, func(t *testing.T) {
			f := newFakeSync()
			var lines, want []string
			for i := 0; i < 1503; i++ {
				name := fmt.Sprintf("scope.%04d", i)
				lines = append(lines, name)
				if i%3 != 0 {
					f.store(syncT).archives[name] = nil
					if name > after {
						want = append(want, name)
					}
				}
			}
			lines = append(lines, "zzz.outside")
			path := filepath.Join(t.TempDir(), "scope.names")
			if err := os.WriteFile(path, []byte(strings.Join(lines, "\n")+"\n"), 0600); err != nil {
				t.Fatal(err)
			}
			b := &metadataDiscovery{fakeSync: f}
			s := &syncer{o: syncOptions{namesFile: path, prefix: "scope."}, b: b}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			l := s.startScopedList(ctx, syncT, after)
			var got []string
			for {
				if err := l.advance(ctx); err != nil {
					t.Fatal(err)
				}
				if l.done {
					break
				}
				got = append(got, l.head)
			}
			<-l.finished
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("membership mismatch: got %d want %d", len(got), len(want))
			}
			if b.calls < 3 {
				t.Fatalf("expected multiple bounded batches, got %d", b.calls)
			}
		})
	}
}

func TestScopedDiscoveryMetadataError(t *testing.T) {
	path := filepath.Join(t.TempDir(), "scope.names")
	if err := os.WriteFile(path, []byte("name\n"), 0600); err != nil {
		t.Fatal(err)
	}
	sentinel := errors.New("metadata unavailable")
	b := &metadataDiscovery{fakeSync: newFakeSync(), err: sentinel}
	s := &syncer{o: syncOptions{namesFile: path}, b: b}
	l := s.startScopedList(context.Background(), syncT, "")
	if err := l.advance(context.Background()); !errors.Is(err, sentinel) {
		t.Fatalf("lost metadata error: %v", err)
	}
	<-l.finished
}
