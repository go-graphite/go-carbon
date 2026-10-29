//go:build linux && (amd64 || arm64)

package whisperio

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
	"unsafe"

	whisper "github.com/go-graphite/go-whisper"
	"golang.org/x/sys/unix"
)

func TestSameCaptureXFSDetectsMainAndSidecarCoW(t *testing.T) {
	root := t.TempDir()
	var fs unix.Statfs_t
	if err := unix.Statfs(root, &fs); err != nil {
		t.Fatal(err)
	}
	if fs.Type != unix.XFS_SUPER_MAGIC {
		t.Skip("native XFS proof requires TMPDIR on an owned XFS directory")
	}
	source, copyPath := filepath.Join(root, "source.wsp"), filepath.Join(root, "copy.wsp")
	now := int(time.Now().Unix())
	w, err := whisper.CreateWithOptions(source, whisper.MustParseRetentionDefs("1s:1h"), whisper.Average, 0.5, &whisper.Options{Compressed: true, OutOfOrder: true})
	if err != nil {
		t.Fatal(err)
	}
	if err := w.UpdateMany([]*whisper.TimeSeriesPoint{{Time: now - 4, Value: 1}, {Time: now - 2, Value: 2}}); err != nil {
		t.Fatal(err)
	}
	// A correction inside the compressed mutable buffer need not create OOO
	// data. Go back beyond that buffer so this fixture really has a sidecar.
	if err := w.UpdateMany([]*whisper.TimeSeriesPoint{{Time: now - 900, Value: 111.25}}); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	// XFS can leave unwritten zero extents in a sparse OOO sidecar, whereas
	// reflink copies represent those as holes. Such maps deliberately fall back
	// to verification. Initialize this small fixture to exercise positive OOO
	// equality as well as its CoW change and missing-sidecar detection.
	sidecar := whisper.OutOfOrderSidecarPath(source)
	data, err := os.ReadFile(sidecar)
	if err != nil {
		t.Fatal(err)
	}
	f, err := os.OpenFile(sidecar, os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteAt(data, 0); err != nil {
		t.Fatal(err)
	}
	if err := f.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := Capture(ctx, source, copyPath, true); err != nil {
		t.Fatal(err)
	}
	assertSame := func(want bool) {
		t.Helper()
		got, err := SameCapture(ctx, source, copyPath)
		if err != nil || got != want {
			for _, path := range []string{source, copyPath, whisper.OutOfOrderSidecarPath(source), whisper.OutOfOrderSidecarPath(copyPath)} {
				f, openErr := os.Open(path)
				if openErr != nil {
					t.Logf("extent diagnostic %s: %v", path, openErr)
					continue
				}
				r := fiemapRequest{Length: ^uint64(0), Flags: fiemapSync, Count: 64}
				_, _, errno := unix.Syscall(unix.SYS_IOCTL, f.Fd(), fiemapIOCTL, uintptr(unsafe.Pointer(&r)))
				t.Logf("extent diagnostic %s: errno=%v mapped=%d extents=%+v", path, errno, r.Mapped, r.Extents[:min(r.Mapped, 64)])
				_ = f.Close()
			}
			t.Fatalf("same=%v want=%v error=%v", got, want, err)
		}
	}
	assertSame(true)
	mutate := func(path string) {
		t.Helper()
		f, err := os.OpenFile(path, os.O_RDWR, 0)
		if err != nil {
			t.Fatal(err)
		}
		defer f.Close()
		info, err := f.Stat()
		if err != nil {
			t.Fatal(err)
		}
		var last [1]byte
		if _, err := f.ReadAt(last[:], info.Size()-1); err != nil {
			t.Fatal(err)
		}
		last[0] ^= 1
		if _, err := f.WriteAt(last[:], info.Size()-1); err != nil {
			t.Fatal(err)
		}
	}
	// Do not fsync here: synchronization in FIEMAP must detect dirty CoW data.
	mutate(source)
	assertSame(false)
	if err := Capture(ctx, source, copyPath, true); err != nil {
		t.Fatal(err)
	}
	assertSame(true)
	if _, err := os.Stat(sidecar); err != nil {
		t.Fatal(err)
	}
	mutate(sidecar)
	assertSame(false)
	if err := Capture(ctx, source, copyPath, true); err != nil {
		t.Fatal(err)
	}
	assertSame(true)
	if err := os.Remove(whisper.OutOfOrderSidecarPath(copyPath)); err != nil {
		t.Fatal(err)
	}
	assertSame(false)
}
