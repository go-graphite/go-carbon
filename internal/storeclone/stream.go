package storeclone

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Wire format per connection: magic, then per file a uint16 name length, the
// name, a uint64 size, the bytes and their SHA-256; a zero name length ends
// the connection and the receiver answers "OK\n" once every file is synced.
// The last connection carries ListName, the sender's list of all files; the
// receiver checks it against what it received and stops.
const (
	magic    = "STORECLONE1\n"
	ListName = "CLONE-FILES.json"
	chunk    = 1 << 20
)

// Gate pauses sending; Wait blocks while paused.
type Gate interface{ Wait(context.Context) error }

type openGate struct{}

func (openGate) Wait(context.Context) error { return nil }

// Progress counts bytes and files for the caller's reporter.
type Progress struct{ Bytes, Files atomic.Int64 }

// Send streams files from staging to addr over streams parallel connections.
func Send(ctx context.Context, staging string, files []File, addr string, streams int, gate Gate, progress *Progress) error {
	if gate == nil {
		gate = openGate{}
	}
	if progress == nil {
		progress = &Progress{}
	}
	var next atomic.Int64
	errs := make([]error, streams)
	var wg sync.WaitGroup
	for i := 0; i < streams; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			errs[i] = sendStream(ctx, addr, func() (*File, bool) {
				n := next.Add(1) - 1
				if n >= int64(len(files)) {
					return nil, false
				}
				return &files[n], true
			}, func(f *File, w io.Writer) error { return sendFile(ctx, staging, f, w, gate, progress) })
		}(i)
	}
	wg.Wait()
	if err := errors.Join(errs...); err != nil {
		return err
	}
	list, err := json.Marshal(files)
	if err != nil {
		return err
	}
	sent := false
	return sendStream(ctx, addr, func() (*File, bool) {
		if sent {
			return nil, false
		}
		sent = true
		return &File{Name: ListName, Size: int64(len(list))}, true
	}, func(f *File, w io.Writer) error {
		sum := sha256.Sum256(list)
		_, err := w.Write(append(list, sum[:]...))
		return err
	})
}

func sendStream(ctx context.Context, addr string, next func() (*File, bool), body func(*File, io.Writer) error) error {
	var d net.Dialer
	conn, err := d.DialContext(ctx, "tcp", addr)
	if err != nil {
		return err
	}
	defer conn.Close()
	stop := context.AfterFunc(ctx, func() { _ = conn.Close() })
	defer stop()
	w := bufio.NewWriterSize(conn, 4*chunk)
	if _, err := w.WriteString(magic); err != nil {
		return err
	}
	for {
		f, ok := next()
		if !ok {
			break
		}
		var header [2]byte
		binary.BigEndian.PutUint16(header[:], uint16(len(f.Name)))
		if _, err := w.Write(append(header[:], f.Name...)); err != nil {
			return err
		}
		if err := binary.Write(w, binary.BigEndian, uint64(f.Size)); err != nil {
			return err
		}
		if err := body(f, w); err != nil {
			return fmt.Errorf("%s: %w", f.Name, err)
		}
	}
	if _, err := w.Write([]byte{0, 0}); err != nil {
		return err
	}
	if err := w.Flush(); err != nil {
		return err
	}
	reply, err := bufio.NewReader(conn).ReadString('\n')
	if err != nil {
		return fmt.Errorf("receiver: %w", err)
	}
	if reply != "OK\n" {
		return fmt.Errorf("receiver: %s", strings.TrimSpace(reply))
	}
	return ctx.Err()
}

func sendFile(ctx context.Context, staging string, f *File, w io.Writer, gate Gate, progress *Progress) error {
	in, err := os.Open(filepath.Join(staging, f.Name))
	if err != nil {
		return err
	}
	defer in.Close()
	h := sha256.New()
	buf := make([]byte, chunk)
	for left := f.Size; left > 0; {
		if err := gate.Wait(ctx); err != nil {
			return err
		}
		n, err := io.ReadFull(in, buf[:min(left, chunk)])
		if err != nil {
			return err
		}
		h.Write(buf[:n])
		if _, err := w.Write(buf[:n]); err != nil {
			return err
		}
		left -= int64(n)
		progress.Bytes.Add(int64(n))
	}
	sum := h.Sum(nil)
	f.SHA256 = hex.EncodeToString(sum)
	progress.Files.Add(1)
	_, err = w.Write(sum)
	return err
}

// Receive accepts connections on ln and writes files into dir (created if
// missing, must hold no file of the same name) until the sender's list
// arrives. It then checks every listed file against what it received and
// writes ListName into dir.
func Receive(ctx context.Context, ln net.Listener, dir string, progress *Progress) ([]File, error) {
	if progress == nil {
		progress = &Progress{}
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	var mu sync.Mutex
	got := map[string]File{}
	done := make(chan []File, 1)
	failed := make(chan error, 1)
	go func() {
		<-ctx.Done()
		_ = ln.Close()
	}()
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				failed <- err
				return
			}
			go func() {
				defer conn.Close()
				list, err := receiveStream(conn, dir, progress, func(f File) {
					mu.Lock()
					got[f.Name] = f
					mu.Unlock()
				})
				if err == nil && list != nil {
					mu.Lock()
					err = checkList(list, got)
					mu.Unlock()
					if err == nil {
						err = writeList(dir, list)
					}
				}
				if err != nil {
					_, _ = fmt.Fprintf(conn, "ERR %v\n", strings.ReplaceAll(err.Error(), "\n", " "))
					select {
					case failed <- err:
					default:
					}
					return
				}
				_, _ = io.WriteString(conn, "OK\n")
				if list != nil {
					done <- list
				}
			}()
		}
	}()
	select {
	case list := <-done:
		return list, nil
	case err := <-failed:
		return nil, err
	}
}

func receiveStream(conn net.Conn, dir string, progress *Progress, record func(File)) ([]File, error) {
	r := bufio.NewReaderSize(conn, 4*chunk)
	head := make([]byte, len(magic))
	if _, err := io.ReadFull(r, head); err != nil || string(head) != magic {
		return nil, fmt.Errorf("bad stream header from %s", conn.RemoteAddr())
	}
	var list []File
	for {
		var n uint16
		if err := binary.Read(r, binary.BigEndian, &n); err != nil {
			return nil, err
		}
		if n == 0 {
			break
		}
		name := make([]byte, n)
		if _, err := io.ReadFull(r, name); err != nil {
			return nil, err
		}
		var size uint64
		if err := binary.Read(r, binary.BigEndian, &size); err != nil {
			return nil, err
		}
		if !validName(string(name)) {
			return nil, fmt.Errorf("invalid file name %q", name)
		}
		h := sha256.New()
		if string(name) == ListName {
			data := make([]byte, size)
			if _, err := io.ReadFull(r, data); err != nil {
				return nil, err
			}
			h.Write(data)
			if err := checkSum(r, h.Sum(nil)); err != nil {
				return nil, err
			}
			if err := json.Unmarshal(data, &list); err != nil {
				return nil, err
			}
			continue
		}
		out, err := os.OpenFile(filepath.Join(dir, string(name)), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
		if err != nil {
			return nil, err
		}
		_, err = io.CopyN(io.MultiWriter(out, h, countWriter{&progress.Bytes}), r, int64(size))
		sum := h.Sum(nil)
		if err == nil {
			err = checkSum(r, sum)
		}
		if err = errors.Join(err, out.Sync(), out.Close()); err != nil {
			return nil, fmt.Errorf("%s: %w", name, err)
		}
		progress.Files.Add(1)
		record(File{Name: string(name), Size: int64(size), SHA256: hex.EncodeToString(sum)})
	}
	return list, syncDir(dir)
}

func checkSum(r io.Reader, want []byte) error {
	got := make([]byte, sha256.Size)
	if _, err := io.ReadFull(r, got); err != nil {
		return err
	}
	if string(got) != string(want) {
		return errors.New("sha256 mismatch")
	}
	return nil
}

func checkList(list []File, got map[string]File) error {
	if len(list) != len(got) {
		return fmt.Errorf("sender listed %d files, received %d", len(list), len(got))
	}
	for _, f := range list {
		g, ok := got[f.Name]
		if !ok || g.Size != f.Size || g.SHA256 != f.SHA256 {
			return fmt.Errorf("%s: listed %d %s, received %d %s", f.Name, f.Size, f.SHA256, g.Size, g.SHA256)
		}
	}
	return nil
}

func writeList(dir string, list []File) error {
	data, err := json.MarshalIndent(map[string]any{"written": time.Now().UTC().Format(time.RFC3339), "files": list}, "", " ")
	if err != nil {
		return err
	}
	path := filepath.Join(dir, ListName)
	if err := os.WriteFile(path, data, 0o644); err != nil {
		return err
	}
	return syncDir(dir)
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	return errors.Join(d.Sync(), d.Close())
}

func validName(name string) bool {
	return name != "" && name != "." && name != ".." && !strings.ContainsAny(name, "/\\\x00") && len(name) <= 255
}

type countWriter struct{ n *atomic.Int64 }

func (c countWriter) Write(p []byte) (int, error) {
	c.n.Add(int64(len(p)))
	return len(p), nil
}
