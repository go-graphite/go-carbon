//go:build unix

package handoff

import (
	"encoding/binary"
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Exercise every blocking phase after a failed transfer. Even a successor that
// keeps making progress, or repeatedly registers, cannot renew retention.
func TestInputRetentionDeadline(t *testing.T) {
	savedWindow, savedPeer := InputRetentionTimeout, PeerExecutable
	InputRetentionTimeout = 100 * time.Millisecond
	t.Cleanup(func() { InputRetentionTimeout, PeerExecutable = savedWindow, savedPeer })
	unsupported := protocolScript(t, "")
	stalledProbe := filepath.Join(t.TempDir(), "probe")
	if err := os.WriteFile(stalledProbe, []byte("#!/bin/sh\nexec sleep 5\n"), 0700); err != nil {
		t.Fatal(err)
	}
	for _, mode := range []string{"absent", "silent", "input_request", "payload", "descriptor_ack", "commit_ack", "crash_loop", "incompatible", "probe"} {
		t.Run(mode, func(t *testing.T) {
			var attempts atomic.Int32
			PeerExecutable = func(*net.UnixConn) (string, error) {
				if attempts.Add(1) > 1 {
					switch mode {
					case "incompatible":
						return unsupported, nil
					case "probe":
						return stalledProbe, nil
					}
				}
				return "", errors.ErrUnsupported
			}
			listener, err := net.ListenTCP("tcp", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)})
			if err != nil {
				t.Fatal(err)
			}
			defer listener.Close()
			f, err := listener.File()
			if err != nil {
				t.Fatal(err)
			}
			r, w, err := os.Pipe()
			if err != nil {
				t.Fatal(err)
			}
			defer w.Close()
			source := &retainedSource{in: &Input{Receivers: []InputReceiver{{Name: "tcp", Listener: f, Conns: []InputConn{{File: r, Leftover: make([]byte, 1<<20)}}}}}, retained: make(chan struct{}, 1)}
			defer source.in.Close()
			offer, err := NewOffer(socketPath(t))
			if err != nil {
				t.Fatal(err)
			}
			defer offer.Close()
			done := make(chan error, 1)
			go func() { _, err := offer.ServeInput(source, time.Second); done <- err }()
			// Interrupt the first payload transfer before any FD ownership.
			c := Register(offer.path)
			if c == nil {
				t.Fatal("register")
			}
			if _, _, err = c.Take(&net.TCPAddr{}); err != nil {
				t.Fatal(err)
			}
			if _, err = c.conn.Write([]byte{msgInputRequest}); err != nil {
				t.Fatal(err)
			}
			var header [5]byte
			if _, err = io.ReadFull(c.conn, header[:]); err != nil {
				t.Fatal(err)
			}
			_ = c.Close()
			select {
			case <-source.retained:
			case <-time.After(3 * time.Second):
				t.Fatal("not retained")
			}
			started := time.Now()
			clientDone := make(chan struct{})
			offerDone := make(chan struct{})
			releaseClient := sync.OnceFunc(func() { close(offerDone) })
			defer releaseClient()
			go func() {
				defer close(clientDone)
				if mode == "absent" {
					return
				}
				for {
					c := Register(offer.path)
					if c == nil {
						return
					}
					_ = c.conn.SetDeadline(time.Now().Add(time.Second))
					if mode == "incompatible" {
						_, _ = c.conn.Read(header[:1])
					}
					if mode == "crash_loop" || mode == "incompatible" {
						_ = c.Close()
						continue
					}
					defer c.Close()
					if mode == "silent" || mode == "probe" {
						_, _ = c.conn.Read(header[:1]) // peer closes at retention deadline
						return
					}
					if _, _, err := c.Take(&net.TCPAddr{}); err != nil || mode == "input_request" {
						_, _ = c.conn.Read(header[:1])
						return
					}
					_, _ = c.conn.Write([]byte{msgInputRequest})
					if _, err := io.ReadFull(c.conn, header[:]); err != nil {
						return
					}
					if mode == "payload" {
						// Read no payload: a blocked write must expire too.
						<-offerDone
						return
					}
					_, _ = io.CopyN(io.Discard, c.conn, int64(binary.BigEndian.Uint32(header[1:]))+(1<<20))
					if mode == "descriptor_ack" {
						// Leave descriptors unread and send no acknowledgement.
						<-offerDone
						return
					}
					files, _ := receiveFiles(c.conn, 2)
					for _, f := range files {
						_ = f.Close()
					}
					_, _ = c.conn.Read(header[:1])
					return
				}
			}()
			select {
			case err = <-done:
			case <-time.After(2 * time.Second):
				t.Fatal("retention did not bound offer")
			}
			if !errors.Is(err, ErrInputRetentionExpired) {
				t.Fatalf("got %v", err)
			}
			if elapsed := time.Since(started); elapsed < 60*time.Millisecond || elapsed > time.Second {
				t.Fatalf("retention elapsed %v", elapsed)
			}
			releaseClient()
			<-clientDone
			if _, err := source.in.Receivers[0].Conns[0].File.Stat(); err != nil {
				t.Fatal("offer closed original before its owner could account for it", err)
			}
		})
	}
}
