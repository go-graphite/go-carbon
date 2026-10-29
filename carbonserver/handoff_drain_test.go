package carbonserver

import (
	"bufio"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"
)

// After a socket handoff, a connection the old instance accepted just before
// StopAccepting may not have sent its request yet. Stop must still answer it:
// http.Server drops requests it reads after Shutdown began.
func TestStopAnswersConnectionAcceptedBeforeStopAccepting(t *testing.T) {
	t.Parallel()
	l := NewCarbonserverListener(nil)
	ln, err := net.ListenTCP("tcp", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	l.tcpListener = ln
	accepted := make(chan struct{})
	l.httpServer = &http.Server{
		Handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, "ok") }),
		ConnState: func(conn net.Conn, state http.ConnState) {
			l.trackConnState(conn, state)
			if state == http.StateNew {
				close(accepted)
			}
		},
	}
	t.Cleanup(func() { _ = l.httpServer.Close(); l.serverWG.Wait() })
	l.serverWG.Add(1)
	go func() { defer l.serverWG.Done(); _ = l.httpServer.Serve(ln) }()

	conn, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	waitListenerEvent(t, accepted)

	l.StopAccepting()
	stopped := make(chan struct{})
	go func() { defer close(stopped); _ = l.Stop() }()
	waitListenerEvent(t, l.exitChan)
	if _, err = io.WriteString(conn, "GET / HTTP/1.1\r\nHost: x\r\n\r\n"); err != nil {
		t.Fatal(err)
	}
	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	resp, err := http.ReadResponse(bufio.NewReader(conn), nil)
	if err != nil {
		t.Fatal("request dropped during stop:", err)
	}
	body, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK || strings.TrimSpace(string(body)) != "ok" || !resp.Close {
		t.Fatal("unexpected response", resp.StatusCode, string(body), resp.Close)
	}
	select {
	case <-stopped:
	case <-time.After(10 * time.Second):
		t.Fatal("stop did not finish")
	}
}
