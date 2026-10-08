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
	l := NewCarbonserverListener(nil)
	ln, err := net.ListenTCP("tcp", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	l.tcpListener = ln
	l.httpServer = &http.Server{
		Handler:   http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, "ok") }),
		ConnState: l.trackConnState,
	}
	l.serverWG.Add(1)
	go func() { defer l.serverWG.Done(); _ = l.httpServer.Serve(ln) }()

	conn, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	deadline := time.Now().Add(5 * time.Second)
	for accepted := false; !accepted; {
		l.connStates.Range(func(_, _ any) bool { accepted = true; return false })
		if time.Now().After(deadline) {
			t.Fatal("connection not accepted")
		}
		time.Sleep(time.Millisecond)
	}

	l.StopAccepting()
	stopped := make(chan struct{})
	go func() { defer close(stopped); _ = l.Stop() }()
	time.Sleep(50 * time.Millisecond)
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
