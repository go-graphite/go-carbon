package tcp

import (
	"bytes"
	"encoding/json"
	"io"
	"net"
	"testing"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// A socket restored by net.FileConn can lack a peer address after its peer
// disconnects. Keep the connection non-nil to exercise the address lookup.
type streamFailureConn struct {
	net.Conn
	peer net.Addr
}

func (c streamFailureConn) RemoteAddr() net.Addr { return c.peer }

func TestStreamFailurePeerAddress(t *testing.T) {
	for _, tc := range []struct {
		name string
		peer net.Addr
		want string
	}{
		{name: "unavailable"},
		{name: "connected", peer: &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 2003}, want: "127.0.0.1:2003"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var logs bytes.Buffer
			core := zapcore.NewCore(zapcore.NewJSONEncoder(zap.NewProductionEncoderConfig()), zapcore.AddSync(&logs), zap.ErrorLevel)
			rcv := &TCP{logger: zap.New(core)}
			rcv.streamFailure(streamFailureConn{peer: tc.peer}, io.ErrUnexpectedEOF)
			if got := rcv.streamCounters[streamDecodeErrors].Load(); got != 1 {
				t.Fatalf("decode errors = %d, want 1", got)
			}
			if rcv.errors != 1 {
				t.Fatalf("errors = %d, want 1", rcv.errors)
			}
			var fields map[string]any
			if err := json.Unmarshal(logs.Bytes(), &fields); err != nil {
				t.Fatal(err)
			}
			if fields["msg"] != "stream decode failed" || fields["peer"] != tc.want || fields["error"] != io.ErrUnexpectedEOF.Error() {
				t.Fatalf("unexpected error fields: %v", fields)
			}
		})
	}
}
