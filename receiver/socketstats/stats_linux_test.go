//go:build linux

package socketstats

import (
	"encoding/binary"
	"encoding/hex"
	"net"
	"os"
	"strings"
	"testing"
	"time"
)

func TestCounterFixtures(t *testing.T) {
	got := map[string]uint64{}
	for file, protocol := range map[string]string{"netstat": "TcpExt:", "snmp": "Tcp:"} {
		f, err := os.Open("testdata/" + file)
		if err != nil {
			t.Fatal(err)
		}
		err = readCounters(f, protocol, got)
		f.Close()
		if err != nil {
			t.Fatal(err)
		}
	}
	if len(got) != 12 || got["ListenOverflows"] != 42 || got["ListenDrops"] != 41 || got["TCPRcvQDrop"] != 6 || got["InErrs"] != 2 {
		t.Fatal(got)
	}
	for _, input := range []string{"", "TcpExt: ListenDrops\n", "TcpExt: ListenDrops\nTcpExt: x\n", "TcpExt: ListenDrops\nTcp: 1\n", "TcpExt: ListenDrops A\nTcpExt: 1\n"} {
		if err := readCounters(strings.NewReader(input), "TcpExt:", map[string]uint64{}); err == nil {
			t.Fatalf("accepted %q", input)
		}
	}
	// Older kernels omit fields; absence must not be presented as zero.
	got = map[string]uint64{}
	if err := readCounters(strings.NewReader("TcpExt: ListenDrops\nTcpExt: 9\n"), "TcpExt:", got); err != nil || len(got) != 1 {
		t.Fatal(got, err)
	}
}

func TestSocketFixtures(t *testing.T) {
	// inet_diag_msg fixtures use the kernel's native endianness.
	raw, err := os.ReadFile("testdata/diag.hex")
	if err != nil {
		t.Fatal(err)
	}
	data, err := hex.DecodeString(strings.TrimSpace(string(raw)))
	if err != nil {
		t.Fatal(err)
	}
	if binary.NativeEndian.Uint32([]byte{1, 0, 0, 0}) != 1 {
		t.Skip("little-endian capture")
	}
	addr := &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 2003}
	var q queues
	if err = addSocket(&q, data, addr); err != nil {
		t.Fatal(err)
	}
	if q.length != 3 || q.limit != 2048 || q.bytes != 0 || q.connections != 0 {
		t.Fatal(q)
	}
	data[1] = 1
	binary.NativeEndian.PutUint32(data[56:], 1234)
	if err = addSocket(&q, data, addr); err != nil {
		t.Fatal(err)
	}
	if q.bytes != 1234 || q.connections != 1 {
		t.Fatal(q)
	}
	// An unrelated address or port, TIME_WAIT and SYN_RECV do not contribute.
	for _, state := range []byte{6, 3} {
		data[1] = state
		_ = addSocket(&q, data, addr)
	}
	data[1] = 1
	_ = addSocket(&q, data, &net.TCPAddr{IP: addr.IP, Port: 2004})
	_ = addSocket(&q, data, &net.TCPAddr{IP: net.ParseIP("127.0.0.2"), Port: 2003})
	if q.bytes != 1234 || q.connections != 1 {
		t.Fatal(q)
	}
	data[0] = 10
	copy(data[8:24], net.ParseIP("::1").To16())
	_ = addSocket(&q, data, &net.TCPAddr{IP: net.ParseIP("::1"), Port: 2003})
	if q.bytes != 2468 || q.connections != 2 {
		t.Fatal(q)
	}
	if err = addSocket(&q, data[:71], addr); err == nil {
		t.Fatal("accepted truncated message")
	}
}

func TestLiveQueues(t *testing.T) {
	for _, network := range []string{"tcp4", "tcp6"} {
		t.Run(network, func(t *testing.T) {
			ip := "127.0.0.1:0"
			if network == "tcp6" {
				ip = "[::1]:0"
			}
			l, err := net.Listen(network, ip)
			if err != nil {
				t.Skip(err)
			}
			defer l.Close()
			addr := l.Addr().(*net.TCPAddr)
			client, err := net.Dial(network, l.Addr().String())
			if err != nil {
				t.Fatal(err)
			}
			defer client.Close()
			q, err := readQueues(addr)
			if err != nil {
				t.Fatal(err)
			}
			if q.limit == 0 || q.length != 1 {
				t.Fatalf("pending accept: %+v", q)
			}
			server, err := l.Accept()
			if err != nil {
				t.Fatal(err)
			}
			defer server.Close()
			if _, err = client.Write(make([]byte, 1000)); err != nil {
				t.Fatal(err)
			}
			deadline := time.Now().Add(time.Second)
			for {
				q, err = readQueues(addr)
				if err != nil {
					t.Fatal(err)
				}
				if q.bytes == 1000 && q.connections == 1 && q.length == 0 {
					break
				}
				if time.Now().After(deadline) {
					t.Fatalf("unread data: %+v", q)
				}
				time.Sleep(time.Millisecond)
			}
			other := sample(map[string]*net.TCPAddr{"tcp": addr})
			if !other.hostOK || !other.socketsOK || other.listeners["tcp"].connections != 1 {
				t.Fatalf("sample: %+v", other)
			}
		})
	}
}
func BenchmarkSample(b *testing.B) {
	l, err := net.Listen("tcp", "[::]:0")
	if err != nil {
		b.Fatal(err)
	}
	defer l.Close()

	for i := 0; i < 280; i++ {
		client, err := net.Dial("tcp", l.Addr().String())
		if err != nil {
			b.Fatal(err)
		}
		server, err := l.Accept()
		if err != nil {
			client.Close()
			b.Fatal(err)
		}
		defer client.Close()
		defer server.Close()
	}
	listeners := map[string]*net.TCPAddr{"tcp": l.Addr().(*net.TCPAddr)}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s := sample(listeners)
		if !s.hostOK || !s.socketsOK {
			b.Fatal(s)
		}
	}
}

func TestSamplerLifecycle(t *testing.T) {
	c := New(map[string]*net.TCPAddr{})
	if c == nil {
		t.Fatal("Linux sampler missing")
	}
	defer c.Stop()
	deadline := time.Now().Add(time.Second)
	for {
		c.mu.RLock()
		s := c.current
		c.mu.RUnlock()
		if s.hostOK && s.socketsOK {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("initial sample missing")
		}
		time.Sleep(time.Millisecond)
	}
	c.Stop() // Stopping twice is safe.
}
