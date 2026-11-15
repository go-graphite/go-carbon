package buckyd

import (
	"sync"
	"testing"
)

func TestTransferSourceOrigin(t *testing.T) {
	s := &Service{nodes: []Node{{Server: "ring.example", Port: 2003}}}
	s.SetTransferSources([]string{"source.example", "fixed.example:8080", "[::1]:8081"})
	for _, tc := range []struct{ input, want string }{
		{"http://RING.EXAMPLE:8080/", "http://ring.example:8080"},
		{"https://source.example", "https://source.example:443"},
		{"http://source.example", "http://source.example:80"},
		{"http://fixed.example:8080", "http://fixed.example:8080"},
		{"http://[::1]:8081", "http://[::1]:8081"},
	} {
		t.Run(tc.input, func(t *testing.T) {
			got, err := s.transferSourceOrigin(tc.input)
			if err != nil || got.String() != tc.want {
				t.Fatalf("origin = %v, %v; want %s", got, err, tc.want)
			}
		})
	}
	for _, input := range []string{
		"http://unlisted.example", "file://source.example", "http://user@source.example",
		"http://source.example/path", "http://source.example?token=x", "http://source.example?",
		"http://source.example#fragment", "http://source.example:0", "http://source.example:65536",
		"http://source.example:invalid", "http://fixed.example:8081", "http://[::1]:8080",
	} {
		t.Run(input, func(t *testing.T) {
			if got, err := s.transferSourceOrigin(input); err == nil {
				t.Fatalf("accepted destination %v", got)
			}
		})
	}
}

func TestTransferSourcesReload(t *testing.T) {
	s := &Service{nodes: []Node{{Server: "ring.example", Port: 2003}}}
	sources := []string{"old.example"}
	s.SetTransferSources(sources)
	sources[0] = "untrusted.example"
	if _, err := s.transferSourceOrigin("http://old.example"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.transferSourceOrigin("http://untrusted.example"); err == nil {
		t.Fatal("caller mutated published policy")
	}
	s.SetTransferSources([]string{"new.example"})
	if _, err := s.transferSourceOrigin("http://old.example"); err == nil {
		t.Fatal("removed source remains authorized")
	}
	if _, err := s.transferSourceOrigin("http://new.example"); err != nil {
		t.Fatal(err)
	}
	s.SetTransferSources(nil)
	if _, err := s.transferSourceOrigin("http://new.example"); err == nil {
		t.Fatal("cleared source remains authorized")
	}
	if _, err := s.transferSourceOrigin("http://ring.example"); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 1000; j++ {
				s.SetTransferSources([]string{"next.example"})
				_, _ = s.transferSourceOrigin("http://next.example")
			}
		}()
	}
	wg.Wait()
}
