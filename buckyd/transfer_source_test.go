package buckyd

import "testing"

func TestTransferSourceOrigin(t *testing.T) {
	s := &Service{nodes: []Node{{Server: "ring.example", Port: 2003}}}
	for _, tc := range []struct{ input, want string }{
		{"http://RING.EXAMPLE:8080/", "http://RING.EXAMPLE:8080"},
		{"https://source.example", "https://source.example:443"},
		{"http://unlisted.example", "http://unlisted.example:80"},
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
		"file://source.example", "http://user@source.example",
		"http://source.example/path", "http://source.example?token=x", "http://source.example?",
		"http://source.example#fragment", "http://source.example:0", "http://source.example:65536",
		"http://source.example:invalid",
	} {
		t.Run(input, func(t *testing.T) {
			if got, err := s.transferSourceOrigin(input); err == nil {
				t.Fatalf("accepted destination %v", got)
			}
		})
	}
}
