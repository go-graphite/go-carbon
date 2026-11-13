package chunkstore

import "testing"

func TestTransferMergeFallbackCauses(t *testing.T) {
	var s transferMergeStats
	s.fallback(TransferMergeFallbackClassic+"/sidecar", "a.b")
	s.fallback(TransferMergeFallbackClassic+"/sidecar", "a.c")
	s.fallback(TransferMergeFallbackNaN, "a.d")
	s.fallback("unknown/x", "a.e")
	_, _, fallbacks := s.snapshot()
	if fallbacks[TransferMergeFallbackClassic] != 2 || fallbacks[TransferMergeFallbackNaN] != 1 || fallbacks[TransferMergeFallbackOther] != 1 {
		t.Fatalf("fallbacks %v", fallbacks)
	}
	causes := s.causeSnapshot()
	if c := causes["classic/sidecar"]; c.Count != 2 || c.Sample != "a.c" {
		t.Fatalf("classic/sidecar %+v", c)
	}
	if c := causes["nan"]; c.Count != 1 || c.Sample != "a.d" {
		t.Fatalf("nan %+v", c)
	}
	if len(causes) != 3 {
		t.Fatalf("causes %v", causes)
	}
}
