// Package v2wire defines authenticated, bounded placement delivery frames.
// The same source and golden fixture are shared by relay and store.
package v2wire

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"unicode/utf8"
)

const MaxFrame = 64 << 10
const Data = "data"
const Ack = "ack"

// Stream is the durable sequence scope. A new epoch starts at position one.
// Producer must include a persistent incarnation; credentials are scoped to it.
type Stream struct {
	Group     string `json:"group"`
	Replica   string `json:"replica"`
	Store     string `json:"store"`
	MapDigest string `json:"map_digest"`
	Revision  uint64 `json:"revision"`
	Epoch     uint64 `json:"epoch"`
	Producer  string `json:"producer"`
	Lane      string `json:"lane"`
}
type Frame struct {
	Kind      string `json:"kind"`
	Stream    Stream `json:"stream"`
	Sequence  uint64 `json:"sequence"`
	VShard    uint32 `json:"vshard"`
	Name      string `json:"name"`
	Bits      uint64 `json:"bits"`
	Timestamp int64  `json:"timestamp"`
}

func (f Frame) Validate() error {
	s := f.Stream
	for _, value := range []string{s.Group, s.Replica, s.Store, s.MapDigest, s.Producer, s.Lane, f.Name} {
		if !utf8.ValidString(value) {
			return errors.New("placement text must be valid UTF-8")
		}
	}
	if (f.Kind != Data && f.Kind != Ack) || s.Group == "" || s.Replica == "" || s.Store == "" || len(s.MapDigest) != 64 || s.Revision == 0 || s.Epoch == 0 || s.Producer == "" || s.Lane == "" || f.Sequence == 0 || f.VShard >= 4096 || f.Name == "" {
		return errors.New("invalid placement frame")
	}
	if _, err := hex.DecodeString(s.MapDigest); err != nil {
		return err
	}
	return nil
}

// Digest binds retries to the complete original payload, including IEEE bits.
func (f Frame) Digest() string {
	f.Kind = Data
	b, _ := json.Marshal(f)
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}
func (f Frame) Acknowledgement() Frame { f.Kind = Ack; return f }
func Write(w io.Writer, f Frame, key []byte) error {
	if len(key) < 32 {
		return errors.New("placement key must contain at least 32 bytes")
	}
	if err := f.Validate(); err != nil {
		return err
	}
	b, err := json.Marshal(f)
	if err != nil {
		return err
	}
	if len(b) > MaxFrame {
		return errors.New("placement frame limit")
	}
	packet := make([]byte, 8, 8+len(b)+sha256.Size)
	copy(packet, "V2A1")
	binary.BigEndian.PutUint32(packet[4:], uint32(len(b)))
	packet = append(packet, b...)
	mac := hmac.New(sha256.New, key)
	mac.Write(packet)
	packet = mac.Sum(packet)
	for len(packet) > 0 {
		n, e := w.Write(packet)
		if e != nil {
			return e
		}
		if n == 0 {
			return io.ErrShortWrite
		}
		packet = packet[n:]
	}
	return nil
}

// Read verifies with one configured peer key. ReadKeys supports several
// authenticated producer incarnations on the same store endpoint.
func Read(r io.Reader, key []byte) (Frame, error) { return read(r, func(string) []byte { return key }) }
func ReadKeys(r io.Reader, keys map[string][]byte) (Frame, error) {
	return read(r, func(producer string) []byte { return keys[producer] })
}
func read(r io.Reader, keyFor func(string) []byte) (Frame, error) {
	var f Frame
	var head [8]byte
	if _, e := io.ReadFull(r, head[:]); e != nil {
		return f, e
	}
	n := binary.BigEndian.Uint32(head[4:])
	if string(head[:4]) != "V2A1" || n == 0 || n > MaxFrame {
		return f, errors.New("placement header or frame limit")
	}
	b := make([]byte, int(n)+sha256.Size)
	if _, e := io.ReadFull(r, b); e != nil {
		return f, e
	}
	// The unauthenticated identity only selects a key. No frame reaches an
	// admission callback before its entire header and payload MAC verifies.
	d := json.NewDecoder(bytes.NewReader(b[:n]))
	d.DisallowUnknownFields()
	if e := d.Decode(&f); e != nil {
		return f, e
	}
	canonical, _ := json.Marshal(f)
	if !bytes.Equal(canonical, b[:n]) {
		return f, errors.New("noncanonical placement frame")
	}
	key := keyFor(f.Stream.Producer)
	if len(key) < 32 {
		return f, errors.New("unknown placement producer or short key")
	}
	mac := hmac.New(sha256.New, key)
	mac.Write(head[:])
	mac.Write(b[:n])
	if !hmac.Equal(mac.Sum(nil), b[n:]) {
		return f, errors.New("placement authentication failed")
	}
	return f, f.Validate()
}
