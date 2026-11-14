package main

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"io"
	"math"
	"testing"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
)

func digestFixture(t *testing.T, points []store.Point) []byte {
	t.Helper()
	var block store.PointBlock
	for i, p := range points {
		block.Present[i/64] |= 1 << uint(i%64)
		block.Points[i] = p
	}
	r := store.TransferMetric{Config: store.MetricConfig{Name: "x", Retentions: []store.Retention{{Step: 1, Count: 10}}}, Version: store.TransferVersion{Name: "x", ID: 1}, Keys: []store.RelayoutKV{{Key: make([]byte, 6), Value: store.EncodePointBlock(&block)}}}
	var wire bytes.Buffer
	if err := store.WriteTransfer(&wire, []store.TransferMetric{r}); err != nil {
		t.Fatal(err)
	}
	var frame bytes.Buffer
	if err := binary.Write(&frame, binary.BigEndian, uint64(wire.Len())); err != nil {
		t.Fatal(err)
	}
	frame.Write(wire.Bytes())
	return frame.Bytes()
}

func TestDigestFramesSortFiniteAndWindow(t *testing.T) {
	points := []store.Point{{Timestamp: 86401, Value: 2}, {Timestamp: 86399, Value: math.Copysign(0, -1)}, {Timestamp: 86400, Value: math.NaN()}, {Timestamp: 86402, Value: math.Inf(1)}, {Timestamp: 86403, Value: 4}}
	first := digestFixture(t, points)
	points[0], points[1] = points[1], points[0]
	second := digestFixture(t, points)
	var out bytes.Buffer
	if err := digestFrames(bytes.NewReader(append(first, second...)), &out, 86399, 86403); err != nil {
		t.Fatal(err)
	}
	decoder := json.NewDecoder(&out)
	var a, b map[string]struct {
		Buckets [][]any `json:"buckets"`
	}
	if err := decoder.Decode(&a); err != nil {
		t.Fatal(err)
	}
	if err := decoder.Decode(&b); err != nil {
		t.Fatal(err)
	}
	aj, _ := json.Marshal(a)
	bj, _ := json.Marshal(b)
	if !bytes.Equal(aj, bj) || len(a["x"].Buckets) != 2 || a["x"].Buckets[0][2] != float64(1) || a["x"].Buckets[1][2] != float64(1) {
		t.Fatal(string(aj), string(bj))
	}
	if err := decoder.Decode(&a); err != io.EOF {
		t.Fatal(err)
	}
	if a["x"].Buckets[0][3] != "c69902a4bd778d7a" {
		t.Fatal("canonical timestamp/IEEE byte encoding changed", a)
	}
	points[0].Value = 0
	out.Reset()
	if err := digestFrames(bytes.NewReader(digestFixture(t, points)), &out, 86399, 86403); err != nil {
		t.Fatal(err)
	}
	if err := json.NewDecoder(&out).Decode(&b); err != nil {
		t.Fatal(err)
	}
	if a["x"].Buckets[0][3] == b["x"].Buckets[0][3] {
		t.Fatal("negative zero bits lost")
	}
}

func TestDigestRejectsIncompleteCorruptAndOversizedFrames(t *testing.T) {
	frame := digestFixture(t, []store.Point{{Timestamp: 1, Value: 1}})
	for _, raw := range [][]byte{frame[:7], frame[:8], frame[:len(frame)-1], append([]byte{0, 0, 0, 0, 4, 0, 0, 1}, frame...)} {
		if err := digestFrames(bytes.NewReader(raw), io.Discard, 0, 2); err == nil {
			t.Fatal("bad frame accepted")
		}
	}
	frame[len(frame)-1] ^= 1
	var out bytes.Buffer
	if err := digestFrames(bytes.NewReader(frame), &out, 0, 2); err == nil || out.Len() != 0 {
		t.Fatal("corrupt checksum published")
	}
}

func TestPointFramesMatchExistingDecoder(t *testing.T) {
	frame := digestFixture(t, []store.Point{{Timestamp: 3, Value: math.Copysign(0, -1)}, {Timestamp: 2, Value: math.NaN()}, {Timestamp: 1, Value: 7}})
	want, err := decode(bytes.NewReader(frame[8:]), nil)
	if err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	if err = pointFrames(bytes.NewReader(append(frame, frame...)), &out); err != nil {
		t.Fatal(err)
	}
	d := json.NewDecoder(&out)
	for range 2 {
		var got map[string]struct {
			Archives []struct{ Points map[string][]string }
		}
		if err = d.Decode(&got); err != nil {
			t.Fatal(err)
		}
		for ai, a := range want["x"].Archives {
			for timestamp, bits := range a.Points {
				values := got["x"].Archives[ai].Points[timestamp]
				if len(values) != 1 || values[0] != bits {
					t.Fatal(timestamp, values, bits)
				}
			}
		}
	}
}

func TestDigestRetainsConflictingValuesDeterministically(t *testing.T) {
	points := []store.Point{{Timestamp: 1, Value: 2}, {Timestamp: 1, Value: 1}, {Timestamp: 1, Value: 1}}
	var a, b bytes.Buffer
	if err := digestFrames(bytes.NewReader(digestFixture(t, points)), &a, 0, 2); err != nil {
		t.Fatal(err)
	}
	points[0], points[1] = points[1], points[0]
	if err := digestFrames(bytes.NewReader(digestFixture(t, points)), &b, 0, 2); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(a.Bytes(), b.Bytes()) {
		t.Fatal(a.String(), b.String())
	}
	var out map[string]struct {
		Buckets   [][]any
		Ambiguous [][3]int64
	}
	if err := json.Unmarshal(a.Bytes(), &out); err != nil {
		t.Fatal(err)
	}
	if out["x"].Buckets[0][2] != float64(2) || len(out["x"].Ambiguous) != 1 || out["x"].Ambiguous[0] != [3]int64{0, 1, 2} {
		t.Fatal(out)
	}
}
