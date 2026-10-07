package recovery

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/go-graphite/go-carbon/points"
)

func validateRecordsSerial(in *Index) error {
	for file, count := range in.recordCounts {
		end := uint64(0)
		for id := uint64(1); id <= count; id++ {
			r := in.record(file, id)
			if r.offset != end || r.length == 0 || r.length > uint64(len(in.source[file]))-end || r.previous >= id {
				return fmt.Errorf("invalid recovery record chain")
			}
			name, err := recordMetric(in.source[file][r.offset : r.offset+r.length])
			if err != nil {
				return err
			}
			// Cache keys must have the same spelling as their filesystem metric.
			// Legacy aliases such as a..b or a/b require ordered persistence before
			// reads; looking them up as a.b in the pending source would miss data.
			if len(name) == 0 || name[0] == '.' || name[len(name)-1] == '.' || bytes.Contains(name, []byte("..")) || bytes.IndexByte(name, '/') >= 0 || bytes.IndexByte(name, 0) >= 0 {
				return fmt.Errorf("pending reads require canonical metric names")
			}
			end += r.length
		}
		if end != uint64(len(in.source[file])) {
			return fmt.Errorf("unindexed recovery source bytes")
		}
	}
	return nil
}

func TestParallelRecordValidationMatchesSerial(t *testing.T) {
	const boundary = 256 * 1024
	b := NewBuilder(nil)
	var source []byte
	batch := points.OnePoint("shared.metric", 1, 123)
	raw := batch.AppendBinary(nil)
	for i := 0; i < boundary+17; i++ {
		source = append(source, raw...)
		if err := b.Add(0, batch, len(raw)); err != nil {
			t.Fatal(err)
		}
	}
	var output bytes.Buffer
	if err := b.Write(&output); err != nil {
		t.Fatal(err)
	}
	data := output.Bytes()
	initial, err := Open(data, source, nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, scenario := range []string{"valid", "previous offset", "previous length", "boundary gap", "zero length", "forward chain", "alias before boundary", "alias after boundary", "trailing bytes", "empty table"} {
		t.Run(scenario, func(t *testing.T) {
			in := *initial
			in.data = append([]byte(nil), data...)
			in.source[0] = append([]byte(nil), source...)
			put := func(id, field, value uint64) {
				binary.LittleEndian.PutUint64(in.data[in.recordOffsets[0]+(id-1)*recordSize+field:], value)
			}
			switch scenario {
			case "previous offset":
				put(boundary, 0, ^uint64(0))
			case "previous length":
				put(boundary, 8, ^uint64(0))
			case "boundary gap":
				put(boundary+1, 0, in.record(0, boundary+1).offset+1)
			case "zero length":
				put(boundary+1, 8, 0)
			case "forward chain":
				put(boundary+1, 16, boundary+1)
			case "alias before boundary", "alias after boundary":
				id := uint64(boundary)
				if scenario == "alias after boundary" {
					id++
				}
				r := in.record(0, id)
				name, e := recordMetric(in.source[0][r.offset : r.offset+r.length])
				if e != nil {
					t.Fatal(e)
				}
				name[2] = '/'
			case "trailing bytes":
				in.source[0] = append(in.source[0], 0)
			case "empty table":
				in.recordCounts[0] = 0
			}
			fast, slow := in.validateRecords(), validateRecordsSerial(&in)
			if (fast == nil) != (slow == nil) {
				t.Fatalf("parallel %v != serial %v", fast, slow)
			}
			if scenario != "valid" && fast == nil {
				t.Fatal("malformed range accepted")
			}
		})
	}
}
