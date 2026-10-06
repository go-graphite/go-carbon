package points

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"strings"
)

const MB = 1048576

func ReadPlain(r io.Reader, callback func(*Points)) error {
	reader := bufio.NewReaderSize(r, MB)

	for {
		line, err := reader.ReadSlice('\n')
		if errors.Is(err, bufio.ErrBufferFull) {
			// Preserve support for lines larger than the reader buffer without
			// allocating a second byte slice for every ordinary WAL record.
			prefix := append([]byte(nil), line...)
			var rest []byte
			rest, err = reader.ReadBytes('\n')
			prefix = append(prefix, rest...)
			line = prefix
		}

		if err != nil && !errors.Is(err, io.EOF) {
			return err
		}

		if len(line) == 0 {
			break
		}

		if line[len(line)-1] != '\n' {
			return errors.New("unfinished line in file")
		}

		p, err := ParseText(string(line))

		if err == nil {
			callback(p)
		}
	}

	return nil
}

func ReadBinary(r io.Reader, callback func(*Points)) error {
	reader := bufio.NewReaderSize(r, MB)
	var p *Points
	buf := make([]byte, MB)

	flush := func() {
		if p != nil {
			callback(p)
			p = nil
		}
	}

	defer flush()

	for {
		flush()

		l, err := binary.ReadVarint(reader)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return err
		}
		if l < 0 || l > MB {
			return fmt.Errorf("invalid metric name length: %d", l)
		}

		if _, err := io.ReadFull(reader, buf[:l]); err != nil {
			return err
		}

		cnt, err := binary.ReadVarint(reader)
		if err != nil {
			return err
		}
		if cnt < 0 {
			return fmt.Errorf("invalid point count: %d", cnt)
		}

		var v, t, v0, t0 int64

		for i := int64(0); i < cnt; i++ {
			v0, err = binary.ReadVarint(reader)
			if err != nil {
				return err
			}
			v += v0

			t0, err = binary.ReadVarint(reader)
			if err != nil {
				return err
			}
			t += t0

			if i == int64(0) {
				// Bound eager allocation for damaged input; large valid batches can
				// still grow. Keep partial-record recovery of complete points.
				p = &Points{Metric: string(buf[:l]), Data: make([]Point, 0, min(cnt, 16384))}
				p.Add(math.Float64frombits(uint64(v)), t)
			} else {
				p.Add(math.Float64frombits(uint64(v)), t)
			}
		}
	}

	return nil
}

func ReadFromFile(filename string, callback func(*Points)) error {
	file, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	if strings.HasSuffix(strings.ToLower(filename), ".bin") {
		return ReadBinary(file, callback)
	}

	return ReadPlain(file, callback)
}
