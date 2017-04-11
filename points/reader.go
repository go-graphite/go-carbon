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
		line, err := reader.ReadBytes('\n')

		if err != nil && err != io.EOF {
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
		if l > MB {
			return fmt.Errorf("metric name too long: %d", l)
		}

		if err == io.EOF {
			break
		}

		if err != nil {
			return err
		}

		io.ReadAtLeast(reader, buf[:l], int(l))

		cnt, err := binary.ReadVarint(reader)

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
				p = OnePoint(string(buf[:l]), math.Float64frombits(uint64(v)), t)
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
