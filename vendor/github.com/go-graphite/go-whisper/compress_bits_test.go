package whisper

import (
	"fmt"
	"math/rand"
	"testing"
)

// The original decoder is the independent oracle for values and cursor state,
// including partial reads, EOF and zero-width reads. Keep it unchanged.
func (br *bitsReader) readBeforeByteReverse(c int) uint64 {
	if c > 64 {
		panic("bitsReader can't read more than 64 bits")
	}

	var data uint64
	oldc := c
	for {
		if br.badRead = br.current >= len(br.buf); br.badRead || c <= 0 {
			// TODO: should reset data?
			// data = 0
			break
		}

		if c < br.bitPos+1 {
			data <<= uint(c)
			data |= (uint64(br.buf[br.current]>>uint(br.bitPos+1-c)) & ((1 << uint(c)) - 1))
			br.bitPos -= c
			break
		}

		data <<= uint(br.bitPos + 1)
		data |= (uint64(br.buf[br.current] & ((1 << uint(br.bitPos+1)) - 1)))
		c -= br.bitPos + 1
		br.current++
		br.bitPos = 7
		continue
	}

	var result uint64
	for i := 8; i <= 64; i += 8 {
		if oldc-i < 0 {
			result |= (data & (1<<uint(oldc%8) - 1)) << uint(i-8)
			break
		}
		result |= ((data >> uint(oldc-i)) & 0xFF) << uint(i-8)
	}
	return result
}

func TestBitsReadByteReverseMatchesOriginal(t *testing.T) {
	random := rand.New(rand.NewSource(2147483647))
	for size := 0; size <= 12; size++ {
		for value := 0; value < 256; value++ {
			buffer := make([]byte, size)
			_, _ = random.Read(buffer)
			if size > 0 {
				buffer[0] = byte(value)
			}
			for position := 0; position < 8; position++ {
				for count := -1; count <= 64; count++ {
					actual := bitsReader{buf: buffer, bitPos: position}
					want := actual
					got, expected := actual.Read(count), want.readBeforeByteReverse(count)
					if got != expected || actual.current != want.current || actual.bitPos != want.bitPos || actual.badRead != want.badRead {
						t.Fatalf("size=%d byte=%d position=%d width=%d: got=%x state=%+v expected=%x state=%+v", size, value, position, count, got, actual, expected, want)
					}
				}
			}
		}
	}
	for trial := 0; trial < 1024; trial++ {
		buffer := make([]byte, 1+random.Intn(1024))
		_, _ = random.Read(buffer)
		actual := bitsReader{buf: buffer, bitPos: 7}
		want := actual
		for i := 0; i < 512; i++ {
			count := random.Intn(65)
			got, expected := actual.Read(count), want.readBeforeByteReverse(count)
			if got != expected || actual.current != want.current || actual.bitPos != want.bitPos || actual.badRead != want.badRead {
				t.Fatalf("trial%d read%d width%d: got=%x expected=%x", trial, i, count, got, expected)
			}
		}
	}
}

var bitsReadBenchmarkSink uint64

func BenchmarkBitsReadByteReverse(b *testing.B) {
	buffer := make([]byte, 4096)
	for i := range buffer {
		buffer[i] = byte(i*137 + 89)
	}
	for _, count := range []int{1, 2, 6, 8, 9, 17, 32, 64} {
		for _, variant := range []string{"before", "after"} {
			b.Run(fmt.Sprintf("width%d/%s", count, variant), func(b *testing.B) {
				reader := bitsReader{buf: buffer, bitPos: 7}
				var total uint64
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					if reader.current > len(buffer)-16 {
						reader.current, reader.bitPos = 0, 7
					}
					if variant == "before" {
						total += reader.readBeforeByteReverse(count)
					} else {
						total += reader.Read(count)
					}
				}
				bitsReadBenchmarkSink = total
			})
		}
	}
}
