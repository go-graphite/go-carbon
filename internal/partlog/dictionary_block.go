package partlog

import (
	"bytes"
	"encoding/binary"
	"errors"

	"github.com/klauspost/compress/zstd"
)

var dictionaryBlockMagic = []byte{'P', 'D', 'C', 2}

// DictionaryBlockEncoder owns one ordered stream. Each compressed block is
// independently checksummed; the dictionary epoch still spans blocks. Close
// releases compressor memory. As with DictionaryEncoder, append failure is fatal.
type DictionaryBlockEncoder struct {
	dictionary DictionaryEncoder
	encoder    *zstd.Encoder
}

func (e *DictionaryBlockEncoder) Encode(fs []DictionaryFrame) ([]byte, error) {
	raw, err := e.dictionary.Encode(fs)
	if err != nil {
		return nil, err
	}
	if e.encoder == nil {
		e.encoder, err = zstd.NewWriter(nil, zstd.WithEncoderConcurrency(1), zstd.WithEncoderLevel(zstd.SpeedFastest), zstd.WithWindowSize(64<<10), zstd.WithEncoderCRC(true))
		if err != nil {
			return nil, err
		}
	}
	header := append([]byte(nil), dictionaryBlockMagic...)
	header = binary.AppendUvarint(header, uint64(len(raw)))
	b := e.encoder.EncodeAll(raw, header)
	if len(b) >= len(raw) {
		return raw, nil
	}
	return b, nil
}

func (e *DictionaryBlockEncoder) Close() {
	if e.encoder != nil {
		_ = e.encoder.Close()
		e.encoder = nil
	}
}

// DictionaryBlockDecoder accepts both uncompressed PDC1 and compressed PDC2.
// Limits are checked before allocation and enforced again by the decoder.
type DictionaryBlockDecoder struct {
	dictionary DictionaryDecoder
	decoder    *zstd.Decoder
}

func IsDictionaryBlockRecord(b []byte) bool {
	return IsDictionaryRecord(b) || bytes.HasPrefix(b, dictionaryBlockMagic)
}

func (d *DictionaryBlockDecoder) Decode(b []byte) ([]DictionaryFrame, error) {
	if IsDictionaryRecord(b) {
		return d.dictionary.Decode(b)
	}
	if !bytes.HasPrefix(b, dictionaryBlockMagic) || len(b) > maxPayload {
		return nil, errDictionary
	}
	n, k := binary.Uvarint(b[len(dictionaryBlockMagic):])
	if k <= 0 || n == 0 || n > maxPayload {
		return nil, errDictionary
	}
	var err error
	if d.decoder == nil {
		d.decoder, err = zstd.NewReader(nil, zstd.WithDecoderConcurrency(1), zstd.WithDecoderMaxMemory(maxPayload), zstd.WithDecoderMaxWindow(64<<10), zstd.WithDecodeAllCapLimit(true))
		if err != nil {
			return nil, err
		}
	}
	raw, err := d.decoder.DecodeAll(b[len(dictionaryBlockMagic)+k:], make([]byte, 0, int(n)))
	if err != nil {
		return nil, err
	}
	if len(raw) != int(n) {
		return nil, errors.New("journal dictionary block length mismatch")
	}
	return d.dictionary.Decode(raw)
}

func (d *DictionaryBlockDecoder) Close() {
	if d.decoder != nil {
		d.decoder.Close()
		d.decoder = nil
	}
}
