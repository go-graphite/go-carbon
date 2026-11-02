// Package codec implements version 1 of the Graphite stream input protocol.
// It depends only on the standard library and klauspost/compress/zstd, so relays
// can import it independently of the receiver and its point representation.
//
// A connection starts with Magic (8 bytes), Version (1 byte), and flags (1 byte).
// FlagZstd compresses every nonempty frame payload as an independent zstd frame.
// All other flag bits are reserved and must be zero. Integers in the fixed
// header are big endian; payload integers use Go's encoding/binary varints.
//
// Each frame has a 20-byte header:
//
//	byte       kind: 0=data, 1=reset, 2=state
//	3 bytes    reserved, zero
//	uint32     payload length on the wire
//	uint32     uncompressed payload length
//	uint32     number of rows (zero for reset and state)
//	uint32     CRC-32C (Castagnoli) of the wire payload
//
// A data payload contains:
//
//	uvarint    first newly defined id (the current dictionary size)
//	uvarint    number of new names, then each: uvarint length, name bytes
//	uvarint    byte length of the id column, then of the timestamp column
//	ids        uvarint differences from the previous row's id (initially zero)
//	timestamps zigzag varints: timestamp minus this id's previous timestamp
//	values     XOR of IEEE 754 bits with this id's previous value bits
//
// IDs are dense, zero based, and connection local. Rows are stably sorted by id;
// ordering between different names is not preserved. An id's first point predicts
// the previous row's timestamp (zero at frame start), and zero value bits. Signed
// timestamp arithmetic wraps modulo 2^64. A value XOR is a zero byte if unchanged;
// otherwise its header is 1 + 8*leading_zero_bytes + significant_byte_count - 1,
// followed by those significant bytes, most significant first. Trailing zero bytes
// are omitted. This preserves every float bit pattern, including NaNs and -0.
//
// A reset has no payload and forgets all names, predictors, and caller slots.
// A state payload has first id, count, then for each id: length, name bytes, a
// predictor-present byte (0 or 1), and, if present, int64 timestamp and uint64
// value bits (both big endian). State frames append definitions without emitting
// points. A snapshot is a reset followed by bounded state frames; replaying it
// restores the dictionary and predictors but intentionally not caller slots.
//
// Both wire and uncompressed frame lengths, row counts, name lengths, and total
// dictionary size and retained bytes are bounded before allocation. Bad frames are terminal: no
// callback or external name resolver runs for a rejected frame, and the decoder must be discarded. A clean
// EOF is valid only between frames. Encoder and Decoder are not concurrency safe.
package codec
