package chunkstore

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"io"
)

const TransferProofContentType = "application/vnd.go-carbon.transfer-proofs-v1"

// EncodeTransferProofs preserves every receipt field, including nil counts.
// The negotiated envelope avoids JSON round trips for sparse batch receipts.
func EncodeTransferProofs(proofs []TransferProof) ([]byte, error) {
	if len(proofs) == 0 || len(proofs) > MaxTransferMetrics {
		return nil, ErrTransferLimit
	}
	size := 44
	for _, p := range proofs {
		if len(p.Source.Name) > 4096 || len(p.Target.Name) > 4096 || len(p.Digest) > 256 || len(p.Counts) > 65536 {
			return nil, ErrTransferLimit
		}
		size += 76 + len(p.Source.Name) + len(p.Target.Name) + len(p.Digest) + 8*len(p.Counts)
		if size > 32<<20 {
			return nil, ErrTransferLimit
		}
	}
	data := make([]byte, 0, size)
	data = append(data, "BKTPF001"...)
	u32 := func(n uint32) { data = binary.BigEndian.AppendUint32(data, n) }
	u64 := func(n uint64) { data = binary.BigEndian.AppendUint64(data, n) }
	text := func(s string) { u32(uint32(len(s))); data = append(data, s...) }
	version := func(v TransferVersion) { text(v.Name); u64(v.ID); u64(v.Generation); u64(v.Revision) }
	u32(uint32(len(proofs)))
	for _, p := range proofs {
		u64(uint64(p.KeyCount))
		flags := uint32(0)
		if p.Sampled {
			flags |= 1
		}
		if p.Merged {
			flags |= 2
		}
		if p.Counts == nil {
			flags |= 4
		}
		u32(flags)
		version(p.Source)
		version(p.Target)
		text(p.Digest)
		u32(uint32(len(p.Counts)))
		for _, count := range p.Counts {
			u64(uint64(count))
		}
	}
	digest := sha256.Sum256(data)
	return append(data, digest[:]...), nil
}

func ReadTransferProofs(r io.Reader) ([]TransferProof, error) {
	data, err := io.ReadAll(io.LimitReader(r, (32<<20)+1))
	if err != nil {
		return nil, err
	}
	if len(data) > 32<<20 {
		return nil, ErrTransferLimit
	}
	if len(data) < 44 || string(data[:8]) != "BKTPF001" {
		return nil, ErrFormat
	}
	body := data[:len(data)-sha256.Size]
	digest := sha256.Sum256(body)
	if !bytes.Equal(digest[:], data[len(body):]) {
		return nil, ErrFormat
	}
	d := transferDecoder{data: body[8:]}
	n := d.count(76)
	if n == 0 || n > MaxTransferMetrics {
		return nil, ErrTransferLimit
	}
	proofs := make([]TransferProof, n)
	version := func() TransferVersion { return TransferVersion{string(d.blob(4096)), d.u64(), d.u64(), d.u64()} }
	for i := range proofs {
		p := &proofs[i]
		p.KeyCount = int(d.u64())
		flags := d.u32()
		if flags > 7 {
			return nil, ErrFormat
		}
		p.Sampled, p.Merged = flags&1 != 0, flags&2 != 0
		p.Source, p.Target = version(), version()
		p.Digest = string(d.blob(256))
		n := d.count(8)
		if n > 65536 {
			return nil, ErrTransferLimit
		}
		if flags&4 == 0 {
			p.Counts = make([]int, n)
		} else if n != 0 {
			return nil, ErrFormat
		}
		for j := range p.Counts {
			p.Counts[j] = int(d.u64())
		}
		if d.err != nil {
			return nil, d.err
		}
	}
	if len(d.data) != 0 {
		return nil, ErrFormat
	}
	return proofs, nil
}
