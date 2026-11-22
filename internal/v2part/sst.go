package v2part

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"hash"
	"io"
	"os"
	"slices"
	"sync"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/sstable"
)

// Writer accepts strictly sorted GCP3 SET records in exactly one scope. It owns
// a new, exclusively created staging path; Close syncs it, Abort removes it.
// The caller owns directory syncing, pinning, installation and publication.
type Writer struct {
	path    string
	decoder keyDecoder
	spec    Spec
	out     *hashedOutput
	sst     interface {
		Set([]byte, []byte) error
		Close() error
	}
	first, last    []byte
	lastName       ID
	names, records uint64
	err            error
	closed         bool
}
type hashedOutput struct {
	f        *os.File
	buf      *bufio.Writer
	h        hash.Hash
	n        uint64
	limit    uint64
	aborted  bool
	released bool
}

var ErrPartSizeLimit = errors.New("sealed part byte limit exceeded")

var partOutputBuffers = sync.Pool{New: func() any { return bufio.NewWriterSize(io.Discard, 256<<10) }}

func (o *hashedOutput) release() {
	if !o.released {
		o.released = true
		o.buf.Reset(io.Discard)
		partOutputBuffers.Put(o.buf)
	}
}

func (o *hashedOutput) Write(b []byte) error {
	if o.aborted {
		return io.ErrClosedPipe
	}
	if o.limit != 0 && (o.n > o.limit || uint64(len(b)) > o.limit-o.n) {
		return ErrPartSizeLimit
	}
	n, err := o.buf.Write(b)
	_, _ = o.h.Write(b[:n])
	o.n += uint64(n)
	return err
}
func (o *hashedOutput) Finish() error {
	defer o.release()
	if o.aborted {
		return io.ErrClosedPipe
	}
	err := o.buf.Flush()
	if err == nil {
		err = o.f.Sync()
	}
	return errors.Join(err, o.f.Close())
}
func (o *hashedOutput) Abort() {
	if !o.aborted {
		o.aborted = true
		_ = o.f.Close()
		o.release()
	}
}

func NewWriter(path string, spec Spec) (*Writer, error) {
	if err := validateSpec(spec); err != nil {
		return nil, err
	}
	policies, err := canonicalPolicies(spec.Policies)
	if err != nil {
		return nil, err
	}
	spec.Policies = policies
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return nil, err
	}
	buffer := partOutputBuffers.Get().(*bufio.Writer)
	buffer.Reset(f)
	out := &hashedOutput{f: f, buf: buffer, h: sha256.New()}
	if spec.Encoding == DenseEncoding {
		table, err := newDenseSSTWriter(out)
		if err != nil {
			out.Abort()
			_ = os.Remove(path)
			return nil, err
		}
		return &Writer{path: path, spec: spec, out: out, sst: table}, nil
	}
	// Match core's current default SST container, so ingestion needs no DB
	// format ratchet. GCP3 is the key schema, independent of SST container.
	opts := sstable.WriterOptions{Compression: sstable.SnappyCompression, TableFormat: sstable.TableFormatRocksDBv2}
	return &Writer{path: path, spec: spec, out: out, sst: sstable.NewWriter(out, opts)}, nil
}

func (w *Writer) Set(k Key, value []byte) error {
	key, err := Encode(k)
	if err != nil {
		w.err = errors.Join(w.err, err)
		return w.err
	}
	return w.SetEncoded(key, value)
}
func (w *Writer) SetEncoded(key, value []byte) error {
	if w.closed {
		return io.ErrClosedPipe
	}
	if w.err != nil {
		return w.err
	}
	k, err := w.decoder.decode(key)
	if err != nil {
		w.err = err
		return err
	}
	return w.setDecoded(key, k, value)
}

// setDecoded is internal: key must have been validated by the owning writer.
func (w *Writer) setDecoded(key []byte, k Key, value []byte) error {
	var err error
	if !w.spec.Scope.contains(k) || (w.records > 0 && bytes.Compare(w.last, key) >= 0) {
		err = errors.New("unsorted or foreign part key")
	}
	if err == nil && !hasArchive(w.spec.Policies, k.Archive) {
		err = errors.New("archive policy missing")
	}
	if err == nil {
		err = w.sst.Set(key, value)
	}
	if err != nil {
		w.err = err
		return err
	}
	if w.records == 0 {
		w.first = bytes.Clone(key)
	}
	if w.records == 0 || w.lastName != k.Metric {
		w.names++
		w.lastName = k.Metric
	}
	w.last = append(w.last[:0], key...)
	w.records++
	return nil
}
func hasArchive(policies []ArchivePolicy, archive uint32) bool {
	if archive == MetadataArchive {
		return true
	}
	for _, p := range policies {
		if p.Archive == archive {
			return true
		}
	}
	return false
}
func (w *Writer) Abort() error {
	if w.closed {
		return nil
	}
	w.closed = true
	w.out.Abort()
	_ = w.sst.Close() // release SST buffers/queues even when the staged file is discarded
	return os.Remove(w.path)
}
func (w *Writer) Close() (Descriptor, error) {
	if w.closed {
		return Descriptor{}, io.ErrClosedPipe
	}
	if w.err != nil || w.records == 0 {
		err := w.err
		if err == nil {
			err = errors.New("empty part")
		}
		return Descriptor{}, errors.Join(err, w.Abort())
	}
	err := w.sst.Close()
	w.closed = true
	if err != nil {
		return Descriptor{}, errors.Join(err, os.Remove(w.path))
	}
	d := Descriptor{Version: SchemaVersion, KeyVersion: KeyVersion, Scope: w.spec.Scope, Encoding: w.spec.Encoding, Source: w.spec.Source, Bytes: w.out.n, FirstKey: w.first, LastKey: w.last, Coverage: Coverage{MinClock: w.spec.MinClock, MinTime: w.spec.MinTime, MaxTime: w.spec.MaxTime, Names: w.names, Records: w.records, Policies: w.spec.Policies, MinExpiry: w.spec.MinExpiry, MaxExpiry: w.spec.MaxExpiry}}
	if w.spec.Encoding == DenseEncoding {
		d.Version = GroupFileVersion
	}
	copy(d.SHA256[:], w.out.h.Sum(nil))
	return d, d.Validate()
}

// VerifyStream hashes bytes during copy without decoding a single metric. The
// destination is staging only until this succeeds and its owner syncs/installs
// it. An extra byte, truncation, context cancellation or checksum mismatch fails.
func VerifyStream(ctx context.Context, dst io.Writer, src io.Reader, d Descriptor) error {
	if err := d.Validate(); err != nil {
		return err
	}
	h := sha256.New()
	buf := make([]byte, 256<<10)
	var total uint64
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		n, err := src.Read(buf)
		if n > 0 {
			if uint64(n) > d.Bytes-total {
				return errors.New("part exceeds declared size")
			}
			if _, e := io.Copy(io.MultiWriter(dst, h), bytes.NewReader(buf[:n])); e != nil {
				return e
			}
			total += uint64(n)
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrNoProgress
		}
	}
	var sum Digest
	copy(sum[:], h.Sum(nil))
	if total != d.Bytes || sum != d.SHA256 {
		return errors.New("part size/checksum mismatch")
	}
	return nil
}

// Read visits borrowed encoded keys/values from a GCP3 SST and validates all
// records, including interior keys (checking only min/max is insufficient).
// A nil expected descriptor enables the one-way adapter for existing core SSTs;
// it still rejects mixed scopes, tombstones and nonzero internal sequences.
// With an expected descriptor, physical SHA256 is verified before callbacks.
func Read(ctx context.Context, path string, expected *Descriptor, visit func(Key, []byte) error) (err error) {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	if expected != nil {
		if err = VerifyStream(ctx, io.Discard, f, *expected); err != nil {
			return errors.Join(err, f.Close())
		}
		if _, err = f.Seek(0, io.SeekStart); err != nil {
			return errors.Join(err, f.Close())
		}
	}
	readable, err := sstable.NewSimpleReadable(f)
	if err != nil {
		return errors.Join(err, f.Close())
	}
	r, err := sstable.NewReader(readable, sstable.ReaderOptions{})
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, r.Close()) }()
	if r.Properties.NumRangeDeletions != 0 || r.Properties.NumRangeKeys() != 0 {
		return errors.New("range mutations in portable part")
	}
	it, err := r.NewIter(nil, nil)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, it.Close()) }()
	var scope Scope
	var first, last []byte
	var lastName ID
	var names, records uint64
	var shards []uint16
	for ik, value := it.First(); ik != nil; ik, value = it.Next() {
		if err := ctx.Err(); err != nil {
			return err
		}
		if ik.Kind() != pebble.InternalKeyKindSet || ik.SeqNum() != 0 {
			return errors.New("non-SET or nonzero-sequence part record")
		}
		k, e := Decode(ik.UserKey)
		if e != nil {
			return e
		}
		if records == 0 {
			scope = Scope{Group: k.Group, VShard: k.VShard, Partition: k.Partition, Part: k.Part}
			if expected != nil {
				scope = expected.Scope
			}
			first = bytes.Clone(ik.UserKey)
		}
		if !scope.contains(k) || (records > 0 && bytes.Compare(last, ik.UserKey) >= 0) {
			return errors.New("mixed or unsorted part scope")
		}
		if expected != nil && (!expected.Scope.contains(k) || !hasArchive(expected.Coverage.Policies, k.Archive)) {
			return errors.New("record outside descriptor")
		}
		if records == 0 || lastName != k.Metric {
			names++
			lastName = k.Metric
		}
		if expected != nil && len(expected.VShards) > 0 && (len(shards) == 0 || shards[len(shards)-1] != k.VShard) {
			shards = append(shards, k.VShard)
		}
		last = append(last[:0], ik.UserKey...)
		records++
		data, _, e := value.Value(nil)
		if e != nil {
			return e
		}
		if visit != nil {
			if e = visitLogical(k, data, visit); e != nil {
				return e
			}
		}
	}
	if err := it.Error(); err != nil {
		return err
	}
	if records == 0 {
		return errors.New("empty part SST")
	}
	if expected != nil && (names != expected.Coverage.Names || records != expected.Coverage.Records || !bytes.Equal(first, expected.FirstKey) || !bytes.Equal(last, expected.LastKey)) {
		return errors.New("part coverage count/key mismatch")
	}
	if expected != nil && len(expected.VShards) > 0 && !slices.Equal(shards, expected.VShards) {
		return errors.New("part vshard inventory mismatch")
	}
	return nil
}

// visitLogical visits a record as the logical stream: a run with a folded
// envelope is its run, then the envelope at the metric's envelope key.
// Descriptor coverage still counts physical records.
func visitLogical(k Key, data []byte, visit func(Key, []byte) error) error {
	if k.Archive == MetadataArchive {
		return visit(k, data)
	}
	envelope, run, err := SplitHeader(data)
	if err == nil {
		err = visit(k, run)
	}
	if err == nil && envelope != nil {
		err = visit(EnvelopeKey(k), envelope)
	}
	return err
}
