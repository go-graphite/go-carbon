package chunkstore

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/cockroachdb/pebble"
)

// The base encoding stays unchanged. This permanent envelope rejects older
// binaries before they open Pebble and resume a partially cleaned intent.
const nativeAbandonFormatPrefix = "go-carbon-native-abandon-v1\n"

// NativeAbandonReceipt is a cleanup receipt, never proof that a range was copied.
// Published catalogs and durable NativeReceipts are deliberately left untouched.
type NativeAbandonReceipt struct {
	Identity       string
	State          string // cleaning, pieces, abandoned, or completed
	IntentBytes    int
	DeletedKeys    uint64
	PublishedNames int
}

func nativeAbandonKey(identity string) []byte { return []byte("COMBINE/abandon/" + identity) }

func (s *Store) nativeCombineNotAbandoned(identity string) error {
	_, closer, err := s.db.Get(nativeAbandonKey(identity))
	if err == nil {
		return errors.Join(fmt.Errorf("native identity %s abandoned: %w", identity, ErrConflict), closer.Close())
	}
	if errors.Is(err, pebble.ErrNotFound) {
		return nil
	}
	return err
}

func (s *Store) fenceNativeAbandon() error {
	s.coldPack.formatMu.Lock()
	defer s.coldPack.formatMu.Unlock()
	f, err := s.fs.Open(s.fs.PathJoin(s.dir, "CHUNKSTORE"))
	if err != nil {
		return err
	}
	value, readErr := io.ReadAll(io.LimitReader(f, 128))
	if err := errors.Join(readErr, f.Close()); err != nil {
		return err
	}
	if strings.HasPrefix(string(value), nativeAbandonFormatPrefix) {
		return nil
	}
	return replaceFormatMarker(s.fs, s.dir, nativeAbandonFormatPrefix+string(value))
}

// AbandonNativeCombine explicitly retires the exact legacy identity without
// publishing another name. It never decodes Raw, Origins, Positions, or Names.
// Pebble still supplies its monolithic encoded intent value; decoder scratch,
// published-ID protection and every delete batch have independent fixed caps.
func (s *Store) AbandonNativeCombine(ctx context.Context, identity string) (out NativeAbandonReceipt, err error) {
	if len(identity) != 64 {
		return out, ErrFormat
	}
	if _, err := hex.DecodeString(identity); err != nil {
		return out, ErrFormat
	}
	ctx, cancel := s.NativeTransferContext(ctx)
	defer cancel()
	release, err := s.admitNative(ctx)
	if err != nil {
		return out, err
	}
	defer release()
	unlock, err := s.lockNativeRange(ctx, identity)
	if err != nil {
		return out, err
	}
	defer unlock()
	if err := errors.Join(ctx.Err(), s.checkNativeReady(), s.fileOwnershipError()); err != nil {
		return out, err
	}
	if failure := s.nativeFailure.Load(); failure != nil {
		return out, failure.err
	}
	out.Identity = identity
	// Even a very large final receipt is not decoded or modified by ABANDON.
	if _, closer, err := s.db.Get(nativeReceiptKey(identity)); err == nil {
		out.State = "completed"
		return out, closer.Close()
	} else if !errors.Is(err, pebble.ErrNotFound) {
		return out, err
	}
	if value, closer, err := s.db.Get(nativeAbandonKey(identity)); err == nil {
		if len(value) > 1024 {
			_ = closer.Close()
			return out, ErrFormat
		}
		err := json.Unmarshal(value, &out)
		err = errors.Join(err, closer.Close())
		if err != nil {
			return out, err
		}
		if out.Identity != identity {
			return out, ErrFormat
		}
		if out.State == "abandoned" {
			return out, nil
		}
		if out.State != "cleaning" && out.State != "pieces" {
			return out, ErrFormat
		}
	} else if !errors.Is(err, pebble.ErrNotFound) {
		return out, err
	}
	if out.State != "pieces" {
		value, closer, err := s.db.Get(nativeCombineIntentKey(identity))
		if err != nil {
			return out, err
		}
		defer closer.Close()
		out.IntentBytes = len(value)
		// Validate the entire document and every cleanup path before a fence
		// or deletion. Both passes stream one span/directory at a time.
		if err := s.walkNativeAbandon(ctx, identity, value, nil); err != nil {
			return out, err
		}
		keep, err := s.nativeAbandonPublished(ctx, identity)
		if err != nil {
			return out, err
		}
		out.PublishedNames = len(keep)
		if err := s.fenceNativeAbandon(); err != nil {
			return out, err
		}
		out.State = "cleaning"
		if err := s.saveNativeAbandon(out); err != nil {
			return out, err
		}
		if err := s.nativeStep("abandon-fenced"); err != nil {
			return out, err
		}
		if err := s.walkNativeAbandon(ctx, identity, value, func(span *nativeSpan, directory string) error {
			if span != nil {
				return s.deleteNativeAbandonSpan(ctx, *span, keep, &out)
			}
			return s.fs.RemoveAll(directory)
		}); err != nil {
			return out, err
		}
		// Once this sync completes no subsequent attempt scans hidden data.
		// Piece records can now be removed in bounded batches without losing
		// the published-ID protection required by a cleanup retry.
		out.State = "pieces"
		if err := s.saveNativeAbandon(out); err != nil {
			return out, err
		}
		if err := s.nativeStep("abandon-cleaned"); err != nil {
			return out, err
		}
	}
	prefix := nativeCombinePiecePrefix(identity)
	if err := s.deleteNativeAbandonSpan(ctx, nativeSpan{prefix, prefixEnd(prefix)}, nil, &out); err != nil {
		return out, err
	}
	if err := ctx.Err(); err != nil {
		return out, err
	}
	out.State = "abandoned"
	b := s.db.NewBatch()
	defer b.Close()
	data, err := json.Marshal(out)
	if err != nil {
		return out, err
	}
	if err := errors.Join(b.Delete(nativeCombineIntentKey(identity), nil), b.Set(nativeAbandonKey(identity), data, nil)); err != nil {
		return out, err
	}
	err = b.Commit(pebble.Sync)
	s.nativeWriteFailed(err)
	return out, err
}

func (s *Store) saveNativeAbandon(out NativeAbandonReceipt) error {
	data, err := json.Marshal(out)
	if err != nil {
		return err
	}
	err = s.db.Set(nativeAbandonKey(out.Identity), data, pebble.Sync)
	s.nativeWriteFailed(err)
	return err
}

func (s *Store) nativeAbandonPublished(ctx context.Context, identity string) (map[[16]byte]struct{}, error) {
	keep := make(map[[16]byte]struct{})
	prefix := nativeCombinePiecePrefix(identity)
	it, err := s.db.NewIter(&pebble.IterOptions{LowerBound: prefix, UpperBound: prefixEnd(prefix)})
	if err != nil {
		return nil, err
	}
	defer it.Close()
	for it.First(); it.Valid(); it.Next() {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if len(it.Value()) > 1<<20 {
			return nil, ErrTransferLimit
		}
		var piece struct {
			Meta     struct{ ID, Generation uint64 }
			Position int
			Deferred bool
		}
		if err := json.Unmarshal(it.Value(), &piece); err != nil {
			return nil, err
		}
		if piece.Meta.ID == 0 || piece.Meta.Generation == 0 || piece.Position < 0 || !bytes.Equal(it.Key(), nativeCombinePieceKey(identity, piece.Position)) {
			return nil, ErrFormat
		}
		if !piece.Deferred {
			prefix := chunkPrefix(piece.Meta.ID, piece.Meta.Generation)
			keep[[16]byte(prefix[1:17])] = struct{}{}
			// Conservative allowance includes Go's map growth and buckets.
			if len(keep) > nativeRangeMetadataBytes/128 {
				return nil, ErrTransferLimit
			}
		}
	}
	return keep, it.Error()
}

func nativeAbandonID(key []byte) ([16]byte, error) {
	if len(key) >= 22 && key[0] == continuationBucketKeyPrefix {
		key = key[5:]
	}
	if len(key) < 17 || key[0] != 'c' && key[0] != 'u' {
		return [16]byte{}, ErrFormat
	}
	return [16]byte(key[1:17]), nil
}

func (s *Store) deleteNativeAbandonSpan(ctx context.Context, span nativeSpan, keep map[[16]byte]struct{}, out *NativeAbandonReceipt) error {
	it, err := s.db.NewIter(&pebble.IterOptions{LowerBound: span.Lower, UpperBound: span.Upper})
	if err != nil {
		return err
	}
	defer it.Close()
	b := s.db.NewBatch()
	defer func() { b.Close() }()
	flush := func() error {
		if err := ctx.Err(); err != nil {
			return err
		}
		data, err := json.Marshal(out)
		if err != nil {
			return err
		}
		if err := b.Set(nativeAbandonKey(out.Identity), data, nil); err != nil {
			return err
		}
		if err := b.Commit(pebble.Sync); err != nil {
			s.nativeWriteFailed(err)
			return err
		}
		b.Close()
		b = s.db.NewBatch()
		return s.nativeStep("abandon-delete-batch")
	}
	for it.First(); it.Valid(); it.Next() {
		if err := ctx.Err(); err != nil {
			return err
		}
		if len(it.Key()) > 4096 {
			return ErrTransferLimit
		}
		if keep != nil {
			id, err := nativeAbandonID(it.Key())
			if err != nil {
				return err
			}
			if _, ok := keep[id]; ok {
				continue
			}
		}
		if err := b.Delete(it.Key(), nil); err != nil {
			return err
		}
		out.DeletedKeys++
		if b.Len() >= 1<<20 {
			if err := flush(); err != nil {
				return err
			}
		}
	}
	if err := it.Error(); err != nil {
		return err
	}
	if b.Count() == 0 {
		return nil
	}
	return flush()
}

// Limit individual JSON strings before Decoder can grow its token buffer.
// The existing Pebble value is borrowed; no copy of the full intent is made.
type nativeAbandonJSONReader struct {
	*bytes.Reader
	quoted, escape bool
	length, bare   int
}

func (r *nativeAbandonJSONReader) Read(p []byte) (int, error) {
	n, err := r.Reader.Read(p)
	for _, c := range p[:n] {
		if !r.quoted {
			switch {
			case c == '"':
				r.quoted = true
				r.length = 0
				r.bare = 0
			case strings.ContainsRune(" \t\r\n{}[],:", rune(c)):
				r.bare = 0
			default:
				r.bare++
				if r.bare > 128 {
					return 0, ErrTransferLimit
				}
			}
			continue
		}
		r.length++
		if r.length > 1<<20 {
			return 0, ErrTransferLimit
		}
		switch {
		case r.escape:
			r.escape = false
		case c == '\\':
			r.escape = true
		case c == '"':
			r.quoted = false
		}
	}
	return n, err
}

// visit receives only already validated spans and canonical private directories.
func (s *Store) walkNativeAbandon(ctx context.Context, identity string, value []byte, visit func(*nativeSpan, string) error) error {
	d := json.NewDecoder(&nativeAbandonJSONReader{Reader: bytes.NewReader(value)})
	d.UseNumber()
	gotReceipt := false
	err := walkNativeAbandonObject(ctx, d, func(field string) error {
		switch field {
		case "Receipt":
			return walkNativeAbandonObject(ctx, d, func(field string) error {
				if field != "Identity" {
					return skipNativeAbandonJSON(ctx, d, 0)
				}
				var id string
				if err := d.Decode(&id); err != nil {
					return err
				}
				if id != identity {
					return ErrFormat
				}
				gotReceipt = true
				return nil
			})
		case "Spans", "Directories":
			token, err := d.Token()
			if err != nil {
				return err
			}
			if token == nil {
				return nil
			}
			if token != json.Delim('[') {
				return ErrFormat
			}
			for d.More() {
				if err := ctx.Err(); err != nil {
					return err
				}
				if field == "Spans" {
					var span nativeSpan
					if err := walkNativeAbandonObject(ctx, d, func(field string) error {
						switch field {
						case "Lower":
							return d.Decode(&span.Lower)
						case "Upper":
							return d.Decode(&span.Upper)
						default:
							return skipNativeAbandonJSON(ctx, d, 0)
						}
					}); err != nil {
						return err
					}
					if len(span.Lower) > 256 || len(span.Upper) > 256 || bytes.Compare(span.Lower, span.Upper) >= 0 {
						return ErrFormat
					}
					if _, err := nativeAbandonID(span.Lower); err != nil {
						return err
					}
					if span.Upper[0] != span.Lower[0] {
						return ErrFormat
					}
					if visit != nil {
						if err := visit(&span, ""); err != nil {
							return err
						}
					}
				} else {
					var directory string
					if err := d.Decode(&directory); err != nil {
						return err
					}
					name := s.fs.PathBase(directory)
					if !strings.HasPrefix(name, ".native-"+identity+"-") || directory != s.fs.PathJoin(s.dir, name) {
						return ErrFormat
					}
					if visit != nil {
						if err := visit(nil, directory); err != nil {
							return err
						}
					}
				}
			}
			_, err = d.Token()
			return err
		default:
			return skipNativeAbandonJSON(ctx, d, 0)
		}
	})
	if err != nil {
		return err
	}
	if !gotReceipt {
		return ErrFormat
	}
	if _, err := d.Token(); !errors.Is(err, io.EOF) {
		return ErrFormat
	}
	return nil
}

func walkNativeAbandonObject(ctx context.Context, d *json.Decoder, field func(string) error) error {
	token, err := d.Token()
	if err != nil {
		return err
	}
	if token != json.Delim('{') {
		return ErrFormat
	}
	for d.More() {
		if err := ctx.Err(); err != nil {
			return err
		}
		token, err := d.Token()
		if err != nil {
			return err
		}
		name, ok := token.(string)
		if !ok {
			return ErrFormat
		}
		if err := field(name); err != nil {
			return err
		}
	}
	_, err = d.Token()
	return err
}

func skipNativeAbandonJSON(ctx context.Context, d *json.Decoder, depth int) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if depth > 64 {
		return ErrFormat
	}
	token, err := d.Token()
	if err != nil {
		return err
	}
	if delim, ok := token.(json.Delim); ok {
		if delim != '[' && delim != '{' {
			return ErrFormat
		}
		for d.More() {
			if err := skipNativeAbandonJSON(ctx, d, depth+1); err != nil {
				return err
			}
		}
		_, err = d.Token()
	}
	return err
}
