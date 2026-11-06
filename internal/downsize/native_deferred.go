package downsize

import (
	"bytes"
	"context"
	"errors"
	"fmt"

	"github.com/go-graphite/go-carbon/internal/chunkstore"
)

// A deferred attempt uses the original plan range and names, but a separately
// identified, immutable selection. Neither its receipt nor its intent replaces
// the phase-1 receipt. One attempted parent range consumes one max-batches slot.
func (c *nativeController) executeDeferred(ctx context.Context, parent task, names []string) error {
	s := parent.Native
	if s == nil || s.Intent || s.Pending != nil {
		return errors.New("native deferrals require an acknowledged range without metric work in flight")
	}
	if s.Receipt.Count != len(names) || len(s.Done) != len(s.Receipt.Copied) {
		return errors.New("native deferred bitmap differs from plan")
	}
	original := chunkstore.NativeRange{Operation: c.journal.operation, Plan: c.planData.PlanID, Stream: parent.Stream, Source: parent.Source, Start: parent.Start, End: parent.End, Names: names, Combine: s.Combine}
	id, hash, err := original.Identity()
	if err != nil || id != s.Receipt.Identity || hash != s.Receipt.NamesSHA256 {
		return errors.New("native deferred parent differs from plan")
	}
	var durable chunkstore.NativeReceipt
	if _, err := c.requestOnce(ctx, parent.Target, "/v1/native/receipt", original, &durable); err != nil {
		return fmt.Errorf("native deferred parent receipt: %w", err)
	}
	if err := durable.Validate(); err != nil {
		return err
	}
	if durable.Identity != id || durable.NamesSHA256 != hash || durable.Digest != s.Receipt.Digest || !bytes.Equal(durable.Copied, s.Receipt.Copied) {
		return errors.New("native deferred parent receipt changed")
	}
	next := *s
	next.Done = bytes.Clone(s.Done)
	next.Combines = append([]nativeDeferredAttempt(nil), s.Combines...)
	if len(next.Combines) == 0 || next.Combines[len(next.Combines)-1].Receipt != nil {
		selection := make([]byte, len(s.Done))
		for i := range names {
			if !s.done(i) {
				selection[i/8] |= 1 << uint(i%8)
			}
		}
		next.Combines = append(next.Combines, nativeDeferredAttempt{Selection: selection})
		parent.Native = &next
		c.stateMu.Lock()
		err = c.journal.save(parent)
		c.stateMu.Unlock()
		if err != nil {
			return err
		}
	}
	attempt := &next.Combines[len(next.Combines)-1]
	request := original
	request.Combine, request.DeferredFrom, request.Selection = true, id, attempt.Selection
	if len(next.Combines) > 1 {
		previous := next.Combines[len(next.Combines)-2].Receipt
		if previous == nil {
			return errors.New("native deferred attempt has no preceding acknowledgement")
		}
		request.DeferredFrom = previous.Identity
	}
	identity, namesHash, err := request.Identity()
	if err != nil {
		return err
	}
	for i := range names {
		if s.done(i) && request.Selection[i/8]&(1<<uint(i%8)) != 0 {
			return errors.New("native deferred selection includes completed ordinal")
		}
	}
	var receipt chunkstore.NativeReceipt
	_, err = c.requestOnce(ctx, parent.Target, "/v1/native/receipt", request, &receipt)
	if err != nil {
		var api *apiError
		if !errors.As(err, &api) || api.Status != 404 {
			return err
		}
		session, sessionErr := c.session(ctx, parent.Source)
		// A staged receiver can finish without the donor. A missing source pin
		// is harmless on that recovery path; otherwise the pull fails closed.
		if sessionErr != nil {
			session = ""
		}
		event("native_deferred_transfer", map[string]any{"stream": parent.Stream, "start": parent.Start, "end": parent.End, "parent_receipt": id, "receipt": identity})
		if _, err := c.requestOnce(ctx, parent.Target, "/v1/native/pull", struct {
			Range   chunkstore.NativeRange
			Session string
		}{request, session}, &receipt); err != nil {
			return errors.Join(err, sessionErr)
		}
	}
	if err := receipt.Validate(); err != nil {
		return err
	}
	if receipt.Identity != identity || receipt.NamesSHA256 != namesHash || receipt.Count != len(names) {
		return errors.New("native deferred acknowledgement differs from selection")
	}
	for i, copied := range receipt.Copied {
		if copied&^attempt.Selection[i] != 0 {
			return errors.New("native deferred acknowledgement includes unselected ordinal")
		}
		next.Done[i] |= copied
	}
	attempt.Receipt = &receipt
	parent.Native = &next
	parent.Bytes += receipt.Bytes
	if next.complete() {
		parent.Phase = "done"
	}
	c.stateMu.Lock()
	err = c.journal.save(parent)
	c.stateMu.Unlock()
	if err != nil {
		return err
	}
	event("native_deferred_verified", map[string]any{"stream": parent.Stream, "start": parent.Start, "end": parent.End, "bytes": receipt.Bytes, "copied_names": receipt.CopiedCount, "deferred_names": receipt.DeferredCount, "parent_receipt": id, "receipt": identity})
	return nil
}
