package downsize

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/go-graphite/go-carbon/internal/chunkstore"
)

// One range record carries the native copied bitmap and the reviewed importer's
// single in-flight fallback. Children never become overlapping journal ranges.
type nativeRangeState struct {
	Intent   bool `json:",omitempty"`
	Combine  bool `json:",omitempty"`
	Receipt  chunkstore.NativeReceipt
	Done     []byte
	Pending  *task                   `json:",omitempty"`
	Combines []nativeDeferredAttempt `json:",omitempty"`
}

// Nil Receipt is a durable intent. Keep its original selection on recovery;
// recalculating it after a lost acknowledgement could identify different work.
type nativeDeferredAttempt struct {
	Selection []byte
	Receipt   *chunkstore.NativeReceipt `json:",omitempty"`
}

func (s *nativeRangeState) done(i int) bool {
	return i >= 0 && i/8 < len(s.Done) && s.Done[i/8]&(1<<uint(i%8)) != 0
}
func (s *nativeRangeState) complete() bool {
	if s.Intent {
		return false
	}
	for i := range s.Receipt.Count {
		if !s.done(i) {
			return false
		}
	}
	return true
}

func (j *journal) saveNativeChild(child task) error {
	parent, ok := j.tasks[child.NativeParent]
	if !ok || parent.Native == nil {
		return errors.New("native fallback parent missing")
	}
	if parent.Native.Intent {
		return errors.New("native acknowledgement must be resolved before metric fallback")
	}
	next := *parent.Native
	next.Done = bytes.Clone(next.Done)
	if len(child.NativeOrdinals) != len(child.Names) && child.Phase != "done" {
		return errors.New("native fallback ordinal/name mismatch")
	}
	if child.Phase == "done" {
		for _, i := range child.NativeOrdinals {
			if i < 0 || i >= next.Receipt.Count {
				return errors.New("native fallback ordinal outside parent")
			}
			next.Done[i/8] |= 1 << uint(i%8)
		}
		next.Pending = nil
		parent.Bytes += child.Bytes
	} else {
		saved := child
		next.Pending = &saved
	}
	parent.Native = &next
	parent.Names = nil
	parent.Proofs = nil
	parent.Phase = "verified"
	if next.complete() {
		parent.Phase = "done"
		parent.Native = nil
	}
	return j.appendRow(journalRow{Operation: j.operation, Plan: j.plan, Task: parent})
}

func (m *mover) nativeRangeNames(t task) ([]string, error) {
	if len(t.Names) > 0 {
		return t.Names, nil
	}
	for _, r := range m.planData.Streams {
		if r.SHA256 == t.Stream {
			var names []string
			err := m.planData.walk(r, func(ordinal int64, row planRow) error {
				if ordinal >= t.Start && ordinal < t.End {
					names = append(names, row.Metric)
				}
				return nil
			})
			if err != nil {
				return nil, err
			}
			if int64(len(names)) != t.End-t.Start {
				return nil, errors.New("native journal range differs from plan")
			}
			return names, nil
		}
	}
	return nil, errors.New("native journal stream missing from plan")
}

func (m *mover) executeNativeFallback(ctx context.Context, parent task) error {
	names, err := m.nativeRangeNames(parent)
	if err != nil {
		return err
	}
	request := chunkstore.NativeRange{Operation: m.journal.operation, Plan: m.planData.PlanID, Stream: parent.Stream, Source: parent.Source, Start: parent.Start, End: parent.End, Names: names, Combine: parent.Native.Combine}
	var durable chunkstore.NativeReceipt
	if _, err := m.requestOnce(ctx, parent.Target, "/v1/native/receipt", request, &durable); err != nil {
		return fmt.Errorf("native handoff receipt unresolved (resume --engine bulk; never replay through metric mode): %w", err)
	}
	if parent.Native.Intent {
		identity, namesHash, err := request.Identity()
		if err != nil {
			return err
		}
		if err := durable.Validate(); err != nil {
			return err
		}
		if durable.Identity != identity || durable.NamesSHA256 != namesHash || durable.Count != len(names) || len(durable.Copied) != (len(names)+7)/8 {
			return errors.New("native recovered receipt differs from plan")
		}
		parent.Native = &nativeRangeState{Combine: request.Combine, Receipt: durable, Done: bytes.Clone(durable.Copied)}
		parent.Bytes = durable.Bytes
		if parent.Native.complete() {
			parent.Phase = "done"
			parent.Native = nil
		}
		m.stateMu.Lock()
		err = m.journal.save(parent)
		m.stateMu.Unlock()
		if err != nil {
			return err
		}
		for _, skip := range durable.Skipped {
			nativeSkipEvent(durable.Identity, skip)
		}
		if parent.Phase == "done" {
			return nil
		}
	}
	if len(durable.Skipped) > 0 {
		return fmt.Errorf("native range has %d size skips; inspect the journal before choosing a fallback", len(durable.Skipped))
	}
	if durable.Identity != parent.Native.Receipt.Identity || durable.Digest != parent.Native.Receipt.Digest || !bytes.Equal(durable.Copied, parent.Native.Receipt.Copied) {
		return errors.New("native handoff receipt differs")
	}
	if parent.Native.Pending != nil {
		if err := m.executeMetricPolicy(ctx, *parent.Native.Pending); err != nil {
			return err
		}
	}
	key := taskKey(parent)
	for {
		m.stateMu.Lock()
		parent = m.journal.tasks[key]
		m.stateMu.Unlock()
		if parent.Phase == "done" {
			return nil
		}
		if parent.Native == nil {
			return errors.New("native fallback lost bitmap")
		}
		count, budget := m.tuner.batch(parent.Source, parent.Target)
		child := task{Stream: parent.Stream, Start: parent.Start, Source: parent.Source, Target: parent.Target, NativeParent: key}
		for i, name := range names {
			if parent.Native.done(i) {
				continue
			}
			if len(child.Names) > 0 && (len(child.Names) >= count || child.Bytes+int64(512+len(name)*2) > budget) {
				break
			}
			child.Names = append(child.Names, name)
			child.NativeOrdinals = append(child.NativeOrdinals, i)
			child.Bytes += int64(512 + len(name)*2)
		}
		if len(child.Names) == 0 {
			return errors.New("native fallback has no unfinished names")
		}
		child.Start = parent.Start + int64(child.NativeOrdinals[0])
		child.End = child.Start + int64(len(child.Names))
		if err := m.executeMetricPolicy(ctx, child); err != nil {
			return err
		}
	}
}

// A native-only journal can learn the metric admission baseline before its
// first reviewed import. Any saved metric child or changed completion bitmap
// rules that out; normal journals retain their existing fail-closed behavior.
func (j *journal) nativeOnlyWork() bool {
	for _, t := range j.tasks {
		if t.Kind != "native-range" {
			return false
		}
		if t.Native != nil && (t.Native.Pending != nil || (!t.Native.Intent && !bytes.Equal(t.Native.Done, t.Native.Receipt.Copied))) {
			return false
		}
	}
	return true
}

func (m *mover) executeMetricPolicy(ctx context.Context, t task) error {
	if m.tuner != nil && m.tuner.maxSpeed != nil {
		return m.executeMaximumSpeed(ctx, t)
	}
	return m.execute(ctx, t)
}

// The existing pull API returns these definitive rejection statuses before
// importing any point. Preserve that distinction from a lost import response:
// the child may retry without bypassing its parent's durable copied bitmap.
func (m *mover) saveNativeDeclinedPull(t task) error {
	t.Kind, t.Phase, t.Proofs = "", "", nil
	m.stateMu.Lock()
	defer m.stateMu.Unlock()
	return m.journal.save(t)
}

func nativeDefinitivePullRejection(api *apiError) bool {
	switch api.Status {
	case 401, 403, 429:
		return true // Authentication and transfer admission precede ImportTransfer.
	case 404, 413:
		// Do not infer that an importer-side metadata/limit error happened
		// before publication. Only an explicit upstream source rejection proves it.
		return strings.HasSuffix(strings.TrimSpace(api.Message), fmt.Sprintf(": HTTP %d source export: %d %s", api.Status, api.Status, http.StatusText(api.Status)))
	}
	return false
}

func nativeSkipEvent(identity string, skip chunkstore.NativeSkip) {
	event("native_skipped", map[string]any{"receipt": identity, "ordinal": skip.Ordinal, "name": skip.Name, "bytes": skip.Bytes, "physical_points": skip.PhysicalPoints, "archive_slots": skip.ArchiveSlots, "worker_bytes": skip.MemoryBytes, "reason": skip.Reason})
}
