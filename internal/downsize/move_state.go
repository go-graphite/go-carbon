package downsize

import (
	"bufio"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"syscall"

	"github.com/go-graphite/go-carbon/internal/chunkstore"
)

type task struct {
	Native           *nativeRangeState `json:",omitempty"`
	NativeParent     string            `json:",omitempty"`
	NativeOrdinals   []int             `json:",omitempty"`
	Kind, Checkpoint string            `json:",omitempty"`
	Retries          int
	Stream           string
	Start, End       int64
	Source, Target   string
	Names            []string                   `json:",omitempty"`
	Proofs           []chunkstore.TransferProof `json:",omitempty"`
	Phase            string
	Bytes            int64
	// idleAttempts counts idle-timeout retries of one in-memory batch; it is
	// not journaled, so a resume starts a fresh bounded retry budget.
	idleAttempts int
}
type integritySkip struct {
	Source, Target, Stream string
	Start, End             int64
	Name, Reason, At       string
}
type journal struct {
	maxSpeed                bool
	maxSpeedDecisionSHA     string
	maxSpeedDecisionHistory []string
	integritySkips          []integritySkip
	preserveRows            bool // native handoff must preserve unknown fields from newer controllers
	operation               string
	lock                    *os.File
	path                    string
	sinceCompact            int
	file                    *os.File
	tasks                   map[string]task
	plan                    string
	admission               *admissionState
	absent                  map[string]absentSourceSkip
}

type journalRow struct {
	NativeSource           *nativeSourceCheckpoint `json:",omitempty"`
	MaxSpeedDecisionSHA256 string                  `json:",omitempty"`
	SkippedIntegrity       *integritySkip          `json:",omitempty"`
	Operation              string
	Plan                   string
	Task                   task
	Admission              *admissionState   `json:",omitempty"`
	SkippedAbsentSource    *absentSourceSkip `json:",omitempty"`
}

func taskKey(t task) string { return fmt.Sprintf("%s:%d:%d", t.Stream, t.Start, t.End) }
func openJournal(path, plan string) (*journal, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	var lock *os.File
	fail := func(err error) (*journal, error) {
		f.Close()
		if lock != nil {
			lock.Close()
		}
		return nil, err
	}
	lock, err = os.OpenFile(path+".lock", os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return fail(err)
	}
	if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		lock.Close()
		return fail(errors.New("state file is already locked"))
	}
	j := &journal{file: f, lock: lock, path: path, plan: plan, tasks: map[string]task{}, absent: map[string]absentSourceSkip{}}
	reader := bufio.NewReader(f)
	var offset int64
	var operation string
	for {
		line, err := reader.ReadBytes('\n')
		if err == io.EOF {
			if len(line) != 0 {
				if err := f.Truncate(offset); err != nil {
					return fail(err)
				}
			}
			break
		}
		if err != nil {
			return fail(err)
		}
		var row journalRow
		if err := json.Unmarshal(line, &row); err != nil {
			return fail(err)
		}
		// Other campaign controllers have additional policy/audit fields. Keep
		// their exact original JSON rows rather than silently compacting them away.
		var rawFields map[string]json.RawMessage
		if err := json.Unmarshal(line, &rawFields); err != nil {
			return fail(err)
		}
		for key := range rawFields {
			switch key {
			case "Operation", "Plan", "Task", "Admission", "SkippedAbsentSource", "MaxSpeedDecisionSHA256", "SkippedIntegrity":
			default:
				j.preserveRows = true
			}
		}
		if row.MaxSpeedDecisionSHA256 != "" {
			j.recordMaxSpeedDecision(row.MaxSpeedDecisionSHA256)
		}
		if row.SkippedIntegrity != nil {
			j.integritySkips = append(j.integritySkips, *row.SkippedIntegrity)
		}

		if len(row.Operation) != 64 {
			return fail(errors.New("invalid checkpoint operation"))
		}
		if operation == "" {
			operation = row.Operation
		} else if operation != row.Operation {
			return fail(errors.New("mixed checkpoint operations"))
		}
		if row.Plan != plan {
			return fail(errors.New("state belongs to a different plan or deletion mode"))
		}
		if row.Task.End > row.Task.Start {
			j.tasks[taskKey(row.Task)] = row.Task
			if row.Task.Native != nil || row.Task.Kind == "native-range" {
				j.preserveRows = true
			}
		}
		if row.Admission != nil {
			j.admission = row.Admission
		}
		if row.SkippedAbsentSource != nil {
			s := *row.SkippedAbsentSource
			j.absent[s.key()] = s
		}
		offset += int64(len(line))
	}
	if _, err := f.Seek(0, io.SeekEnd); err != nil {
		return fail(err)
	}
	if operation == "" {
		var nonce [32]byte
		if _, err := rand.Read(nonce[:]); err != nil {
			return fail(err)
		}
		operation = hex.EncodeToString(nonce[:])
	}
	j.operation = operation
	if offset == 0 {
		if err := j.save(task{}); err != nil {
			return fail(err)
		}
	}
	if err := syncDirectory(filepath.Dir(path)); err != nil {
		j.close()
		return nil, err
	}
	return j, nil
}
func (j *journal) save(t task) error {
	if t.NativeParent != "" {
		return j.saveNativeChild(t)
	}
	if t.Native != nil || t.Kind == "native-range" {
		j.preserveRows = true
	}
	return j.appendRow(journalRow{Operation: j.operation, Plan: j.plan, Task: t})
}

func (j *journal) saveAdmission(state *admissionState) error {
	return j.appendRow(journalRow{Operation: j.operation, Plan: j.plan, Admission: state})
}

func (j *journal) appendRow(row journalRow) error {
	data, err := json.Marshal(row)
	if err != nil {
		return err
	}
	if _, err = j.file.Write(append(data, '\n')); err != nil {
		return err
	}
	if err = j.file.Sync(); err != nil {
		return err
	}
	if row.Task.End > row.Task.Start {
		j.tasks[taskKey(row.Task)] = row.Task
	}
	if row.Admission != nil {
		j.admission = row.Admission
	}
	if row.MaxSpeedDecisionSHA256 != "" {
		j.recordMaxSpeedDecision(row.MaxSpeedDecisionSHA256)
	}
	if row.SkippedIntegrity != nil {
		j.integritySkips = append(j.integritySkips, *row.SkippedIntegrity)
	}

	j.sinceCompact++
	if j.sinceCompact >= 256 && !j.preserveRows {
		return j.compact()
	}
	return nil
}
func (j *journal) ranges(stream string) []task {
	var result []task
	for _, t := range j.tasks {
		if t.Stream == stream {
			result = append(result, t)
		}
	}
	sort.Slice(result, func(i, k int) bool { return result[i].Start < result[k].Start })
	return result
}

func syncDirectory(path string) error {
	d, err := os.Open(path)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
func (j *journal) close() { _ = j.file.Close(); _ = j.lock.Close() }
func (j *journal) compact() error {
	if j.preserveRows {
		j.sinceCompact = 0
		return j.file.Sync()
	}

	temporary, err := os.OpenFile(j.path+".next", os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	encoder := json.NewEncoder(temporary)
	err = encoder.Encode(journalRow{Operation: j.operation, Plan: j.plan, Admission: j.admission})
	if err == nil {
		for _, sha := range j.maxSpeedDecisionHistory {
			if err = encoder.Encode(journalRow{Operation: j.operation, Plan: j.plan, MaxSpeedDecisionSHA256: sha}); err != nil {
				break
			}
		}
	}
	if err == nil {
		for i := range j.integritySkips {
			if err = encoder.Encode(journalRow{Operation: j.operation, Plan: j.plan, SkippedIntegrity: &j.integritySkips[i]}); err != nil {
				break
			}
		}
	}

	if err == nil {
		for _, t := range j.tasks {
			if err = encoder.Encode(journalRow{Operation: j.operation, Plan: j.plan, Task: t}); err != nil {
				break
			}
		}
	}
	if err == nil {
		for _, skip := range j.absent {
			if err = encoder.Encode(struct {
				Operation, Plan     string
				SkippedAbsentSource absentSourceSkip
			}{j.operation, j.plan, skip}); err != nil {
				break
			}
		}
	}
	if err == nil {
		err = temporary.Sync()
	}
	err = errors.Join(err, temporary.Close())
	if err != nil {
		return err
	}
	if err := os.Rename(j.path+".next", j.path); err != nil {
		return err
	}
	if err := syncDirectory(filepath.Dir(j.path)); err != nil {
		return err
	}
	next, err := os.OpenFile(j.path, os.O_RDWR|os.O_APPEND, 0600)
	if err != nil {
		return err
	}
	_ = j.file.Close()
	j.file = next
	j.sinceCompact = 0
	return nil
}

func (j *journal) recordMaxSpeedDecision(sha string) {
	if j.maxSpeedDecisionSHA == sha {
		return
	}
	j.maxSpeedDecisionHistory = append(j.maxSpeedDecisionHistory, sha)
	j.maxSpeedDecisionSHA = sha
}
