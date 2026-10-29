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
	Kind, Checkpoint string `json:",omitempty"`
	Retries          int
	Stream           string
	Start, End       int64
	Source, Target   string
	Names            []string                   `json:",omitempty"`
	Proofs           []chunkstore.TransferProof `json:",omitempty"`
	Phase            string
	Bytes            int64
}
type journal struct {
	operation    string
	lock         *os.File
	path         string
	sinceCompact int
	file         *os.File
	tasks        map[string]task
	plan         string
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
	j := &journal{file: f, lock: lock, path: path, plan: plan, tasks: map[string]task{}}
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
		var row struct {
			Operation string
			Plan      string
			Task      task
		}
		if err := json.Unmarshal(line, &row); err != nil {
			return fail(err)
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
	data, err := json.Marshal(struct {
		Operation string
		Plan      string
		Task      task
	}{j.operation, j.plan, t})
	if err != nil {
		return err
	}
	if _, err = j.file.Write(append(data, '\n')); err != nil {
		return err
	}
	if err = j.file.Sync(); err != nil {
		return err
	}
	if t.End > t.Start {
		j.tasks[taskKey(t)] = t
	}
	j.sinceCompact++
	if j.sinceCompact >= 256 {
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
	temporary, err := os.OpenFile(j.path+".next", os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	encoder := json.NewEncoder(temporary)
	err = encoder.Encode(struct {
		Operation string
		Plan      string
		Task      task
	}{j.operation, j.plan, task{}})
	if err == nil {
		for _, t := range j.tasks {
			if err = encoder.Encode(struct {
				Operation string
				Plan      string
				Task      task
			}{j.operation, j.plan, t}); err != nil {
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
