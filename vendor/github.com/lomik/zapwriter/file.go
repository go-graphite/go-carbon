package zapwriter

import (
	"fmt"
	"net/url"
	"os"
	"os/user"
	"path/filepath"
	"strconv"
	"sync"
	"time"
)

// with external rotate support
type FileOutput struct {
	sync.Mutex
	timeout   time.Duration
	interval  time.Duration
	checkNext time.Time
	f         *os.File
	path      string // filename

	exit     chan interface{}
	exitOnce sync.Once
	exitWg   sync.WaitGroup
}

func newFileOutput(path string) (*FileOutput, error) {
	u, err := url.Parse(path)

	if err != nil {
		return nil, err
	}

	var timeout time.Duration
	var interval time.Duration

	params := DSN(u.Query())
	if timeout, err = params.Duration("timeout", "1s"); err != nil {
		return nil, err
	}

	if interval, err = params.Duration("interval", "1s"); err != nil {
		return nil, err
	}

	f, err := os.OpenFile(u.Path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}

	r := &FileOutput{
		checkNext: time.Now().Add(timeout),
		timeout:   timeout,
		interval:  interval,
		f:         f,
		path:      u.Path,
		exit:      make(chan interface{}),
	}

	r.exitWg.Add(1)
	go func() {
		r.reopenChecker(r.exit)
		r.exitWg.Done()
	}()

	return r, nil
}

func File(path string) (*FileOutput, error) {
	return newFileOutput(path)
}

func (r *FileOutput) reopenChecker(exit chan interface{}) {
	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			r.doWithCheck(func() {})
		case <-exit:
			return
		}
	}
}

func (r *FileOutput) reopen() *os.File {
	prev := r.f
	next, err := os.OpenFile(r.path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		fmt.Println(err.Error())
		return r.f
	}

	r.f = next
	prev.Close()
	return r.f
}

func (r *FileOutput) Write(p []byte) (n int, err error) {
	r.doWithCheck(func() { n, err = r.f.Write(p) })
	return
}

func (r *FileOutput) Sync() (err error) {
	r.doWithCheck(func() { err = r.f.Sync() })
	return
}

func (r *FileOutput) Close() (err error) {
	r.exitOnce.Do(func() {
		close(r.exit)
	})
	r.exitWg.Wait()
	err = r.f.Close()
	return
}

func PrepareFileForUser(filename string, owner *user.User) error {
	u, err := url.Parse(filename)
	if err != nil {
		return err
	}

	if u.Path == "" || u.Path == "stderr" || u.Path == "stdout" || u.Path == "none" {
		return nil
	}

	if u.Scheme != "" && u.Scheme != "file" {
		return nil
	}

	if err := os.MkdirAll(filepath.Dir(u.Path), 0755); err != nil {
		return err
	}

	fd, err := os.OpenFile(u.Path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if fd != nil {
		fd.Close()
	}
	if err != nil {
		return err
	}
	if err := os.Chmod(u.Path, 0644); err != nil {
		return err
	}
	if owner != nil {

		uid, err := strconv.ParseInt(owner.Uid, 10, 0)
		if err != nil {
			return err
		}

		gid, err := strconv.ParseInt(owner.Gid, 10, 0)
		if err != nil {
			return err
		}

		if err := os.Chown(u.Path, int(uid), int(gid)); err != nil {
			return err
		}
	}

	return nil
}
