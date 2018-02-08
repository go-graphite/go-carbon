package zapwriter

import (
	"fmt"
	"net/url"
	"os"
	"os/user"
	"path/filepath"
	"strconv"
	"sync"
	"syscall"
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

	s := u.Query().Get("timeout")
	if s == "" {
		timeout = time.Second
	} else {
		timeout, err = time.ParseDuration(s)
		if err != nil {
			return nil, err
		}
	}

	s = u.Query().Get("interval")
	if s == "" {
		interval = time.Second
	} else {
		interval, err = time.ParseDuration(s)
		if err != nil {
			return nil, err
		}
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
			r.Lock()
			r.check()
			r.Unlock()
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

func (r *FileOutput) check() {
	now := time.Now()

	if now.Before(r.checkNext) {
		return
	}

	r.checkNext = time.Now().Add(r.timeout)

	fInfo, err := r.f.Stat()
	if err != nil {
		fmt.Println(err.Error())
		return
	}

	fStat, ok := fInfo.Sys().(*syscall.Stat_t)
	if !ok {
		fmt.Println("Not a syscall.Stat_t")
		return
	}

	pInfo, err := os.Stat(r.path)
	if err != nil {
		// file deleted (?)
		r.reopen()
		return
	}

	pStat, ok := pInfo.Sys().(*syscall.Stat_t)
	if !ok {
		fmt.Println("Not a syscall.Stat_t")
		return
	}

	if fStat.Ino != pStat.Ino {
		// file on disk changed
		r.reopen()
		return
	}
}

func (r *FileOutput) Write(p []byte) (n int, err error) {
	r.Lock()
	r.check()
	n, err = r.f.Write(p)
	r.Unlock()
	return
}

func (r *FileOutput) Sync() (err error) {
	r.Lock()
	r.check()
	err = r.f.Sync()
	r.Unlock()
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
