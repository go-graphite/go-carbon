// +build !windows

package zapwriter

import (
	"time"
	"fmt"
	"syscall"
	"os"
)

func (r *FileOutput) doWithCheck(f func()) {
	r.Lock()
	defer r.Unlock()
	r.check()
	f()
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
