package storecopy

import (
	"errors"
	"os"
	"reflect"
	"syscall"
)

// FileStamp identifies an already checksummed and synced staging file. Ctime
// catches ordinary writes even when a tool restores the original mtime.
type FileStamp struct {
	Size   int64  `json:"size"`
	Mtime  int64  `json:"mtime_ns"`
	Ctime  int64  `json:"ctime_ns"`
	Device uint64 `json:"device"`
	Inode  uint64 `json:"inode"`
}

func stamp(info os.FileInfo) (FileStamp, error) {
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || !info.Mode().IsRegular() {
		return FileStamp{}, errors.New("staging file identity is unavailable")
	}
	// Go exposes the same Unix change time as Ctim on Linux and Ctimespec on
	// Darwin. Reflection here is once per file, outside the streaming hot path.
	value := reflect.ValueOf(stat).Elem()
	change := value.FieldByName("Ctim")
	if !change.IsValid() {
		change = value.FieldByName("Ctimespec")
	}
	if !change.IsValid() {
		return FileStamp{}, errors.New("staging file change time is unavailable")
	}
	ctime := change.FieldByName("Sec").Int()*1e9 + change.FieldByName("Nsec").Int()
	return FileStamp{info.Size(), info.ModTime().UnixNano(), ctime, uint64(stat.Dev), uint64(stat.Ino)}, nil
}
