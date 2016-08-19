package logging

import (
	"bytes"
	"io"
	"os"
	"os/signal"
	"os/user"
	"path/filepath"
	"strconv"
	"sync"
	"syscall"

	"github.com/Sirupsen/logrus"
	"github.com/howeyc/fsnotify"
)

var std = NewFileLogger()

func init() {
	logrus.SetFormatter(&TextFormatter{})

	// signal watcher
	signalChan := make(chan os.Signal, 16)
	signal.Notify(signalChan, syscall.SIGHUP)

	go func() {
		for {
			select {
			case <-signalChan:
				err := std.Reopen()
				logrus.Infof("HUP received, reopen log %#v", std.Filename())
				if err != nil {
					logrus.Errorf("Reopen log %#v failed: %s", std.Filename(), err.Error())
				}
			}
		}
	}()
}

// FileLogger wrapper
type FileLogger struct {
	sync.RWMutex
	filename    string
	fd          *os.File
	watcherDone chan bool
}

// NewFileLogger create instance FileLogger
func NewFileLogger() *FileLogger {
	return &FileLogger{
		filename:    "",
		fd:          nil,
		watcherDone: nil,
	}
}

// Open file for logging
func (l *FileLogger) Open(filename string) error {
	l.Lock()
	l.filename = filename
	l.Unlock()

	reopenErr := l.Reopen()
	if l.watcherDone != nil {
		close(l.watcherDone)
	}
	l.watcherDone = make(chan bool)
	l.fsWatch(l.filename, l.watcherDone)

	return reopenErr
}

//
func (l *FileLogger) fsWatch(filename string, quit chan bool) {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		logrus.Warningf("fsnotify.NewWatcher(): %s", err)
		return
	}

	if filename == "" {
		return
	}

	subscribe := func() {
		if err := watcher.WatchFlags(filename, fsnotify.FSN_CREATE|fsnotify.FSN_DELETE|fsnotify.FSN_RENAME); err != nil {
			logrus.Warningf("fsnotify.Watcher.Watch(%s): %s", filename, err)
		}
	}

	subscribe()

	go func() {
		defer watcher.Close()

		for {
			select {
			case <-watcher.Event:
				l.Reopen()
				subscribe()

				logrus.Infof("Reopen log %#v by fsnotify event", std.Filename())
				if err != nil {
					logrus.Errorf("Reopen log %#v failed: %s", std.Filename(), err.Error())
				}

			case <-quit:
				return
			}
		}
	}()
}

// Reopen file
func (l *FileLogger) Reopen() error {
	l.Lock()
	defer l.Unlock()

	var newFd *os.File
	var err error

	if l.filename != "" {
		newFd, err = os.OpenFile(l.filename, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)

		if err != nil {
			return err
		}
	} else {
		newFd = nil
	}

	oldFd := l.fd
	l.fd = newFd

	var loggerOut io.Writer

	if l.fd != nil {
		loggerOut = l.fd
	} else {
		loggerOut = os.Stderr
	}
	logrus.SetOutput(loggerOut)

	if oldFd != nil {
		oldFd.Close()
	}

	return nil
}

// Filename returns current filename
func (l *FileLogger) Filename() string {
	l.RLock()
	defer l.RUnlock()
	return l.filename
}

// SetFile for default logger
func SetFile(filename string) error {
	return std.Open(filename)
}

// SetLevel for default logger
func SetLevel(lvl string) error {
	level, err := logrus.ParseLevel(lvl)
	if err != nil {
		return err
	}
	logrus.SetLevel(level)
	return nil
}

// PrepareFile creates logfile and set it writable for user
func PrepareFile(filename string, owner *user.User) error {
	if filename == "" {
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(filename), 0755); err != nil {
		return err
	}

	fd, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if fd != nil {
		fd.Close()
	}
	if err != nil {
		return err
	}
	if err := os.Chmod(filename, 0644); err != nil {
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

		if err := os.Chown(filename, int(uid), int(gid)); err != nil {
			return err
		}
	}

	return nil
}

type TestOut interface {
	Write(p []byte) (n int, err error)
	String() string
}

type buffer struct {
	sync.Mutex
	b bytes.Buffer
}

func (b *buffer) Write(p []byte) (n int, err error) {
	b.Lock()
	defer b.Unlock()
	return b.b.Write(p)
}
func (b *buffer) String() string {
	b.Lock()
	defer b.Unlock()
	return b.b.String()
}

// Test run callable with changed logging output
func Test(callable func(TestOut)) {
	buf := &buffer{}
	logrus.SetOutput(buf)

	callable(buf)

	var loggerOut io.Writer
	if std.fd != nil {
		loggerOut = std.fd
	} else {
		loggerOut = os.Stderr
	}

	logrus.SetOutput(loggerOut)
}

// TestWithLevel run callable with changed logging output and log level
func TestWithLevel(level string, callable func(TestOut)) {
	originalLevel := logrus.GetLevel()
	defer logrus.SetLevel(originalLevel)
	SetLevel(level)

	Test(callable)
}
