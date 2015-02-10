package logging

import (
	"io"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/Sirupsen/logrus"
)

func init() {
	logrus.SetFormatter(&TextFormatter{})

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, syscall.SIGHUP)

	go func() {
		for signal := range signalChan {
			if signal == syscall.SIGHUP {
				err := std.Reopen()
				logrus.Infof("HUP received, reopen log %#v", std.Filename())
				if err != nil {
					logrus.Errorf("Reopen log %#v failed: %#s", std.Filename(), err.Error())
				}
			}
		}
	}()
}

var std = NewFileLogger()

// FileLogger wrapper
type FileLogger struct {
	sync.RWMutex
	filename string
	fd       *os.File
}

// NewFileLogger create instance FileLogger
func NewFileLogger() *FileLogger {
	return &FileLogger{
		filename: "",
		fd:       nil,
	}
}

// Open file for logging
func (l *FileLogger) Open(filename string) error {
	l.Lock()
	l.filename = filename
	l.Unlock()

	return l.Reopen()
}

// Reopen file
func (l *FileLogger) Reopen() error {
	l.Lock()
	defer l.Unlock()

	var newFd *os.File
	var err error

	if l.filename != "" {
		newFd, err = os.OpenFile(l.filename, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)

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
	l.RUnlock()
	return l.filename
}

// SetFile for default logger
func SetFile(filename string) error {
	return std.Open(filename)
}
