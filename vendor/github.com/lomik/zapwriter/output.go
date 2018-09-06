package zapwriter

import (
	"fmt"
	"io"
	"log"
	"net/url"
	"os"
	"sync"
)

type WriteSyncer interface {
	io.Writer
	Sync() error
}

type closeable interface {
	Close() (err error)
}

type Output interface {
	io.Writer
	Sync() error
}

var knownSchemes = make(map[string](func(string) (Output, error)))
var knownSchemesMutex sync.RWMutex

func RegisterScheme(scheme string, constructor func(path string) (Output, error)) {
	knownSchemesMutex.Lock()
	if _, exists := knownSchemes[scheme]; exists {
		log.Fatalf("scheme %#v already registered", scheme)
	}
	knownSchemes[scheme] = constructor
	knownSchemesMutex.Unlock()
}

type output struct {
	sync.RWMutex
	out       WriteSyncer
	closeable bool
	dsn       string
}

func New(dsn string) (Output, error) {
	o := &output{}

	err := o.apply(dsn)
	if err != nil {
		return nil, err
	}

	return o, err
}

func (o *output) apply(dsn string) error {
	if dsn == o.dsn && o.out != nil { // nothing changed
		return nil
	}

	var newOut WriteSyncer
	var newCloseable bool

	u, err := url.Parse(dsn)
	if err != nil {
		return err
	}

	if u.Path == "" || u.Path == "stderr" {
		newOut = os.Stderr
	} else if u.Path == "stdout" {
		newOut = os.Stdout
	} else {
		if u.Scheme == "" || u.Scheme == "file" {
			newOut, err = File(u.Path)
			if err != nil {
				return err
			}
			newCloseable = true
		} else {
			knownSchemesMutex.RLock()
			newFunc, exists := knownSchemes[u.Scheme]
			knownSchemesMutex.RUnlock()

			if !exists {
				return fmt.Errorf("unknown scheme %#v", u.Scheme)
			}

			newOut, err = newFunc(u.String())
			if err != nil {
				return err
			}
			if _, ok := newOut.(closeable); ok {
				newCloseable = true
			}
		}
	}

	if o.out != nil && o.closeable {
		if c, ok := o.out.(closeable); ok {
			c.Close()
		}
		o.out = nil
	}

	o.out = newOut
	o.closeable = newCloseable

	return nil
}

func (o *output) Sync() (err error) {
	o.RLock()
	if o.out != nil {
		err = o.out.Sync()
	}
	o.RUnlock()
	return
}

func (o *output) Write(p []byte) (n int, err error) {
	o.RLock()
	if o.out != nil {
		n, err = o.out.Write(p)
	}
	o.RUnlock()
	return
}
