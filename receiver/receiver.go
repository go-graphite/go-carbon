package receiver

import (
	"fmt"
	"net"
	"net/url"

	"github.com/lomik/go-carbon/cache"
	"github.com/lomik/go-carbon/points"
)

type Receiver interface {
	Stop()
	Name() string
	Stat(func(metric string, value float64))
}

type Option func(Receiver) error

// PickleMaxMessageSize creates option for New contructor
func PickleMaxMessageSize(size uint32) Option {
	return func(r Receiver) error {
		if t, ok := r.(*TCP); ok {
			t.maxPickleMessageSize = size
		}
		return nil
	}
}

// UDPLogIncomplete creates option for New contructor
func UDPLogIncomplete(enable bool) Option {
	return func(r Receiver) error {
		if t, ok := r.(*UDP); ok {
			t.logIncomplete = enable
		}
		return nil
	}
}

// Name creates option for New contructor
func Name(name string) Option {
	return func(r Receiver) error {
		if t, ok := r.(*TCP); ok {
			t.name = name
		}
		if t, ok := r.(*UDP); ok {
			t.name = name
		}
		return nil
	}
}

func blackhole(p *points.Points) {}

// New creates udp, tcp, pickle receiver
func New(dsn string, c *cache.Cache, opts ...Option) (Receiver, error) {
	u, err := url.Parse(dsn)
	if err != nil {
		return nil, err
	}

	if u.Scheme == "tcp" || u.Scheme == "pickle" {
		addr, err := net.ResolveTCPAddr("tcp", u.Host)
		if err != nil {
			return nil, err
		}

		r := &TCP{
			cache: c,
			name:  u.Scheme,
		}

		if u.Scheme == "pickle" {
			r.isPickle = true
			r.maxPickleMessageSize = 67108864 // 64Mb
		}

		for _, optApply := range opts {
			optApply(r)
		}

		if err = r.Listen(addr); err != nil {
			return nil, err
		}

		return r, err
	}

	if u.Scheme == "udp" {
		addr, err := net.ResolveUDPAddr("udp", u.Host)
		if err != nil {
			return nil, err
		}

		r := &UDP{
			cache: c,
			name:  u.Scheme,
		}

		for _, optApply := range opts {
			optApply(r)
		}

		err = r.Listen(addr)
		if err != nil {
			return nil, err
		}

		return r, err
	}

	return nil, fmt.Errorf("unknown proto %#v", u.Scheme)
}
