//go:build !unix

package handoff

import (
	"errors"
	"net"
	"time"
)

const Protocol = "go-carbon-read-handoff/1"

var RegisterTimeout = 10 * time.Second

func SuccessorSupported(string) bool { return false }

type Offer struct{}

func NewOffer(string) (*Offer, error) { return nil, errors.ErrUnsupported }
func (*Offer) Close() error           { return nil }
func (*Offer) Serve(time.Duration, *net.TCPListener, func()) (bool, error) {
	return false, errors.ErrUnsupported
}

type Claim struct{}

func Register(string) *Claim { return nil }
func (*Claim) Close() error  { return nil }
func (*Claim) Take(*net.TCPAddr) (net.Listener, func() error, error) {
	return nil, nil, errors.ErrUnsupported
}
