//go:build !unix

package handoff

import (
	"errors"
	"net"
	"os"
	"time"
)

const Protocol = "go-carbon-read-handoff/1"

const InputProtocol = "go-carbon-input-handoff/1"

var RegisterTimeout = 10 * time.Second

func SuccessorSupported(string) bool { return false }
func InputSupported(string) bool     { return false }

type Input struct{ Receivers []InputReceiver }

type InputReceiver struct {
	Name     string
	Listener *os.File
	Conns    []InputConn
}

type InputConn struct {
	File     *os.File
	Leftover []byte
}

func (*Input) Close() {}

type InputSource interface {
	Detach() (*Input, error)
}

type Offer struct{}

func NewOffer(string) (*Offer, error) { return nil, errors.ErrUnsupported }
func (*Offer) Close() error           { return nil }
func (*Offer) Serve(time.Duration, *net.TCPListener, func(), InputSource, time.Duration) (bool, error) {
	return false, errors.ErrUnsupported
}

type Claim struct{}

func Register(string) *Claim { return nil }
func (*Claim) Close() error  { return nil }
func (*Claim) Take(*net.TCPAddr) (net.Listener, func() error, error) {
	return nil, nil, errors.ErrUnsupported
}
func (*Claim) InputOffered() bool                { return false }
func (*Claim) TakeInput() (*Input, error)        { return nil, nil }
func LockExclusive(string) (func() error, error) { return func() error { return nil }, nil }
func LockShared(string) (func() error, error)    { return func() error { return nil }, nil }
func Locked(string) bool                         { return false }
