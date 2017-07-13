package http

import (
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"sync/atomic"
	"time"

	"go.uber.org/zap"

	"github.com/lomik/go-carbon/helper"
	"github.com/lomik/go-carbon/points"
	"github.com/lomik/go-carbon/receiver"
	"github.com/lomik/go-carbon/receiver/parse"
	"github.com/lomik/zapwriter"
)

func init() {
	receiver.Register(
		"http",
		func() interface{} { return NewOptions() },
		func(name string, options interface{}, store func(*points.Points)) (receiver.Receiver, error) {
			return newHTTP(name, options.(*Options), store)
		},
	)
}

type Options struct {
	Listen         string `toml:"listen"`
	MaxMessageSize uint32 `toml:"max-message-size"`
}

func NewOptions() *Options {
	return &Options{
		Listen:         ":2007",
		MaxMessageSize: 67108864, // 64 Mb
	}
}

// HTTP receive metrics from HTTP requests
type HTTP struct {
	out             func(*points.Points)
	name            string // name for store metrics
	maxMessageSize  uint32
	metricsReceived uint32
	errors          uint32
	listener        *net.TCPListener
	server          *http.Server
	logger          *zap.Logger
	closed          chan struct{}
}

// Addr returns binded socket address. For bind port 0 in tests
func (rcv *HTTP) Addr() net.Addr {
	if rcv.listener == nil {
		return nil
	}
	return rcv.listener.Addr()
}

func newHTTP(name string, options *Options, store func(*points.Points)) (*HTTP, error) {

	addr, err := net.ResolveTCPAddr("tcp", options.Listen)
	if err != nil {
		return nil, err
	}

	tcpListener, err := net.ListenTCP("tcp", addr)
	if err != nil {
		return nil, err
	}

	rcv := &HTTP{
		out:            store,
		name:           name,
		maxMessageSize: options.MaxMessageSize,
		logger:         zapwriter.Logger(name),
		listener:       tcpListener,
		closed:         make(chan struct{}),
	}

	s := &http.Server{
		Addr:           options.Listen,
		Handler:        rcv,
		ReadTimeout:    10 * time.Second,
		WriteTimeout:   10 * time.Second,
		MaxHeaderBytes: 1 << 20,
	}

	rcv.server = s

	go func() {
		s.Serve(tcpListener)
		close(rcv.closed)
	}()

	return rcv, err
}

func (rcv *HTTP) Stop() {
	rcv.listener.Close()
	rcv.server.Close()
	<-rcv.closed
}

func (rcv *HTTP) Stat(send helper.StatCallback) {
	metricsReceived := atomic.LoadUint32(&rcv.metricsReceived)
	atomic.AddUint32(&rcv.metricsReceived, -metricsReceived)
	send("metricsReceived", float64(metricsReceived))

	errors := atomic.LoadUint32(&rcv.errors)
	atomic.AddUint32(&rcv.errors, -errors)
	send("errors", float64(errors))
}

func (rcv *HTTP) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		atomic.AddUint32(&rcv.errors, 1)
		http.Error(w, fmt.Sprintf("Method %#v is not supported", r.Method), http.StatusBadRequest)
		return
	}

	if r.ContentLength > int64(rcv.maxMessageSize) {
		atomic.AddUint32(&rcv.errors, 1)
		http.Error(w, fmt.Sprintf("Message too long. Max allowed message size is %#v", rcv.maxMessageSize), http.StatusBadRequest)
		return
	}

	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		atomic.AddUint32(&rcv.errors, 1)
		http.Error(w, fmt.Sprintf("Read request failed: %s", err.Error()), http.StatusBadRequest)
		return
	}

	var data []*points.Points

	switch r.Header.Get("Content-Type") {
	case "application/python-pickle":
		data, err = parse.Pickle(body)
	case "application/protobuf":
		data, err = parse.Protobuf(body)
	default:
		data, err = parse.Plain(body)
	}

	if err != nil {
		atomic.AddUint32(&rcv.errors, 1)
		http.Error(w, "Parse failed", http.StatusBadRequest)
		return
	}

	cnt := 0
	for i := 0; i < len(data); i++ {
		cnt += len(data[i].Data)
		rcv.out(data[i])
	}

	atomic.AddUint32(&rcv.metricsReceived, uint32(cnt))
}
