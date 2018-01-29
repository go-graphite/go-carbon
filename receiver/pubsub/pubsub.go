package pubsub

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"sync"
	"sync/atomic"

	"cloud.google.com/go/pubsub"
	"go.uber.org/zap"

	"github.com/lomik/go-carbon/helper"
	"github.com/lomik/go-carbon/points"
	"github.com/lomik/go-carbon/receiver"
	"github.com/lomik/go-carbon/receiver/parse"
	"github.com/lomik/zapwriter"
)

// gzipPool provides a sync.Pool of initialized gzip.Readers's to avoid
// the allocation overhead of repeatedly calling gzip.NewReader
var gzipPool sync.Pool

func init() {
	receiver.Register(
		"pubsub",
		func() interface{} { return NewOptions() },
		func(name string, options interface{}, store func(*points.Points)) (receiver.Receiver, error) {
			return newPubSub(nil, name, options.(*Options), store)
		},
	)
}

// Options contains all receiver's options that can be changed by user
type Options struct {
	Project             string `toml:"project"`
	Subscription        string `toml:"subscription"`
	ReceiverGoRoutines  int    `toml:"receiver_go_routines"`
	ReceiverMaxMessages int    `toml:"receiver_max_messages"`
	ReceiverMaxBytes    int    `toml:"receiver_max_bytes"`
}

// NewOptions returns Options struct filled with default values.
func NewOptions() *Options {
	return &Options{
		Project:             "",
		Subscription:        "",
		ReceiverGoRoutines:  4,
		ReceiverMaxMessages: 1000,
		ReceiverMaxBytes:    500e6, // 500MB
	}
}

// PubSub receive metrics from a google pubsub subscription
type PubSub struct {
	out              func(*points.Points)
	name             string
	client           *pubsub.Client
	subscription     *pubsub.Subscription
	cancel           context.CancelFunc
	messagesReceived uint32
	metricsReceived  uint32
	errors           uint32
	logger           *zap.Logger
	closed           chan struct{}
	statsAsCounters  bool
}

// newPubSub returns a PubSub receiver. Optionally accepts a client to allow
// for injecting the fake for tests. If client is nil a real
// client connection to the google pubsub service will be attempted.
func newPubSub(client *pubsub.Client, name string, options *Options, store func(*points.Points)) (*PubSub, error) {
	logger := zapwriter.Logger(name)
	logger.Info("starting google pubsub receiver",
		zap.String("project", options.Project),
		zap.String("subscription", options.Subscription),
	)

	if options.Project == "" {
		return nil, fmt.Errorf("'project' must be specified")
	}

	ctx := context.Background()
	if client == nil {
		c, err := pubsub.NewClient(ctx, options.Project)
		if err != nil {
			return nil, err
		}
		client = c
	}

	sub := client.Subscription(options.Subscription)
	exists, err := sub.Exists(ctx)
	if err != nil {
		return nil, err
	}
	if !exists {
		// TODO: try to create subscription
		return nil, fmt.Errorf("subscription %s in project %s does not exist", options.Subscription, options.Project)
	}

	if options.ReceiverGoRoutines != 0 {
		sub.ReceiveSettings.NumGoroutines = options.ReceiverGoRoutines
	}
	if options.ReceiverMaxBytes != 0 {
		sub.ReceiveSettings.MaxOutstandingBytes = options.ReceiverMaxBytes
	}
	if options.ReceiverMaxMessages != 0 {
		sub.ReceiveSettings.MaxOutstandingMessages = options.ReceiverMaxMessages
	}

	// cancel() will be called to signal the subscription Receive() goroutines to finish and shutdown
	cctx, cancel := context.WithCancel(ctx)

	rcv := &PubSub{
		out:          store,
		name:         name,
		client:       client,
		cancel:       cancel,
		subscription: sub,
		logger:       logger,
		closed:       make(chan struct{}),
	}

	go func() {
		err := rcv.subscription.Receive(cctx, func(ctx context.Context, m *pubsub.Message) {
			rcv.handleMessage(m)
			m.Ack()
		})
		if err != nil {
			rcv.logger.Error(err.Error())
		}
		close(rcv.closed)
	}()

	return rcv, nil
}

func (rcv *PubSub) handleMessage(m *pubsub.Message) {
	atomic.AddUint32(&rcv.messagesReceived, 1)

	var data []byte
	var err error
	var points []*points.Points

	switch m.Attributes["codec"] {
	case "gzip":
		gzr, err := acquireGzipReader(bytes.NewBuffer(m.Data))
		if err != nil {
			rcv.logger.Error(err.Error())
			atomic.AddUint32(&rcv.errors, 1)
			return
		}
		defer releaseGzipReader(gzr)

		data, err = ioutil.ReadAll(gzr)
		if err != nil {
			rcv.logger.Error(err.Error())
			atomic.AddUint32(&rcv.errors, 1)
			return
		}
	default:
		// "none", no compression
		data = m.Data
	}

	switch m.Attributes["content-type"] {
	case "application/python-pickle":
		points, err = parse.Pickle(data)
	case "application/protobuf":
		points, err = parse.Protobuf(data)
	default:
		points, err = parse.Plain(data)
	}
	if err != nil {
		atomic.AddUint32(&rcv.errors, 1)
		rcv.logger.Error(err.Error())
	}
	if len(points) == 0 {
		return
	}

	cnt := 0
	for i := 0; i < len(points); i++ {
		cnt += len(points[i].Data)
		rcv.out(points[i])
	}
	atomic.AddUint32(&rcv.metricsReceived, uint32(cnt))
}

// Stop shuts down the pubsub receiver and waits until all message processing is completed
// before returning
func (rcv *PubSub) Stop() {
	rcv.cancel()
	<-rcv.closed
}

// Stat sends pubsub receiver's internal stats to specified callback
func (rcv *PubSub) Stat(send helper.StatCallback) {
	messagesReceived := atomic.LoadUint32(&rcv.messagesReceived)
	send("messagesReceived", float64(messagesReceived))

	metricsReceived := atomic.LoadUint32(&rcv.metricsReceived)
	send("metricsReceived", float64(metricsReceived))

	errors := atomic.LoadUint32(&rcv.errors)
	send("errors", float64(errors))

	if !rcv.statsAsCounters {
		atomic.AddUint32(&rcv.messagesReceived, -messagesReceived)
		atomic.AddUint32(&rcv.metricsReceived, -metricsReceived)
		atomic.AddUint32(&rcv.errors, -errors)
	}
}

// acquireGzipReader retrieves a (possibly) pre-initialized gzip.Reader from
// the package gzipPool (sync.Pool). This reduces memory allocation overhead by re-using
// gzip.Readers
func acquireGzipReader(r io.Reader) (*gzip.Reader, error) {
	v := gzipPool.Get()
	if v == nil {
		return gzip.NewReader(r)
	}
	zr := v.(*gzip.Reader)
	if err := zr.Reset(r); err != nil {
		return nil, err
	}
	return zr, nil
}

// releaseGzipReader returns a gzip.Reader to the package gzipPool
func releaseGzipReader(zr *gzip.Reader) {
	zr.Close()
	gzipPool.Put(zr)
}
