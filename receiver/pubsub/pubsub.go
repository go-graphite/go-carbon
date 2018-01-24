package pubsub

import (
	"context"
	"fmt"
	"sync/atomic"

	"cloud.google.com/go/pubsub"
	"go.uber.org/zap"

	"github.com/lomik/go-carbon/helper"
	"github.com/lomik/go-carbon/points"
	"github.com/lomik/go-carbon/receiver"
	"github.com/lomik/go-carbon/receiver/parse"
	"github.com/lomik/zapwriter"
)

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
	Project      string `toml:"project"`
	Subscription string `toml:"subscription"`
}

// NewOptions returns Options struct filled with default values.
func NewOptions() *Options {
	return &Options{
		Project:      "",
		Subscription: "",
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

	var data []*points.Points
	var err error

	switch m.Attributes["content-type"] {
	case "application/python-pickle":
		data, err = parse.Pickle(m.Data)
	case "application/protobuf":
		data, err = parse.Protobuf(m.Data)
	default:
		data, err = parse.Plain(m.Data)
	}
	if err != nil {
		atomic.AddUint32(&rcv.errors, 1)
		return
	}
	cnt := 0
	for i := 0; i < len(data); i++ {
		cnt += len(data[i].Data)
		rcv.out(data[i])
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
