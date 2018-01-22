package kafka

import (
	"sync/atomic"

	"go.uber.org/zap"

	"encoding/json"
	"fmt"
	"io/ioutil"
	"strings"
	"sync"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/Shopify/sarama"
	"github.com/lomik/go-carbon/helper"
	"github.com/lomik/go-carbon/helper/atomicfiles"
	"github.com/lomik/go-carbon/points"
	"github.com/lomik/go-carbon/receiver"
	"github.com/lomik/go-carbon/receiver/parse"
	"github.com/lomik/zapwriter"
)

func init() {
	receiver.Register(
		"kafka",
		func() interface{} { return NewOptions() },
		func(name string, options interface{}, store func(*points.Points)) (receiver.Receiver, error) {
			return newKafka(name, options.(*Options), store)
		},
	)
}

// Offset is a special type to define kafka offsets. It's used to create custom marshal/unmarshal functions for configs.
type Offset int64

// MarshalText marshals offset. It's used to handle two special cases "newest" and "oldest".
func (o *Offset) MarshalText() ([]byte, error) {
	switch *o {
	case OffsetNewest:
		return []byte("newest"), nil
	case OffsetOldest:
		return []byte("oldest"), nil
	}
	return []byte(fmt.Sprintf("%v", *o)), nil
}

// UnmarshalText unmarshals text to offset. It handles "newest" and "oldest", oterwise fallbacks to time.ParseDuration.
func (o *Offset) UnmarshalText(text []byte) error {
	offsetName := string(text)

	switch strings.ToLower(offsetName) {
	case "newest":
		*o = OffsetNewest
	case "oldest":
		*o = OffsetOldest
	default:
		d, err := time.ParseDuration(offsetName)
		if err != nil {
			return err
		}
		*o = Offset(time.Now().Add(d).UnixNano())
	}
	return nil
}

var supportedProtocols = []string{"plain", "protobuf", "pickle"}

// Protocol is a special type to allow user to define wire protocol in Config file as a simple text.
type Protocol int

// MarshalText converts internal enum-like representation of protocol to a text
func (p *Protocol) MarshalText() ([]byte, error) {
	switch *p {
	case ProtocolPlain:
		return []byte("plain"), nil
	case ProtocolProtobuf:
		return []byte("protobuf"), nil
	case ProtocolPickle:
		return []byte("pickle"), nil
	}
	return nil, fmt.Errorf("Unsupported offset type %v, supported offsets: %v", p, supportedProtocols)
}

// UnmarshalText converts text from config file to a enum.
func (p *Protocol) UnmarshalText(text []byte) error {
	protocolName := string(text)

	switch strings.ToLower(protocolName) {
	case "plain":
		*p = ProtocolPlain
	case "protobuf":
		*p = ProtocolProtobuf
	case "pickle":
		*p = ProtocolPickle
	default:
		return fmt.Errorf("Unsupported protocol type %v, supported: %v", protocolName, supportedProtocols)
	}
	return nil
}

// ToString returns text representation of current protocol
func (p *Protocol) ToString() string {
	switch *p {
	case ProtocolPlain:
		return "plain"
	case ProtocolProtobuf:
		return "protobuf"
	case ProtocolPickle:
		return "pickle"
	}
	return "unsupported"
}

const (
	// OffsetOldest represents oldest offset in Kafka
	OffsetOldest Offset = -1
	// OffsetNewest represents newest offset in Kafka
	OffsetNewest = -2

	// ProtocolPlain represents graphite line protocol
	ProtocolPlain Protocol = 0
	// ProtocolProtobuf represents protobuf messages
	ProtocolProtobuf = 1
	// ProtocolPickle represents pickled messages
	ProtocolPickle = 2
)

// Duration wrapper time.Duration for TOML
type Duration struct {
	time.Duration
}

var _ toml.TextMarshaler = &Duration{}

// UnmarshalText from TOML
func (d *Duration) UnmarshalText(text []byte) error {
	var err error
	d.Duration, err = time.ParseDuration(string(text))
	return err
}

// MarshalText encode text with TOML format
func (d *Duration) MarshalText() ([]byte, error) {
	return []byte(d.Duration.String()), nil
}

// Options contains all receiver's options that can be changed by user
type Options struct {
	Brokers           []string  `toml:"brokers"`
	Topic             string    `toml:"topic"`
	Partition         int32     `toml:"partition"`
	Protocol          Protocol  `toml:"parse-protocol"`
	StateFile         string    `toml:"state-file"`
	InitialOffset     Offset    `toml:"initial-offset"`
	StateSaveInterval *Duration `toml:"state-save-interval"`
	ReconnectInterval *Duration `toml:"reconnect-interval"`
	FetchInterval     *Duration `toml:"fetch-interval"`
	KafkaVersion      string    `toml:"kafka-version"`
}

// NewOptions returns Options struct filled with default values.
func NewOptions() *Options {
	return &Options{
		Brokers:           []string{"localhost:9092"},
		Topic:             "graphite",
		Partition:         0,
		Protocol:          ProtocolPlain,
		InitialOffset:     OffsetOldest,
		StateSaveInterval: &Duration{Duration: 60 * time.Second},
		ReconnectInterval: &Duration{Duration: 60 * time.Second},
		FetchInterval:     &Duration{Duration: 250 * time.Millisecond},
		KafkaVersion:      "0.11.0.0",
	}
}

type state struct {
	Offset int64

	stateFile         string
	offsetIsTimestamp bool
}

type optionsKafka struct {
	brokers       []string
	topic         string
	partition     int32
	initialOffset Offset
}

// Kafka receive metrics in protobuf or graphite line format from Kafka partitions
type Kafka struct {
	sync.RWMutex
	out             func(*points.Points)
	name            string // name for store metrics
	metricsReceived uint64
	errors          uint64

	waitGroup         sync.WaitGroup
	connectOptions    optionsKafka
	kafkaState        *state
	stateSaveInterval time.Duration
	reconnectInterval time.Duration
	fetchInterval     time.Duration
	consumer          sarama.PartitionConsumer
	logger            *zap.Logger
	workerClosed      chan struct{}
	closed            chan struct{}
	forceReconnect    chan struct{}
	protocol          Protocol
	statsAsCounters   bool
	version           sarama.KafkaVersion
}

func (s *state) SaveState() error {
	offset := atomic.LoadInt64(&s.Offset)
	newState := state{
		Offset: offset,
	}

	data, err := json.Marshal(&newState)
	if err != nil {
		return err
	}

	err = atomicfiles.WriteFile(s.stateFile, data)
	if err != nil {
		return err
	}
	return nil
}

func (s *state) LoadState() error {
	data, err := ioutil.ReadFile(s.stateFile)
	if err == nil {
		err = json.Unmarshal(data, s)
	}

	return err
}

func newKafka(name string, options *Options, store func(*points.Points)) (*Kafka, error) {
	logger := zapwriter.Logger(name)
	state := &state{
		Offset: 0,

		offsetIsTimestamp: false,
		stateFile:         options.StateFile,
	}

	err := state.LoadState()
	if err != nil {
		logger.Warn("failed to load queue state, falling back to defaults",
			zap.Int64("offset", int64(options.InitialOffset)),
			zap.Error(err),
		)

		switch options.InitialOffset {
		case OffsetOldest:
			state.Offset = sarama.OffsetOldest
		case OffsetNewest:
			state.Offset = sarama.OffsetNewest
		default:
			state.Offset = int64(options.InitialOffset)
			state.offsetIsTimestamp = true
		}
	} else {
		logger.Info("previous state loaded",
			zap.Int64("offset", state.Offset),
		)
	}
	ver, err := sarama.ParseKafkaVersion(options.KafkaVersion)
	if err != nil {
		logger.Error("invalid kafka version",
			zap.String("kafkaVersion", options.KafkaVersion),
			zap.Error(err),
		)
		return nil, err
	}

	rcv := &Kafka{
		out:               store,
		name:              name,
		protocol:          options.Protocol,
		consumer:          nil,
		logger:            logger,
		workerClosed:      make(chan struct{}),
		closed:            make(chan struct{}),
		forceReconnect:    make(chan struct{}),
		fetchInterval:     options.FetchInterval.Duration,
		kafkaState:        state,
		stateSaveInterval: options.StateSaveInterval.Duration,
		reconnectInterval: options.ReconnectInterval.Duration,
		connectOptions: optionsKafka{
			brokers:       options.Brokers,
			topic:         options.Topic,
			partition:     options.Partition,
			initialOffset: options.InitialOffset,
		},
		version: ver,
	}

	go func() {
		rcv.waitGroup.Add(1)
		rcv.connect()
		rcv.waitGroup.Done()
	}()
	rcv.forceReconnect <- struct{}{}

	return rcv, nil
}

func (rcv *Kafka) consume() {
	rcv.Lock()
	defer rcv.Unlock()
	if rcv.consumer != nil {
		return
	}
	rcv.logger.Info("connecting to kafka")

	consumerConfig := sarama.NewConfig()
	consumerConfig.Version = rcv.version
	client, err := sarama.NewClient(rcv.connectOptions.brokers, consumerConfig)
	if err != nil {
		rcv.logger.Error("failed to connect to kafka",
			zap.Duration("reconnect_interval", rcv.reconnectInterval),
			zap.Error(err),
		)
		return
	}

	if rcv.kafkaState.offsetIsTimestamp {
		rcv.kafkaState.offsetIsTimestamp = false
		offset, err := client.GetOffset(rcv.connectOptions.topic, rcv.connectOptions.partition, rcv.kafkaState.Offset)
		if err != nil {
			rcv.logger.Error("failed to get offset, falling back to 'oldest'",
				zap.Error(err),
			)
			rcv.kafkaState.Offset = sarama.OffsetOldest
		} else {
			rcv.logger.Info("got offset for timestamp",
				zap.Int64("timestamp_ns", rcv.kafkaState.Offset),
				zap.Int64("offset", offset),
			)
			rcv.kafkaState.Offset = offset
		}
	}

	consumer, err := sarama.NewConsumerFromClient(client)
	if err != nil {
		rcv.logger.Error("failed to connect to kafka",
			zap.Duration("reconnect_interval", rcv.reconnectInterval),
			zap.Error(err),
		)
		return
	}

	partitionConsumer, err := consumer.ConsumePartition(rcv.connectOptions.topic, rcv.connectOptions.partition, rcv.kafkaState.Offset)
	if err != nil {
		rcv.logger.Error("failed to connect to kafka",
			zap.Duration("reconnect_interval", rcv.reconnectInterval),
			zap.Error(err),
		)
		return
	}
	rcv.consumer = partitionConsumer

	// Stop old worker
	close(rcv.workerClosed)
	rcv.workerClosed = make(chan struct{})

	rcv.logger.Info("connected to kafka")

	go func() {
		rcv.waitGroup.Add(1)
		rcv.worker()
		rcv.waitGroup.Done()
	}()
}

func (rcv *Kafka) connect() {
	for {
		reconnectTimer := time.NewTicker(rcv.reconnectInterval)
		select {
		case <-rcv.closed:
			close(rcv.workerClosed)
			reconnectTimer.Stop()
			return
		case <-reconnectTimer.C:
			rcv.consume()
		case <-rcv.forceReconnect:
			rcv.logger.Info("reconnect forced")
			rcv.consume()
		}
	}
}

// Stop stops kafka receiver. It will return when all goroutines finish their work.
func (rcv *Kafka) Stop() {
	close(rcv.closed)
	rcv.waitGroup.Wait()
}

// Stat sends kafka receiver's internal stats to specified callback
func (rcv *Kafka) Stat(send helper.StatCallback) {
	metricsReceived := atomic.LoadUint64(&rcv.metricsReceived)
	send("metricsReceived", float64(metricsReceived))

	errors := atomic.LoadUint64(&rcv.errors)
	send("errors", float64(errors))

	if !rcv.statsAsCounters {
		atomic.AddUint64(&rcv.metricsReceived, -metricsReceived)
		atomic.AddUint64(&rcv.errors, -errors)
	}
}

func (rcv *Kafka) saveState() {
	err := rcv.kafkaState.SaveState()
	if err != nil {
		rcv.logger.Error("error saving receiver state to file",
			zap.Error(err),
			zap.String("receiver_type", "kafka"),
			zap.String("receiver_name", rcv.name),
		)
	} else {
		rcv.logger.Debug("Kafka state saved",
			zap.Int64("offset", rcv.kafkaState.Offset),
		)
	}
}

func (rcv *Kafka) worker() {
	saveTimer := time.NewTicker(rcv.stateSaveInterval)
	fetchTimer := time.NewTicker(rcv.fetchInterval)
	rcv.logger.Info("Worker started")
	protocolParser := parse.Plain
	switch rcv.protocol {
	case ProtocolProtobuf:
		protocolParser = parse.Protobuf
	case ProtocolPlain:
		protocolParser = parse.Plain
	case ProtocolPickle:
		protocolParser = parse.Pickle
	}
	for {
		select {
		case <-rcv.closed:
			saveTimer.Stop()
			rcv.saveState()
			err := rcv.consumer.Close()
			if err != nil {
				rcv.logger.Error("failed to close consumer",
					zap.Error(err),
				)
			}
			return
		case <-saveTimer.C:
			rcv.saveState()
		case <-fetchTimer.C:
			rcv.Lock()
			var payload []*points.Points
			var err error
			msgChan := rcv.consumer.Messages()
			for {
				messageReceived := true
				select {
				case msg := <-msgChan:
					payload, err = protocolParser(msg.Value)

					if err != nil {
						atomic.AddUint64(&rcv.errors, 1)
						rcv.logger.Error("failed to parse message",
							zap.String("protocol", rcv.protocol.ToString()),
							zap.Error(err),
						)
						continue
					}

					metricsReceived := 0
					for _, p := range payload {
						metricsReceived += len(p.Data)
						rcv.out(p)
					}

					atomic.StoreInt64(&rcv.kafkaState.Offset, msg.Offset)
					atomic.AddUint64(&rcv.metricsReceived, uint64(metricsReceived))
				default:
					messageReceived = false
				}

				if !messageReceived {
					break
				}
			}
			rcv.Unlock()
		}
	}
}
