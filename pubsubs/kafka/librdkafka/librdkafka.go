// Package librdkafka implements a kafka-pubsub provider ontop of the confluent kafka-go library which heavily relies on
// cgo and the c librdkafka library. It's less performant that compared to the pure go version samsara, implemented by
// shopify but still is very able.
package librdkafka

import (
	"context"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/gokit/actorkit"
	"github.com/gokit/actorkit/pubsubs"
	"github.com/gokit/errors"
	"github.com/gokit/xid"
)

const (
	idParam = "kafkaMessageID"
)

var (
	_ Marshaler   = &MarshalerWrapper{}
	_ Unmarshaler = &UnmarshalerWrapper{}
)

// Marshaler defines a interface exposing method to transform a pubsubs.Message
// into a kafka message.
type Marshaler interface {
	Marshal(message pubsubs.Message) (kafka.Message, error)
}

// Unmarshaler defines an interface who's implementer exposes said method to
// transform a kafka message into a pubsubs Message.
type Unmarshaler interface {
	Unmarshal(*kafka.Message) (pubsubs.Message, error)
}

// MarshalerWrapper implements the Marshaler interface.
type MarshalerWrapper struct {
	Envelope pubsubs.Marshaler

	// Partitioner takes giving message returning appropriate
	// partition name to be used for kafka message.
	Partitioner func(pubsubs.Message) string
}

// Marshal implements the Marshaler interface.
func (kc MarshalerWrapper) Marshal(message pubsubs.Message) (kafka.Message, error) {
	envelopeBytes, err := kc.Envelope.Marshal(message.Envelope)
	if err != nil {
		return kafka.Message{}, err
	}

	attrs := make([]kafka.Header, 0, len(message.Envelope.Header)+1)
	attrs = append(attrs, kafka.Header{Key: idParam, Value: message.Envelope.Ref.Bytes()})

	for k, v := range message.Envelope.Header {
		attrs = append(attrs, kafka.Header{Key: k, Value: []byte(v)})
	}

	var key []byte
	if kc.Partitioner != nil {
		key = []byte(kc.Partitioner(message))
	}

	return kafka.Message{
		Key:     key,
		Headers: attrs,
		Value:   envelopeBytes,
		TopicPartition: kafka.TopicPartition{
			Topic:     &message.Topic,
			Partition: kafka.PartitionAny,
		},
	}, nil
}

// UnmarshalerWrapper implements the Unmarshaler interface.
type UnmarshalerWrapper struct {
	Envelope pubsubs.Unmarshaler
}

// Unmarshal implements the Unmarshaler interface.
func (kc UnmarshalerWrapper) Unmarshal(message *kafka.Message) (pubsubs.Message, error) {
	var msg pubsubs.Message
	msg.Topic = *message.TopicPartition.Topic

	var err error
	if msg.Envelope, err = kc.Envelope.Unmarshal(message.Value); err != nil {
		return msg, err
	}

	// confirm header id matches envelope id
	for _, v := range message.Headers {
		switch v.Key {
		case idParam:
			if string(v.Value) != msg.Envelope.Ref.String() {
				return msg, errors.New("Kafka message ID does not matched envelope data")
			}
		default:
			if !msg.Envelope.Has(v.Key) {
				msg.Envelope.Header[v.Key] = string(v.Value)
			}
		}
	}

	return msg, nil
}

//*****************************************************************************
// PubSubFactory
//*****************************************************************************

// PublisherHandler defines a function type which takes a giving PublisherConsumerFactory
// and a given topic, returning a new publisher with all related underline specific
// details added and instantiated.
type PublisherHandler func(*PublisherConsumerFactory, string) (pubsubs.Publisher, error)

// SubscriberHandler defines a function type which takes a giving SubscriptionFactory
// and a given topic, returning a new subscription with all related underline specific
// details added and instantiated.
type SubscriberHandler func(*PublisherConsumerFactory, string, string, pubsubs.Receiver) (pubsubs.Subscription, error)

// GroupSubscriberHandler defines a function returning a subscription to a topic for a giving queue group.
type GroupSubscriberHandler func(*PublisherConsumerFactory, string, string, string, pubsubs.Receiver) (pubsubs.Subscription, error)

// PubSubFactoryGenerator returns a function which taken a PublisherSubscriberFactory returning
// a factory for generating publishers and subscribers.
type PubSubFactoryGenerator func(*PublisherConsumerFactory) pubsubs.PubSubFactory

// PubSubFactory provides a partial function for the generation of a pubsub.PubSubFactory
// using the PubSubFactorGenerator function.
func PubSubFactory(publishers PublisherHandler, subscribers SubscriberHandler, groupSubscribers GroupSubscriberHandler) PubSubFactoryGenerator {
	return func(pbs *PublisherConsumerFactory) pubsubs.PubSubFactory {
		var factory pubsubs.PubSubFactoryImpl
		if publishers != nil {
			factory.Publishers = func(topic string) (pubsubs.Publisher, error) {
				return publishers(pbs, topic)
			}
		}
		if subscribers != nil {
			factory.Subscribers = func(topic string, id string, receiver pubsubs.Receiver) (pubsubs.Subscription, error) {
				return subscribers(pbs, topic, id, receiver)
			}
		}
		if groupSubscribers != nil {
			factory.QueueGroupSubscribers = func(group string, topic string, id string, r pubsubs.Receiver) (pubsubs.Subscription, error) {
				return groupSubscribers(pbs, group, topic, id, r)
			}
		}
		return &factory
	}
}

//****************************************************************************
// Kafka ConsumerFactor
//****************************************************************************

// Config defines configuration fields for use with a PublisherConsumerFactory.
type Config struct {
	Brokers              []string
	ConsumersCount       int
	NoConsumerGroup      bool
	ProjectID            string
	AutoOffsetReset      string
	DefaultConsumerGroup string

	// Marshaler provides the marshaler to be used for serializing messages through the
	// delivery mechanism.
	Marshaler Marshaler

	// Unmarshaler provides the underline unmarshaler to be used for deserializing messages
	// through the delivery mechanism.
	Unmarshaler Unmarshaler

	// Log provides the logging provider for delivery operational logs.
	Log actorkit.Logs

	// PollingTime is the time to be used for polling the underline driver for messages.
	PollingTime time.Duration

	// MessageDeliveryTimeout is the timeout to wait before response
	// from the underline message broker before timeout.
	MessageDeliveryTimeout time.Duration

	// ConsumerOverrides is the Sarama.Config to be used for consumers.
	ConsumerOverrides kafka.ConfigMap

	// ProducerOverrides is the Sarama.Config to be used for producers to override the default.
	ProducerOverrides kafka.ConfigMap
}

func (c *Config) init() error {
	if len(c.Brokers) == 0 {
		return errors.New("missing brokers")
	}
	if c.Log == nil {
		c.Log = &actorkit.DrainLog{}
	}
	if c.ProjectID == "" {
		c.ProjectID = actorkit.PackageName
	}
	if c.MessageDeliveryTimeout <= 0 {
		c.MessageDeliveryTimeout = 2 * time.Second
	}
	if c.ConsumersCount == 0 {
		c.ConsumersCount = runtime.NumCPU()
	}
	if c.AutoOffsetReset == "" {
		c.AutoOffsetReset = "latest"
	}
	if c.DefaultConsumerGroup == "" {
		c.NoConsumerGroup = true
	}
	return nil
}

// PublisherConsumerFactory implements a central factory for creating publishers or consumers for
// topics for a underline kafka infrastructure.
type PublisherConsumerFactory struct {
	config Config

	waiter       sync.WaitGroup
	rootContext  context.Context
	rootCanceler func()

	cl        sync.RWMutex
	consumers map[string]*Consumer
}

// NewPublisherConsumerFactory returns a new instance of a PublisherConsumerFactory.
func NewPublisherConsumerFactory(ctx context.Context, config Config) (*PublisherConsumerFactory, error) {
	if err := config.init(); err != nil {
		return nil, err
	}

	rctx, cano := context.WithCancel(ctx)
	return &PublisherConsumerFactory{
		config:       config,
		rootContext:  rctx,
		rootCanceler: cano,
		consumers:    map[string]*Consumer{},
	}, nil
}

// Wait blocks till all consumers generated by giving factory are closed.
func (ka *PublisherConsumerFactory) Wait() {
	ka.waiter.Wait()
}

// Close closes all Consumers generated by consumer factory.
func (ka *PublisherConsumerFactory) Close() error {
	ka.rootCanceler()
	ka.waiter.Wait()
	return nil
}

// NewPublisher returns a new Publisher for a giving topic,
func (ka *PublisherConsumerFactory) NewPublisher(topic string, userOverrides *kafka.ConfigMap) (*Publisher, error) {
	base, err := generateProducerConfig(&ka.config)
	if err != nil {
		return nil, err
	}

	if userOverrides != nil {
		if err := mergeConfluentConfigs(&base, userOverrides); err != nil {
			return nil, err
		}
	}

	return NewPublisher(ka.rootContext, &ka.config, base, topic)
}

// NewGroupConsumer return a new consumer for a giving topic within a giving group to be used for kafka.
// The provided id value if not empty will be used as the group.id.
func (ka *PublisherConsumerFactory) NewGroupConsumer(topic string, group string, id string, receiver pubsubs.Receiver) (*Consumer, error) {
	if topic == "" {
		return nil, errors.New("topic value can not be empty")
	}

	if id == "" {
		return nil, errors.New("id value can not be empty")
	}

	if group == "" {
		return nil, errors.New("group value can not be empty")
	}

	consumer, err := NewConsumer(ka.rootContext, &ka.config, group, id, topic, receiver)
	if err != nil {
		return nil, err
	}

	errRes := make(chan error, 1)

	ka.waiter.Add(1)
	go func() {
		defer ka.waiter.Done()
		errRes <- consumer.Consume()

		consumer.Wait()
	}()

	if err = <-errRes; err != nil {
		return nil, err
	}

	ka.cl.Lock()
	ka.consumers[topic] = consumer
	ka.cl.Unlock()
	return consumer, nil
}

// NewConsumer return a new consumer for a giving topic to be used for kafka.
// The provided id value if not empty will be used as the group.id.
func (ka *PublisherConsumerFactory) NewConsumer(topic string, id string, receiver pubsubs.Receiver) (*Consumer, error) {
	if topic == "" {
		return nil, errors.New("topic value can not be empty")
	}

	if id == "" {
		return nil, errors.New("id value can not be empty")
	}

	consumer, err := NewConsumer(ka.rootContext, &ka.config, "", id, topic, receiver)
	if err != nil {
		return nil, err
	}

	errRes := make(chan error, 1)

	ka.waiter.Add(1)
	go func() {
		defer ka.waiter.Done()
		errRes <- consumer.Consume()

		consumer.Wait()
	}()

	if err = <-errRes; err != nil {
		return nil, err
	}

	ka.cl.Lock()
	ka.consumers[topic] = consumer
	ka.cl.Unlock()
	return consumer, nil
}

func (ka *PublisherConsumerFactory) hasConsumer(topic string) bool {
	ka.cl.RLock()
	defer ka.cl.RUnlock()
	_, ok := ka.consumers[topic]
	return ok
}

//****************************************************************************
// Kafka Consumer
//****************************************************************************

// Consumer implements a Kafka message subscription consumer.
type Consumer struct {
	id          string
	topic       string
	config      *Config
	kafkaConfig *kafka.ConfigMap

	waiter   sync.WaitGroup
	consumer *kafka.Consumer
	receiver pubsubs.Receiver

	log      actorkit.Logs
	context  context.Context
	canceler func()
}

// NewConsumer returns a new instance of a Consumer.
func NewConsumer(ctx context.Context, config *Config, group string, id string, topic string, receiver pubsubs.Receiver) (*Consumer, error) {
	if group == "" && config.DefaultConsumerGroup != "" {
		group = config.DefaultConsumerGroup
	}

	if id == "" {
		id = xid.New().String()
	}

	var subid = fmt.Sprintf(pubsubs.SubscriberTopicFormat, "librdkafka/kafka", config.ProjectID, topic, id)
	if group == "" {
		group = subid
	}

	kafkaConfig, err := generateConsumerConfig(group, config)
	if err != nil {
		err = errors.WrapOnly(err)
		config.Log.Emit(actorkit.ERROR, actorkit.LogMsgWithContext(err.Error(), "context", func(event *actorkit.LogEvent) {
			event.String("topic", topic).String("group", group)
		}))
		return nil, err
	}

	kconsumer, err := kafka.NewConsumer(&kafkaConfig)
	if err != nil {
		err = errors.WrapOnly(err)
		config.Log.Emit(actorkit.ERROR, actorkit.LogMsgWithContext(err.Error(), "context", func(event *actorkit.LogEvent) {
			event.String("topic", topic).String("group", group)
		}))
		return nil, err
	}

	rctx, can := context.WithCancel(ctx)

	return &Consumer{
		id:          subid,
		config:      config,
		topic:       topic,
		receiver:    receiver,
		log:         config.Log,
		kafkaConfig: &kafkaConfig,
		consumer:    kconsumer,
		context:     rctx,
		canceler:    can,
	}, nil
}

// Consume initializes giving consumer for message consumption
// from underline kafka consumer.
func (c *Consumer) Consume() error {
	if err := c.consumer.Subscribe(c.topic, nil); err != nil {
		err = errors.Wrap(err, "Failed to subscribe to topic %q", c.topic)
		c.log.Emit(actorkit.ERROR, actorkit.LogMsgWithContext(err.Error(), "context", nil).With(func(event *actorkit.LogEvent) {
			event.String("topic", c.topic).String("group", c.id)
		}))
		return err
	}

	c.waiter.Add(1)
	go c.run()
	return nil
}

// Topic returns the topic name of giving subscription.
func (c *Consumer) Topic() string {
	return c.topic
}

// Group returns the group or queue group name of giving subscription.
func (c *Consumer) Group() string {
	return ""
}

// ID returns the identification of giving subscription used for durability if supported.
func (c *Consumer) ID() string {
	return c.id
}

// Stop stops the giving consumer, ending all consuming operations.
func (c *Consumer) Stop() error {
	c.canceler()
	return nil
}

// Wait blocks till the consumer is closed.
func (c *Consumer) Wait() {
	c.waiter.Wait()
}

func (c *Consumer) close() error {
	if err := c.consumer.Close(); err != nil {
		err = errors.Wrap(err, "Failed to close consumer adequately for topic %q", c.topic)
		c.log.Emit(actorkit.ERROR, actorkit.LogMsgWithContext(err.Error(), "context", nil).With(func(event *actorkit.LogEvent) {
			event.String("topic", c.topic).String("group", c.id)
		}))
		return err
	}
	return nil
}

func (c *Consumer) run() {
	defer c.waiter.Done()

	ms := int(c.config.PollingTime.Seconds()) * 1000

	for {
		select {
		case <-c.context.Done():
			c.close()
			return
		default:
			if event := c.consumer.Poll(ms); event != nil {
				switch tm := event.(type) {
				case *kafka.Message:
					// close consumer if returned error is unacceptable.
					if err := c.handleIncomingMessage(tm); err != nil {
						c.close()
						return
					}
				case kafka.PartitionEOF:
				case kafka.OffsetsCommitted:
				default:
				}
			}
		}
	}
}

func (c *Consumer) handleIncomingMessage(msg *kafka.Message) error {
	if msg.TopicPartition.Error != nil {
		err := errors.WrapOnly(msg.TopicPartition.Error)
		return c.handleError(err, pubsubs.NACK, msg)
	}

	rec, err := c.config.Unmarshaler.Unmarshal(msg)
	if err != nil {
		err = errors.WrapOnly(err)
		c.log.Emit(actorkit.ERROR, actorkit.LogMsgWithContext(err.Error(), "context", nil).With(func(event *actorkit.LogEvent) {
			event.String("topic", c.topic).String("group", c.id)
		}))
		return err
	}

	c.log.Emit(actorkit.DEBUG, actorkit.LogMsgWithContext("received new message", "context", nil).With(func(event *actorkit.LogEvent) {
		event.String("topic", c.topic).String("group", c.id).ObjectJSON("message", rec)
	}))

	action, err := c.receiver(rec)
	if err != nil {
		err = errors.WrapOnly(err)
		c.log.Emit(actorkit.ERROR, actorkit.LogMsgWithContext(err.Error(), "context", nil).With(func(event *actorkit.LogEvent) {
			event.String("topic", c.topic).String("group", c.id)
		}))
		return c.handleError(err, action, msg)
	}

	if _, err := c.consumer.StoreOffsets([]kafka.TopicPartition{msg.TopicPartition}); err != nil {
		err = errors.WrapOnly(err)
		c.log.Emit(actorkit.ERROR, actorkit.LogMsgWithContext(err.Error(), "context", nil).With(func(event *actorkit.LogEvent) {
			event.String("topic", c.topic).String("group", c.id)
		}))
		return err
	}

	c.log.Emit(actorkit.DEBUG, actorkit.LogMsgWithContext("consumed new message", "context", nil).With(func(event *actorkit.LogEvent) {
		event.String("topic", c.topic).String("group", c.id).ObjectJSON("message", rec)
	}))

	return nil
}

func (c *Consumer) handleError(err error, action pubsubs.Action, msg *kafka.Message) error {
	switch action {
	case pubsubs.ACK:
		return nil
	case pubsubs.NACK:
		return c.rollback(msg)
	case pubsubs.NOPN:
		return err
	default:
		return nil
	}
}

func (c *Consumer) rollback(msg *kafka.Message) error {
	if err := c.consumer.Seek(msg.TopicPartition, 1000*60); err != nil {
		err = errors.Wrap(err, "Failed to rollback message")
		c.log.Emit(actorkit.ERROR, actorkit.LogMsgWithContext(err.Error(), "context", nil).With(func(event *actorkit.LogEvent) {
			event.String("topic", c.topic).String("group", c.id)
		}))
		return err
	}
	return nil
}

//****************************************************************************
// Kafka Publisher
//****************************************************************************

// Publisher implements a customized wrapper around the kafka Publisher.
// Delivery actorkit envelopes as messages to designated topics.
//
// The publisher is created to run with the time-life of it's context, once
// the passed in parent context expires, closes or get's cancelled. It
// will also self close.
type Publisher struct {
	topic       string
	config      *Config
	log         actorkit.Logs
	waiter      sync.WaitGroup
	context     context.Context
	canceler    func()
	kafkaConfig *kafka.ConfigMap
	producer    *kafka.Producer
}

// NewPublisher returns a new instance of Publisher.
func NewPublisher(ctx context.Context, config *Config, kafkaConfig kafka.ConfigMap, topic string) (*Publisher, error) {
	var kap Publisher
	kap.topic = topic
	kap.config = config
	kap.log = config.Log
	kap.kafkaConfig = &kafkaConfig

	rctx, can := context.WithCancel(ctx)
	kap.context = rctx
	kap.canceler = can

	config.Log.Emit(actorkit.DEBUG, actorkit.LogMsgWithContext("Creating new publisher", "context", nil).
		String("topic", topic).ObjectJSON("config", kafkaConfig))

	producer, err := kafka.NewProducer(kap.kafkaConfig)
	if err != nil {
		err = errors.Wrap(err, "Failed to create new Producer")
		kap.log.Emit(actorkit.ERROR, actorkit.LogMsgWithContext(err.Error(), "context", nil).With(func(event *actorkit.LogEvent) {
			event.String("topic", topic).ObjectJSON("config", kap.kafkaConfig)
		}))
		return nil, err
	}

	kap.log.Emit(actorkit.DEBUG, actorkit.LogMsgWithContext("Created producer for topic", "context", nil).With(func(event *actorkit.LogEvent) {
		event.String("topic", topic).String("addr", producer.String()).ObjectJSON("config", kap.kafkaConfig)
	}))

	kap.producer = producer

	// block until the context get's closed.
	kap.waiter.Add(1)
	go kap.blockUntil()

	return &kap, nil
}

// Close attempts to close giving underline producer.
func (ka *Publisher) Close() error {
	ka.canceler()
	return nil
}

// Wait blocks till the producer get's closed.
func (ka *Publisher) Wait() {
	ka.waiter.Wait()
}

// Publish sends giving message envelope  to given topic.
func (ka *Publisher) Publish(msg actorkit.Envelope) error {
	encoded, err := ka.config.Marshaler.Marshal(pubsubs.Message{Topic: ka.topic, Envelope: msg})
	if err != nil {
		err = errors.Wrap(err, "Failed to marshal incoming message")
		ka.log.Emit(actorkit.ERROR, actorkit.LogMsgWithContext(err.Error(), "context", nil).With(func(event *actorkit.LogEvent) {
			event.String("topic", ka.topic).ObjectJSON("message", msg)
		}))
		return err
	}

	ka.log.Emit(actorkit.DEBUG, actorkit.LogMsgWithContext("publishing new message", "context", nil).With(func(event *actorkit.LogEvent) {
		event.String("topic", ka.topic).ObjectJSON("message", encoded)
	}))

	res := make(chan kafka.Event)
	if err := ka.producer.Produce(&encoded, res); err != nil {
		err = errors.Wrap(err, "failed to send message to producer")
		ka.log.Emit(actorkit.ERROR, actorkit.LogMsgWithContext(err.Error(), "context", nil).With(func(event *actorkit.LogEvent) {
			event.String("topic", ka.topic).ObjectJSON("message", msg)
		}))
		return err
	}

	ka.log.Emit(actorkit.DEBUG, actorkit.LogMsgWithContext("reading event message", "context", nil).With(func(event *actorkit.LogEvent) {
		event.String("topic", ka.topic).ObjectJSON("message", encoded)
	}))

	var event kafka.Event

	select {
	case event = <-res:
		ka.log.Emit(actorkit.DEBUG, actorkit.LogMsgWithContext("event message received", "context", nil).With(func(event *actorkit.LogEvent) {
			event.String("topic", ka.topic).ObjectJSON("message", encoded).ObjectJSON("event", event)
		}))
	case <-time.After(ka.config.MessageDeliveryTimeout):
		err := errors.New("timeout: failed to receive response on event channel")
		ka.log.Emit(actorkit.ERROR, actorkit.LogMsgWithContext(err.Error(), "context", nil).With(func(event *actorkit.LogEvent) {
			event.String("topic", ka.topic).ObjectJSON("message", msg)
		}))
		return err
	}

	kmessage, ok := event.(*kafka.Message)
	if ok {
		err = errors.New("failed to receive *Kafka.Message as event response")
		ka.log.Emit(actorkit.ERROR, actorkit.LogMsgWithContext(err.Error(), "context", nil).With(func(event *actorkit.LogEvent) {
			event.String("topic", ka.topic).ObjectJSON("message", msg)
		}))
		return err
	}

	if kmessage.TopicPartition.Error != nil {
		err = errors.Wrap(kmessage.TopicPartition.Error, "failed to deliver message to kafka topic")
		ka.log.Emit(actorkit.ERROR, actorkit.LogMsgWithContext(err.Error(), "context", nil).With(func(event *actorkit.LogEvent) {
			event.String("topic", ka.topic).ObjectJSON("message", msg)
		}))
		return err
	}

	ka.log.Emit(actorkit.DEBUG, actorkit.LogMsgWithContext("published new message", "context", nil).With(func(event *actorkit.LogEvent) {
		event.String("topic", ka.topic).ObjectJSON("message", encoded)
	}))
	return nil
}

func (ka *Publisher) blockUntil() {
	defer ka.waiter.Done()
	<-ka.context.Done()
	ka.producer.Close()
}

//****************************************************************************
// internal functions
//****************************************************************************

func generateConsumerConfig(id string, config *Config) (kafka.ConfigMap, error) {
	kconfig := kafka.ConfigMap{
		"debug":                ",",
		"session.timeout.ms":   6000,
		"auto.offset.reset":    config.AutoOffsetReset,
		"default.topic.config": kafka.ConfigMap{"auto.offset.reset": config.AutoOffsetReset},
	}

	if !config.NoConsumerGroup {
		kconfig.SetKey("group.id", id)
		kconfig.SetKey("enable.auto.commit", true)

		// to achieve at-least-once delivery we store offsets after processing of the message
		kconfig.SetKey("enable.auto.offset.store", false)
	} else {
		// this group will be not committed, setting just for api requirements
		kconfig.SetKey("group.id", "no_group_"+xid.New().String())
		kconfig.SetKey("enable.auto.commit", false)
	}

	if err := mergeConfluentConfigs(&kconfig, &config.ConsumerOverrides); err != nil {
		return kconfig, err
	}

	kconfig["bootstrap.servers"] = strings.Join(config.Brokers, ",")

	return kconfig, nil
}

func generateProducerConfig(config *Config) (kafka.ConfigMap, error) {
	konfig := kafka.ConfigMap{
		"debug":                        ",",
		"queue.buffering.max.messages": 10000000,
		"queue.buffering.max.kbytes":   2097151,
	}

	if err := mergeConfluentConfigs(&konfig, &config.ProducerOverrides); err != nil {
		return konfig, err
	}

	konfig["bootstrap.servers"] = strings.Join(config.Brokers, ",")

	return konfig, nil
}

func mergeConfluentConfigs(baseConfig *kafka.ConfigMap, valuesToSet *kafka.ConfigMap) error {
	for key, value := range *valuesToSet {
		if err := baseConfig.SetKey(key, value); err != nil {
			return errors.Wrap(err, "cannot overwrite config value for %s", key)
		}
	}

	return nil
}

const digits = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ~+"

func uint64ToID(u uint64) string {
	var buf [13]byte
	i := 13
	// base is power of 2: use shifts and addresss instead of / and %
	for u >= 64 {
		i--
		buf[i] = digits[uintptr(u)&0x3f]
		u >>= 6
	}
	// u < base
	i--
	buf[i] = digits[uintptr(u)]
	i--
	buf[i] = '$'

	return string(buf[i:])
}
