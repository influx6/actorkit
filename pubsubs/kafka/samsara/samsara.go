package samsara

import (
	"context"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/gokit/actorkit"

	"github.com/gokit/xid"

	"github.com/Shopify/sarama"
	"github.com/gokit/actorkit/pubsubs"
	"github.com/gokit/errors"
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
	Marshal(message pubsubs.Message) (sarama.ProducerMessage, error)
}

// Unmarshaler defines an interface who's implementer exposes said method to
// transform a kafka message into a pubsubs Message.
type Unmarshaler interface {
	Unmarshal(*sarama.ConsumerMessage) (pubsubs.Message, error)
}

// Partitioner takes giving message returning appropriate
// partition name to be used for kafka message.
type Partitioner func(pubsubs.Message) string

// MarshalerWrapper implements the Marshaler interface.
type MarshalerWrapper struct {
	Envelope    pubsubs.Marshaler
	Partitioner Partitioner
}

// Marshal implements the Marshaler interface.
func (kc MarshalerWrapper) Marshal(message pubsubs.Message) (sarama.ProducerMessage, error) {
	var newMessage sarama.ProducerMessage

	envelopeBytes, err := kc.Envelope.Marshal(message.Envelope)
	if err != nil {
		return newMessage, err
	}

	attrs := make([]sarama.RecordHeader, 0, len(message.Envelope.Header)+1)
	attrs = append(attrs, sarama.RecordHeader{
		Key:   []byte(idParam),
		Value: message.Envelope.Ref.Bytes(),
	})

	for k, v := range message.Envelope.Header {
		attrs = append(attrs, sarama.RecordHeader{
			Key:   []byte(k),
			Value: []byte(v),
		})
	}

	if kc.Partitioner != nil {
		newMessage.Key = sarama.ByteEncoder(kc.Partitioner(message))
	}

	newMessage.Topic = message.Topic
	newMessage.Headers = attrs
	newMessage.Value = sarama.ByteEncoder(envelopeBytes)
	return newMessage, nil
}

// UnmarshalerWrapper implements the Unmarshaler interface.
type UnmarshalerWrapper struct {
	Envelope pubsubs.Unmarshaler
}

// Unmarshal implements the Unmarshaler interface.
func (kc UnmarshalerWrapper) Unmarshal(message *sarama.ConsumerMessage) (pubsubs.Message, error) {
	var msg pubsubs.Message
	msg.Topic = message.Topic

	var err error
	if msg.Envelope, err = kc.Envelope.Unmarshal(message.Value); err != nil {
		return msg, err
	}

	// confirm header id matches envelope id
	for _, v := range message.Headers {
		var tm = string(v.Key)
		switch tm {
		case idParam:
			if string(v.Value) != msg.Envelope.Ref.String() {
				return msg, errors.New("Kafka message ID does not matched envelope data")
			}
		default:
			if !msg.Envelope.Has(tm) {
				msg.Envelope.Header[tm] = string(v.Value)
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
type SubscriberHandler func(*PublisherConsumerFactory, string, string, pubsubs.Receiver) (actorkit.Subscription, error)

// PubSubFactoryGenerator returns a function which taken a PublisherSubscriberFactory returning
// a factory for generating publishers and subscribers.
type PubSubFactoryGenerator func(*PublisherConsumerFactory) pubsubs.PubSubFactory

// PubSubFactory provides a partial function for the generation of a pubsub.PubSubFactory
// using the PubSubFactorGenerator function.
func PubSubFactory(publishers PublisherHandler, subscribers SubscriberHandler) PubSubFactoryGenerator {
	return func(pbs *PublisherConsumerFactory) pubsubs.PubSubFactory {
		return &pubsubs.PubSubFactoryImpl{
			Publishers: func(topic string) (pubsubs.Publisher, error) {
				return publishers(pbs, topic)
			},
			Subscribers: func(topic string, id string, receiver pubsubs.Receiver) (actorkit.Subscription, error) {
				return subscribers(pbs, topic, id, receiver)
			},
		}
	}
}

//****************************************************************************
// Kafka ConsumerFactor
//****************************************************************************

// Config defines configuration fields for use with a PublisherConsumerFactory.
type Config struct {
	Brokers                []string
	ConsumersCount         int
	NoConsumerGroup        bool
	ProjectID              string
	AutoOffsetReset        string
	DefaultConsumerGroup   string
	Marshaler              Marshaler
	Unmarshaler            Unmarshaler
	Log                    actorkit.Logs
	PollingTime            time.Duration
	MessageDeliveryTimeout time.Duration
	ConsumerOverrides      sarama.Config
	ProducerOverrides      sarama.Config
}

func (c *Config) init() {
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
func NewPublisherConsumerFactory(ctx context.Context, config Config) *PublisherConsumerFactory {
	config.init()
	rctx, cano := context.WithCancel(ctx)
	return &PublisherConsumerFactory{
		config:       config,
		rootContext:  rctx,
		rootCanceler: cano,
		consumers:    map[string]*Consumer{},
	}
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
func (ka *PublisherConsumerFactory) NewPublisher(topic string, userOverrides *sarama.Config) (*Publisher, error) {
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

// NewConsumer return a new consumer for a giving topic to be used for sarama.
// The provided id value if not empty will be used as the group.id.
func (ka *PublisherConsumerFactory) NewConsumer(topic string, id string, receiver pubsubs.Receiver) (*Consumer, error) {
	consumer, err := NewConsumer(ka.rootContext, &ka.config, id, topic, receiver)
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
	kafkaConfig *sarama.Config

	waiter   sync.WaitGroup
	consumer *sarama.Consumer
	receiver pubsubs.Receiver

	log      actorkit.Logs
	context  context.Context
	canceler func()
}

// NewConsumer returns a new instance of a Consumer.
func NewConsumer(ctx context.Context, config *Config, id string, topic string, receiver pubsubs.Receiver) (*Consumer, error) {
	if id == "" && config.DefaultConsumerGroup != "" {
		id = config.DefaultConsumerGroup
	}

	groupID := fmt.Sprintf(pubsubs.SubscriberTopicFormat, "kafka", config.ProjectID, topic, id)
	kafkaConfig, err := generateConsumerConfig(groupID, config)
	if err != nil {
		err = errors.WrapOnly(err)
		config.Log.Emit(actorkit.ERROR, actorkit.LogMsgWithContext(err.Error(), "context", func(event actorkit.LogEvent) {
			event.String("topic", topic).String("group", groupID)
		}))
		return nil, err
	}

	kconsumer, err := sarama.NewConsumer(&kafkaConfig)
	if err != nil {
		err = errors.WrapOnly(err)
		config.Log.Emit(actorkit.ERROR, actorkit.LogMsgWithContext(err.Error(), "context", func(event actorkit.LogEvent) {
			event.String("topic", topic).String("group", groupID)
		}))
		return nil, err
	}

	rctx, can := context.WithCancel(ctx)

	return &Consumer{
		id:          id,
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
		c.log.Emit(actorkit.ERROR, actorkit.LogMsgWithContext(err.Error(), "context", nil).With(func(event actorkit.LogEvent) {
			event.String("topic", c.topic).String("group", c.id)
		}))
		return err
	}

	c.waiter.Add(1)
	go c.run()
	return nil
}

// Stop stops the giving consumer, ending all consuming operations.
func (c *Consumer) Stop() {
	c.canceler()
}

// Wait blocks till the consumer is closed.
func (c *Consumer) Wait() {
	c.waiter.Wait()
}

func (c *Consumer) close() error {
	if err := c.consumer.Close(); err != nil {
		err = errors.Wrap(err, "Failed to close consumer adequately for topic %q", c.topic)
		c.log.Emit(actorkit.ERROR, actorkit.LogMsgWithContext(err.Error(), "context", nil).With(func(event actorkit.LogEvent) {
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
				case *sarama.Message:
					// close consumer if returned error is unacceptable.
					if err := c.handleIncomingMessage(tm); err != nil {
						c.close()
						return
					}
				case sarama.PartitionEOF:
				case sarama.OffsetsCommitted:
				default:
				}
			}
		}
	}
}

func (c *Consumer) handleIncomingMessage(msg *sarama.Message) error {
	if msg.TopicPartition.Error != nil {
		err := errors.WrapOnly(msg.TopicPartition.Error)
		return c.handleError(err, pubsubs.NACK, msg)
	}

	rec, err := c.config.Unmarshaler.Unmarshal(msg)
	if err != nil {
		err = errors.WrapOnly(err)
		c.log.Emit(actorkit.ERROR, actorkit.LogMsgWithContext(err.Error(), "context", nil).With(func(event actorkit.LogEvent) {
			event.String("topic", c.topic).String("group", c.id)
		}))
		return err
	}

	c.log.Emit(actorkit.DEBUG, actorkit.LogMsgWithContext("received new message", "context", nil).With(func(event actorkit.LogEvent) {
		event.String("topic", c.topic).String("group", c.id).ObjectJSON("message", rec)
	}))

	action, err := c.receiver(rec)
	if err != nil {
		err = errors.WrapOnly(err)
		c.log.Emit(actorkit.ERROR, actorkit.LogMsgWithContext(err.Error(), "context", nil).With(func(event actorkit.LogEvent) {
			event.String("topic", c.topic).String("group", c.id)
		}))
		return c.handleError(err, action, msg)
	}

	if _, err := c.consumer.StoreOffsets([]sarama.TopicPartition{msg.TopicPartition}); err != nil {
		err = errors.WrapOnly(err)
		c.log.Emit(actorkit.ERROR, actorkit.LogMsgWithContext(err.Error(), "context", nil).With(func(event actorkit.LogEvent) {
			event.String("topic", c.topic).String("group", c.id)
		}))
		return err
	}

	c.log.Emit(actorkit.DEBUG, actorkit.LogMsgWithContext("consumed new message", "context", nil).With(func(event actorkit.LogEvent) {
		event.String("topic", c.topic).String("group", c.id).ObjectJSON("message", rec)
	}))

	return nil
}

func (c *Consumer) handleError(err error, action pubsubs.Action, msg *sarama.Message) error {
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

func (c *Consumer) rollback(msg *sarama.Message) error {
	if err := c.consumer.Seek(msg.TopicPartition, 1000*60); err != nil {
		err = errors.Wrap(err, "Failed to rollback message")
		c.log.Emit(actorkit.ERROR, actorkit.LogMsgWithContext(err.Error(), "context", nil).With(func(event actorkit.LogEvent) {
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
	kafkaConfig *sarama.ConfigMap
	producer    *sarama.Producer
}

// NewPublisher returns a new instance of Publisher.
func NewPublisher(ctx context.Context, config *Config, kafkaConfig sarama.ConfigMap, topic string) (*Publisher, error) {
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

	producer, err := sarama.NewProducer(kap.kafkaConfig)
	if err != nil {
		err = errors.Wrap(err, "Failed to create new Producer")
		kap.log.Emit(actorkit.ERROR, actorkit.LogMsgWithContext(err.Error(), "context", nil).With(func(event actorkit.LogEvent) {
			event.String("topic", topic).ObjectJSON("config", kap.kafkaConfig)
		}))
		return nil, err
	}

	kap.log.Emit(actorkit.DEBUG, actorkit.LogMsgWithContext("Created producer for topic", "context", nil).With(func(event actorkit.LogEvent) {
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
		ka.log.Emit(actorkit.ERROR, actorkit.LogMsgWithContext(err.Error(), "context", nil).With(func(event actorkit.LogEvent) {
			event.String("topic", ka.topic).ObjectJSON("message", msg)
		}))
		return err
	}

	ka.log.Emit(actorkit.DEBUG, actorkit.LogMsgWithContext("publishing new message", "context", nil).With(func(event actorkit.LogEvent) {
		event.String("topic", ka.topic).ObjectJSON("message", encoded)
	}))

	res := make(chan sarama.Event)
	if err := ka.producer.Produce(&encoded, res); err != nil {
		err = errors.Wrap(err, "failed to send message to producer")
		ka.log.Emit(actorkit.ERROR, actorkit.LogMsgWithContext(err.Error(), "context", nil).With(func(event actorkit.LogEvent) {
			event.String("topic", ka.topic).ObjectJSON("message", msg)
		}))
		return err
	}

	ka.log.Emit(actorkit.DEBUG, actorkit.LogMsgWithContext("reading event message", "context", nil).With(func(event actorkit.LogEvent) {
		event.String("topic", ka.topic).ObjectJSON("message", encoded)
	}))

	var event sarama.Event

	select {
	case event = <-res:
		ka.log.Emit(actorkit.DEBUG, actorkit.LogMsgWithContext("event message received", "context", nil).With(func(event actorkit.LogEvent) {
			event.String("topic", ka.topic).ObjectJSON("message", encoded).ObjectJSON("event", event)
		}))
	case <-time.After(ka.config.MessageDeliveryTimeout):
		err := errors.New("timeout: failed to receive response on event channel")
		ka.log.Emit(actorkit.ERROR, actorkit.LogMsgWithContext(err.Error(), "context", nil).With(func(event actorkit.LogEvent) {
			event.String("topic", ka.topic).ObjectJSON("message", msg)
		}))
		return err
	}

	kmessage, ok := event.(*sarama.Message)
	if ok {
		err = errors.New("failed to receive *Kafka.Message as event response")
		ka.log.Emit(actorkit.ERROR, actorkit.LogMsgWithContext(err.Error(), "context", nil).With(func(event actorkit.LogEvent) {
			event.String("topic", ka.topic).ObjectJSON("message", msg)
		}))
		return err
	}

	if kmessage.TopicPartition.Error != nil {
		err = errors.Wrap(kmessage.TopicPartition.Error, "failed to deliver message to kafka topic")
		ka.log.Emit(actorkit.ERROR, actorkit.LogMsgWithContext(err.Error(), "context", nil).With(func(event actorkit.LogEvent) {
			event.String("topic", ka.topic).ObjectJSON("message", msg)
		}))
		return err
	}

	ka.log.Emit(actorkit.DEBUG, actorkit.LogMsgWithContext("published new message", "context", nil).With(func(event actorkit.LogEvent) {
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

func generateConsumerConfig(id string, config *Config) (sarama.ConfigMap, error) {
	kconfig := sarama.ConfigMap{
		"debug":                ",",
		"session.timeout.ms":   6000,
		"auto.offset.reset":    config.AutoOffsetReset,
		"bootstrap.servers":    strings.Join(config.Brokers, ","),
		"default.topic.config": sarama.ConfigMap{"auto.offset.reset": config.AutoOffsetReset},
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

	return kconfig, nil
}

func generateProducerConfig(config *Config) (sarama.ConfigMap, error) {
	konfig := sarama.ConfigMap{
		"debug":                        ",",
		"queue.buffering.max.messages": 10000000,
		"queue.buffering.max.kbytes":   2097151,
		"bootstrap.servers":            strings.Join(config.Brokers, ","),
	}

	if err := mergeConfluentConfigs(&konfig, &config.ProducerOverrides); err != nil {
		return konfig, err
	}
	return konfig, nil
}

func mergeConfluentConfigs(baseConfig *sarama.ConfigMap, valuesToSet *sarama.ConfigMap) error {
	for key, value := range *valuesToSet {
		if err := baseConfig.SetKey(key, value); err != nil {
			return errors.Wrap(err, "cannot overwrite config value for %s", key)
		}
	}

	return nil
}
