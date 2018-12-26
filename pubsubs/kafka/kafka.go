package kafka

import (
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/gokit/actorkit"

	"github.com/gokit/xid"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/gokit/actorkit/pubsubs"
	"github.com/gokit/errors"
)

const (
	kafkaIDName = "_actorkit_kafka_uid"
)

var (
	_ Marshaler   = &KAMarshaler{}
	_ Unmarshaler = &KAUnmarshaler{}
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

// KAMarshaler implements the Marshaler interface.
type KAMarshaler struct {
	Envelope pubsubs.Marshaler

	// Partitioner takes giving message returning appropriate
	// partition name to be used for kafka message.
	Partitioner func(pubsubs.Message) string
}

// Marshal implements the Marshaler interface.
func (kc *KAMarshaler) Marshal(message pubsubs.Message) (kafka.Message, error) {
	if message.Envelope.Has(kafkaIDName) {
		return kafka.Message{}, errors.New("key %q can not be used as it internally used for kafka message tracking", kafkaIDName)
	}

	envelopeBytes, err := kc.Envelope.Marshal(message.Envelope)
	if err != nil {
		return kafka.Message{}, err
	}

	attrs := make([]kafka.Header, 0, len(message.Envelope.Header)+1)
	attrs = append(attrs, kafka.Header{Key: kafkaIDName, Value: message.Envelope.Ref.Bytes()})

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

// KAUnmarshaler implements the Unmarshaler interface.
type KAUnmarshaler struct {
	Envelope pubsubs.Unmarshaler
}

// Unmarshal implements the Unmarshaler interface.
func (kc *KAUnmarshaler) Unmarshal(message *kafka.Message) (pubsubs.Message, error) {
	var msg pubsubs.Message
	msg.Topic = *message.TopicPartition.Topic

	var err error
	if msg.Envelope, err = kc.Envelope.Unmarshal(message.Value); err != nil {
		return msg, err
	}

	// confirm header id matches envelope id
	for _, v := range message.Headers {
		switch v.Key {
		case kafkaIDName:
			if string(v.Value) != msg.Envelope.Ref.String() {
				return msg, errors.New("Kafka ID does not matched unmarshalled envelope data")
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

// PublisherHandler defines a function type which takes a giving PublisherFactory
// and a given topic, returning a new publisher with all related underline specific
// details added and instantiated.
type PublisherHandler func(*PublisherFactory, string) (pubsubs.Publisher, error)

// SubscriberHandler defines a function type which takes a giving SubscriptionFactory
// and a given topic, returning a new subscription with all related underline specific
// details added and instantiated.
type SubscriberHandler func(*ConsumerFactory, string, string, pubsubs.Receiver) (actorkit.Subscription, error)

// PubSubFactoryGenerator returns a function which taken a PublisherSubscriberFactory returning
// a factory for generating publishers and subscribers.
type PubSubFactoryGenerator func(*PublisherFactory, *ConsumerFactory) pubsubs.PubSubFactory

// PubSubFactory provides a partial function for the generation of a pubsubs.PubSubFactory
// using the PubSubFactorGenerator function.
func PubSubFactory(publishers PublisherHandler, subscribers SubscriberHandler) PubSubFactoryGenerator {
	return func(pub *PublisherFactory, sub *ConsumerFactory) pubsubs.PubSubFactory {
		return &pubsubs.PubSubFactoryImpl{
			Publishers: func(topic string) (pubsubs.Publisher, error) {
				return publishers(pub, topic)
			},
			Subscribers: func(topic string, id string, receiver pubsubs.Receiver) (actorkit.Subscription, error) {
				return subscribers(sub, topic, id, receiver)
			},
		}
	}
}

//****************************************************************************
// Kafka ConsumerFactor
//****************************************************************************

// Config defines configuration fields for use with a ConsumerFactory.
type Config struct {
	Brokers              []string
	ConsumersCount       int
	AutoOffsetReset      string
	NoConsumerGroup      bool
	DefaultConsumerGroup string
	Unmarshaler          Unmarshaler
	Log                  actorkit.Logs
	PollingTime          time.Duration
	Overrides            kafka.ConfigMap
}

// ConsumerFactory implements a kafka subscriber provider which handles and manages
// kafka based consumers of giving topics.
type ConsumerFactory struct {
	config    Config
	doOnce    sync.Once
	closer    chan struct{}
	waiter    sync.WaitGroup
	cl        sync.RWMutex
	consumers map[string]*Consumer
}

// NewConsumerFactory returns a new instance of a ConsumerFactory.
func NewConsumerFactory(config Config, unmarshaler Unmarshaler) *ConsumerFactory {
	var ksub ConsumerFactory
	ksub.config = config
	ksub.closer = make(chan struct{}, 0)
	ksub.consumers = map[string]*Consumer{}

	if config.ConsumersCount == 0 {
		config.ConsumersCount = runtime.NumCPU()
	}

	if config.AutoOffsetReset == "" {
		config.AutoOffsetReset = "latest"
	}

	if config.DefaultConsumerGroup == "" {
		config.NoConsumerGroup = true
	}

	return &ksub
}

// Wait blocks till all consumers generated by giving factory are closed.
func (ka *ConsumerFactory) Wait() {
	ka.waiter.Wait()
}

// Close closes all Consumers generated by consumer factory.
func (ka *ConsumerFactory) Close() error {
	ka.doOnce.Do(func() {
		close(ka.closer)
	})
	ka.waiter.Wait()
	return nil
}

// CreateConsumer return a new consumer for a giving topic to be used for kafka.
// The provided id value if not empty will be used as the group.id.
func (ka *ConsumerFactory) CreateConsumer(topic string, id string, receiver func(message pubsubs.Message) error, director func(error) Directive) (*Consumer, error) {
	var cid string
	if id == "" {
		cid = ka.config.DefaultConsumerGroup
	} else {
		cid = id
	}

	config, err := ka.generateConfig(cid)
	if err != nil {
		return nil, err
	}

	consumer, err := NewConsumer(&ka.config, config, topic, ka.config.PollingTime, ka.config.Unmarshaler, receiver, director)
	if err != nil {
		return nil, err
	}

	ka.waiter.Add(1)

	errRes := make(chan error, 1)
	go func() {
		defer ka.waiter.Done()
		consumer.Consume(ka.closer, errRes)
	}()

	if err = <-errRes; err != nil {
		return nil, err
	}

	ka.cl.Lock()
	ka.consumers[topic] = consumer
	ka.cl.Unlock()
	return consumer, nil
}

func (ka *ConsumerFactory) hasConsumer(topic string) bool {
	ka.cl.RLock()
	defer ka.cl.RUnlock()
	_, ok := ka.consumers[topic]
	return ok
}

func (ka *ConsumerFactory) generateConfig(id string) (*kafka.ConfigMap, error) {
	kconfig := &kafka.ConfigMap{
		"debug":                ",",
		"session.timeout.ms":   6000,
		"auto.offset.reset":    ka.config.AutoOffsetReset,
		"bootstrap.servers":    strings.Join(ka.config.Brokers, ","),
		"default.topic.config": kafka.ConfigMap{"auto.offset.reset": ka.config.AutoOffsetReset},
	}

	if !ka.config.NoConsumerGroup {
		kconfig.SetKey("group.id", id)
		kconfig.SetKey("enable.auto.commit", true)

		// to achieve at-least-once delivery we store offsets after processing of the message
		kconfig.SetKey("enable.auto.offset.store", false)
	} else {
		// this group will be not committed, setting just for api requirements
		kconfig.SetKey("group.id", "no_group_"+xid.New().String())
		kconfig.SetKey("enable.auto.commit", false)
	}

	if err := mergeConfluentConfigs(kconfig, ka.config.Overrides); err != nil {
		return kconfig, err
	}

	return kconfig, nil
}

//****************************************************************************
// Kafka Consumer
//****************************************************************************

// Directive defines a int type to represent directive handling for an error.
type Directive int

// constants of directive.
const (
	Rollback Directive = iota
	Close
)

// Consumer implements a Kafka message subscription consumer.
type Consumer struct {
	topic       string
	config      *Config
	once        sync.Once
	closer      chan struct{}
	unmarshaler Unmarshaler
	log         actorkit.Logs
	polling     time.Duration
	consumer    *kafka.Consumer
	directive   func(error) Directive
	receiver    func(message pubsubs.Message) error
}

// NewConsumer returns a new instance of a Consumer.
func NewConsumer(co *Config, config *kafka.ConfigMap, topic string, polling time.Duration, unmarshaler Unmarshaler, receiver func(message pubsubs.Message) error, director func(error) Directive) (*Consumer, error) {
	kconsumer, err := kafka.NewConsumer(config)
	if err != nil {
		return nil, err
	}

	return &Consumer{
		topic:       topic,
		config:      co,
		log:         co.Log,
		polling:     polling,
		consumer:    kconsumer,
		directive:   director,
		receiver:    receiver,
		unmarshaler: unmarshaler,
		closer:      make(chan struct{}, 0),
	}, nil
}

// Consume initializes giving consumer for message consumption
// from underline kafka consumer.
func (c *Consumer) Consume(kill chan struct{}, errs chan error) {
	if err := c.consumer.Subscribe(c.topic, nil); err != nil {
		em := errors.Wrap(err, "Failed to subscribe to topic %q", c.topic)
		if c.log != nil {
			c.log.Emit(actorkit.ERROR, pubsubs.SubscriptionError{Err: em, Topic: c.topic})
		}
		errs <- em
		return
	}

	errs <- nil
	c.run(kill)
}

// Stop stops the giving consumer, ending all consuming operations.
func (c *Consumer) Stop() {
	c.once.Do(func() {
		close(c.closer)
	})
}

func (c *Consumer) close() {
	if err := c.consumer.Close(); err != nil {
		em := errors.Wrap(err, "Failed to close subscriber for topic %q", c.topic)
		if c.log != nil {
			c.log.Emit(actorkit.ERROR, pubsubs.OpError{Err: em, Topic: c.topic})
		}
	}
}

func (c *Consumer) run(closer chan struct{}) {
	ms := int(c.polling.Seconds()) * 1000

	for {
		select {
		case <-c.closer:
			c.close()
			return
		case <-closer:
			c.close()
			return
		default:
			if event := c.consumer.Poll(ms); event != nil {
				switch tm := event.(type) {
				case *kafka.Message:
					if err := c.handleIncomingMessage(tm); err != nil {
						// close consumer has the given error is unacceptable.
						c.consumer.Close()

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
		return c.handleError(msg.TopicPartition.Error, msg)
	}

	rec, err := c.unmarshaler.Unmarshal(msg)
	if err != nil {
		if c.log != nil {
			c.log.Emit(actorkit.ERROR, pubsubs.UnmarshalingError{Err: errors.Wrap(err, "Failed to marshal message"), Data: msg.Value})
		}
		return c.handleError(err, msg)
	}

	if err := c.receiver(rec); err != nil {
		if c.log != nil {
			c.log.Emit(actorkit.ERROR, pubsubs.MessageHandlingError{Err: errors.Wrap(err, "Failed to process message"), Data: msg.Value, Topic: c.topic})
		}
		return c.handleError(err, msg)
	}

	if _, err := c.consumer.StoreOffsets([]kafka.TopicPartition{msg.TopicPartition}); err != nil {
		if c.log != nil {
			c.log.Emit(actorkit.ERROR, pubsubs.OpError{Err: errors.Wrap(err, "Failed to set new message offset for topic partition %q", c.topic), Topic: c.topic})
		}
		return err
	}

	return nil
}

func (c *Consumer) handleError(err error, msg *kafka.Message) error {
	switch c.directive(err) {
	case Rollback:
		return c.rollback(msg)
	case Close:
		return err
	}
	return nil
}

func (c *Consumer) rollback(msg *kafka.Message) error {
	if err := c.consumer.Seek(msg.TopicPartition, 1000*60); err != nil {
		sem := errors.Wrap(err, "Failed to rollback message")
		if c.log != nil {
			c.log.Emit(actorkit.ERROR, pubsubs.OpError{Err: sem, Topic: c.topic})
		}
		return sem
	}
	return nil
}

//****************************************************************************
// Kafka Publisher
//****************************************************************************

// PublisherFactory defines a factor for creating publishers which send messages for giving
// kafka brokers.
type PublisherFactory struct {
	brokers   []string
	base      kafka.ConfigMap
	log       actorkit.Logs
	marshaler Marshaler
}

// NewPublisherFactory returns a new instance of the PublisherFactory using the default list of brokers
// and default ConfigMap (this is optional, if not provided, one will be used). This default config map
// will be merged with any provided during call to PublisherFactory.Publisher.
func NewPublisherFactory(brokers []string, base *kafka.ConfigMap, marshaler Marshaler, log actorkit.Logs) *PublisherFactory {
	if base == nil {
		base = &kafka.ConfigMap{
			"debug":                        ",",
			"queue.buffering.max.messages": 10000000,
			"queue.buffering.max.kbytes":   2097151,
			"bootstrap.servers":            strings.Join(brokers, ","),
		}
	}

	return &PublisherFactory{
		log:       log,
		base:      *base,
		brokers:   brokers,
		marshaler: marshaler,
	}
}

// NewPublisher returns a new instance of a Publisher for a giving topic.
func (p *PublisherFactory) NewPublisher(topic string, userconf kafka.ConfigMap) (*Publisher, error) {
	newConfig := kafka.ConfigMap{}

	if err := mergeConfluentConfigs(&newConfig, p.base); err != nil {
		return nil, err
	}

	if err := mergeConfluentConfigs(&newConfig, userconf); err != nil {
		return nil, err
	}

	newConfig["bootstrap.servers"] = strings.Join(p.brokers, ",")

	return NewPublisher(topic, &newConfig, p.log, p.marshaler)
}

// Publisher implements a customized wrapper around the kafka Publisher.
type Publisher struct {
	topic     string
	marshaler Marshaler
	once      sync.Once
	config    *kafka.ConfigMap
	producer  *kafka.Producer
	log       actorkit.Logs
}

// NewPublisher returns a new instance of Publisher.
func NewPublisher(topic string, config *kafka.ConfigMap, log actorkit.Logs, marshaler Marshaler) (*Publisher, error) {
	var kap Publisher
	kap.topic = topic
	kap.config = config

	producer, err := kafka.NewProducer(kap.config)
	if err != nil {
		return nil, err
	}

	kap.log = log
	kap.producer = producer
	kap.marshaler = marshaler
	return &kap, nil
}

// Close attempts to close giving underline producer.
func (ka *Publisher) Close() error {
	ka.once.Do(func() {
		ka.producer.Close()
	})
	return nil
}

// Publish sends giving message envelope  to given topic.
func (ka *Publisher) Publish(msg actorkit.Envelope) error {
	encoded, err := ka.marshaler.Marshal(pubsubs.Message{Topic: ka.topic, Envelope: msg})
	if err != nil {
		em := errors.Wrap(err, "Failed to marshal incoming message: %%v", msg)
		if ka.log != nil {
			ka.log.Emit(actorkit.ERROR, pubsubs.MarshalingError{Err: em, Data: msg})
		}
		return err
	}

	res := make(chan kafka.Event)
	if err := ka.producer.Produce(&encoded, res); err != nil {
		em := errors.Wrap(err, "failed to send mesage to producer")
		if ka.log != nil {
			ka.log.Emit(actorkit.ERROR, pubsubs.PublishError{Err: em, Data: encoded, Topic: ka.topic})
		}
		return em
	}

	event := <-res
	kmessage, ok := event.(*kafka.Message)
	if ok {
		em := errors.New("failed to receive *Kafka.Message as event response")
		if ka.log != nil {
			ka.log.Emit(actorkit.ERROR, pubsubs.PublishError{Err: em, Data: encoded, Topic: ka.topic})
		}
		return em
	}

	if kmessage.TopicPartition.Error != nil {
		em := errors.Wrap(kmessage.TopicPartition.Error, "failed to deliver message to kafka topic")
		if ka.log != nil {
			ka.log.Emit(actorkit.ERROR, pubsubs.PublishError{Err: em, Data: encoded, Topic: ka.topic})
		}
		return em
	}

	return nil
}

func mergeConfluentConfigs(baseConfig *kafka.ConfigMap, valuesToSet kafka.ConfigMap) error {
	for key, value := range valuesToSet {
		if err := baseConfig.SetKey(key, value); err != nil {
			return errors.Wrap(err, "cannot overwrite config value for %s", key)
		}
	}

	return nil
}
