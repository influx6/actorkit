package kafka

import (
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/gokit/xid"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/gokit/actorkit/transit"
	"github.com/gokit/errors"
)

const (
	kafkaIDName = "_actorkit_kafka_uid"
)

var (
	_ Marshaler   = &KAMarshaler{}
	_ Unmarshaler = &KAUnmarshaler{}
)

// Marshaler defines a interface exposing method to transform a transit.Message
// into a kafka message.
type Marshaler interface {
	Marshal(message transit.Message) (kafka.Message, error)
}

// Unmarshaler defines an interface who's implementer exposes said method to
// transform a kafka message into a transit Message.
type Unmarshaler interface {
	Unmarshal(kafka.Message) (transit.Message, error)
}

// KAMarshaler implements the Marshaler interface.
type KAMarshaler struct {
	EnvelopeMarshaler transit.Marshaler

	// Partitioner takes giving message returning appropriate
	// partition name to be used for kafka message.
	Partitioner func(transit.Message) string
}

// Marshal implements the Marshaler interface.
func (kc *KAMarshaler) Marshal(message transit.Message) (kafka.Message, error) {
	envelopeBytes, err := kc.EnvelopeMarshaler.Marshal(message.Envelope)
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
	EnvelopeUnmarshaler transit.Unmarshaler
}

// Unmarshal implements the Unmarshaler interface.
func (kc *KAUnmarshaler) Unmarshal(message kafka.Message) (transit.Message, error) {
	var msg transit.Message
	msg.Topic = *message.TopicPartition.Topic

	var err error
	if msg.Envelope, err = kc.EnvelopeUnmarshaler.Unmarshal(message.Value); err != nil {
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

//****************************************************************************
// Kafka Subscriber
//****************************************************************************

// Config defines configuration fields for use with a ConsumerFactory.
type Config struct {
	Brokers []string

	Unmarshaler     Unmarshaler
	ConsumerGroup   string
	NoConsumerGroup bool

	AutoOffsetReset string

	ConsumersCount int

	PollingTime time.Duration
	Overrides   kafka.ConfigMap
}

// ConsumerFactory implements a kafka subscriber provider which handles and manages
// kafka based consumers of giving topics.
type ConsumerFactory struct {
	config Config
}

// NewConsumerFactory returns a new instance of a ConsumerFactory.
func NewConsumerFactory(config Config, unmarshaler Unmarshaler) *ConsumerFactory {
	var ksub ConsumerFactory
	ksub.config = config

	if config.ConsumersCount == 0 {
		config.ConsumersCount = runtime.NumCPU()
	}

	if config.AutoOffsetReset == "" {
		config.AutoOffsetReset = "latest"
	}

	if config.ConsumerGroup == "" {
		config.NoConsumerGroup = true
	}

	return &ksub
}

func (ka *ConsumerFactory) generateConfig() (*kafka.ConfigMap, error) {
	kconfig := &kafka.ConfigMap{
		"debug":                ",",
		"session.timeout.ms":   6000,
		"auto.offset.reset":    ka.config.AutoOffsetReset,
		"bootstrap.servers":    strings.Join(ka.config.Brokers, ","),
		"default.topic.config": kafka.ConfigMap{"auto.offset.reset": ka.config.AutoOffsetReset},
	}

	if !ka.config.NoConsumerGroup {
		kconfig.SetKey("group.id", ka.config.ConsumerGroup)
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

// CreateConsumer return a new consumer
func (ka *ConsumerFactory) CreateConsumer(topic string, receiver func(message transit.Message)) (*Consumer, error) {
	config, err := ka.generateConfig()
	if err != nil {
		return nil, err
	}

	return NewConsumer(config, topic, ka.config.PollingTime, ka.config.Unmarshaler, receiver)
}

// Consumer implements a Kafka message subscription consumer.
type Consumer struct {
	err         error
	topic       string
	oncer       sync.Once
	polling     time.Duration
	waiter      sync.WaitGroup
	unmarshaler Unmarshaler
	closing     chan struct{}
	consumer    *kafka.Consumer
	receiver    func(message transit.Message)
}

// NewConsumer returns a new instance of a Consumer.
func NewConsumer(config *kafka.ConfigMap, topic string, polling time.Duration, unmarshaler Unmarshaler, receiver func(message transit.Message)) (*Consumer, error) {
	kconsumer, err := kafka.NewConsumer(config)
	if err != nil {
		return nil, err
	}

	return &Consumer{
		polling:     polling,
		consumer:    kconsumer,
		receiver:    receiver,
		unmarshaler: unmarshaler,
		closing:     make(chan struct{}, 0),
	}, nil
}

// Consume initializes giving consumer for message consumption
// from underline kafka consumer.
func (c *Consumer) Consume() error {
	var err error
	c.oncer.Do(func() {
		if err = c.consumer.Subscribe(c.topic, nil); err != nil {
			return
		}

		c.waiter.Add(1)
		go c.run()
	})
	return err
}

// Close ends giving consumer blocking till consumer reading goroutine is fully
// closed.
func (c *Consumer) Close() error {
	select {
	case c.closing <- struct{}{}:
	default:
		return nil
	}

	c.waiter.Wait()
	return c.err
}

// Wait blocks till given consumer is closed returning
// any encountered error if any.
func (c *Consumer) Wait() error {
	c.waiter.Wait()
	return c.err
}

func (c *Consumer) run() {
	defer func() {
		c.waiter.Done()
	}()

	ms := int(c.polling.Seconds()) * 1000

	for {
		select {
		case <-c.closing:
			c.err = c.consumer.Close()
			return
		default:
			if event := c.consumer.Poll(ms); event != nil {
				switch tm := event.(type) {
				case *kafka.Message:
					if err := c.handleIncomingMessage(tm); err != nil {
						// close consumer has the given error is unacceptable.
						c.consumer.Close()

						// attach error to
						c.err = err
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
	return nil
}

//****************************************************************************
// Kafka Publisher
//****************************************************************************

// KAPublisher implements a customized wrapper around the kafka Publisher.
type KAPublisher struct {
	marshaler Marshaler
	closer    chan struct{}
	config    *kafka.ConfigMap
	producer  *kafka.Producer
}

// NewKAPublisher returns a new instance of KAPublisher.
func NewKAPublisher(brokers []string, config kafka.ConfigMap, marshaler Marshaler) (*KAPublisher, error) {
	var kap KAPublisher
	kap.closer = make(chan struct{}, 0)
	kap.config = &kafka.ConfigMap{
		"debug":                        ",",
		"queue.buffering.max.messages": 10000000,
		"queue.buffering.max.kbytes":   2097151,
		"bootstrap.servers":            strings.Join(brokers, ","),
	}

	if err := mergeConfluentConfigs(kap.config, config); err != nil {
		return nil, err
	}

	producer, err := kafka.NewProducer(kap.config)
	if err != nil {
		return nil, err
	}

	kap.producer = producer
	kap.marshaler = marshaler
	return &kap, nil
}

// Close attempts to close giving underline producer.
func (ka *KAPublisher) Close() error {
	select {
	case <-ka.closer:
		return nil
	default:
	}

	ka.producer.Close()
	close(ka.closer)
	return nil
}

// Publish sends giving message envelope  to given topic.
func (ka *KAPublisher) Publish(msg transit.Message) error {
	select {}
	encoded, err := ka.marshaler.Marshal(msg)
	if err != nil {
		return err
	}

	res := make(chan kafka.Event)
	if err := ka.producer.Produce(&encoded, res); err != nil {
		return errors.Wrap(err, "failed to send mesage to producer")
	}

	event := <-res
	kmessage, ok := event.(*kafka.Message)
	if ok {
		return errors.New("failed to recieve *Kafka.Message as event response")
	}

	if kmessage.TopicPartition.Error != nil {
		return errors.Wrap(kmessage.TopicPartition.Error, "failed to deliver message to kafka topic")
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

//****************************************************************************
// Kafka Publisher
//****************************************************************************
