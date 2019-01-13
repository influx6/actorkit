// Package samsara implements a kafka-pubsub provider ontop of the Shopify samara library which provides a performant
// kafka client and publishing foundation.
package samsara

import (
	"context"
	"sync"
	"time"

	"github.com/gokit/actorkit"

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

//****************************************************************************
// Kafka Config
//****************************************************************************

// Config defines configuration fields for use with a PublisherConsumerFactory.
type Config struct {
	Brokers     []string
	ProjectID   string
	Marshaler   Marshaler
	Unmarshaler Unmarshaler
	Version     sarama.KafkaVersion
	Log         actorkit.Logs

	// LogConsumerErrors flags if the errors from the consumer being used for
	// all created consumers should be logged using provided logger.
	LogConsumerErrors bool

	// PollingTime is the time to be used for polling the underline driver for messages.
	PollingTime time.Duration

	// MessageDeliveryTimeout is the timeout to wait before response
	// from the underline message broker before timeout.
	MessageDeliveryTimeout time.Duration

	// ConsumerOverrides is the Sarama.Config to be used for consumers.
	ConsumerOverrides *sarama.Config

	// ProducerOverrides is the Sarama.Config to be used for producers to override the default.
	ProducerOverrides *sarama.Config

	// How long after Nack message should be redelivered.
	NackResendSleep time.Duration

	// How long about unsuccessful reconnecting next reconnect will occur.
	ReconnectRetrySleep time.Duration
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
	if c.NackResendSleep <= 0 {
		c.NackResendSleep = time.Millisecond * 100
	}
	if c.ReconnectRetrySleep <= 0 {
		c.ReconnectRetrySleep = time.Second
	}
	if c.MessageDeliveryTimeout <= 0 {
		c.MessageDeliveryTimeout = time.Second * 2
	}
	if c.Version.String() == "" {
		c.Version = sarama.V0_10_0_0
	}
	return nil
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
type SubscriberHandler func(factory *PublisherConsumerFactory, topic string, id string, r pubsubs.Receiver) (pubsubs.Subscription, error)

// QueueGroupSubscriberHandler defines a function type which returns a subscription for a giving topic and group using a
// provided id as durable name for subscription.
type QueueGroupSubscriberHandler func(factory *PublisherConsumerFactory, topic string, group string, id string, r pubsubs.Receiver) (pubsubs.Subscription, error)

// PubSubFactoryGenerator returns a function which taken a PublisherSubscriberFactory returning
// a factory for generating publishers and subscribers.
type PubSubFactoryGenerator func(*PublisherConsumerFactory) pubsubs.PubSubFactory

// PubSubFactory provides a partial function for the generation of a pubsub.PubSubFactory
// using the PubSubFactorGenerator function.
func PubSubFactory(publishers PublisherHandler, subscribers SubscriberHandler, groupSubscribers QueueGroupSubscriberHandler) PubSubFactoryGenerator {
	return func(factory *PublisherConsumerFactory) pubsubs.PubSubFactory {
		var pbs pubsubs.PubSubFactoryImpl
		if publishers != nil {
			pbs.Publishers = func(topic string) (pubsubs.Publisher, error) {
				return publishers(factory, topic)
			}
		}
		if subscribers != nil {
			pbs.Subscribers = func(topic string, id string, receiver pubsubs.Receiver) (pubsubs.Subscription, error) {
				return subscribers(factory, topic, id, receiver)
			}
		}
		if groupSubscribers != nil {
			pbs.QueueGroupSubscribers = func(topic string, group string, id string, r pubsubs.Receiver) (pubsubs.Subscription, error) {
				return groupSubscribers(factory, topic, group, id, r)
			}
		}
		return &pbs
	}
}

//****************************************************************************
// Kafka Consumer
//****************************************************************************

// SaramaConsumingClient implements the message consuming logic for handling a giving incoming
// kafka client created using the provided server broker list and optional consumer group.
type SaramaConsumingClient struct {
	id            string
	topic         string
	consumerGroup string
	config        *Config
	canceller     func()
	waiter        sync.WaitGroup
	receiver      pubsubs.Receiver
	logs          actorkit.Logs
	kafkaConfig   *sarama.Config
	ctx           context.Context
}

// NewSaramaConsumingClient returns a new instance of a SaramaConsumingClient.
//
// Arguments:
//      parentCtx: parent context to control lifetime of consuming client
//      config: Config instance carrying configuration values for consuming client.
//      topic: The topic to be consumed from the kafka client.
//      cGroupName: The consumer group name to be used for consumption from the streams.
//
func NewSaramaConsumingClient(parentCtx context.Context, config *Config, kafkaConfig *sarama.Config, topic string, cGroupName string, id string, receiver pubsubs.Receiver) (*SaramaConsumingClient, error) {
	childCtx, canceler := context.WithCancel(parentCtx)
	return &SaramaConsumingClient{
		id:            id,
		topic:         topic,
		config:        config,
		receiver:      receiver,
		ctx:           childCtx,
		canceller:     canceler,
		logs:          config.Log,
		kafkaConfig:   kafkaConfig,
		consumerGroup: cGroupName,
	}, nil
}

// Wait blocks till giving consumer client closes.
func (sm *SaramaConsumingClient) Wait() {
	sm.waiter.Wait()
}

// Stop ends and closes subscription, cleaning up all
// generated goroutines and ending it's operation.
func (sm *SaramaConsumingClient) Stop() error {
	sm.canceller()
	sm.waiter.Wait()
	return nil
}

// Topic returns the topic name of giving subscription.
func (sm *SaramaConsumingClient) Topic() string {
	return sm.topic
}

// Group returns the group or queue group name of giving subscription.
func (sm *SaramaConsumingClient) Group() string {
	return sm.consumerGroup
}

// ID returns the identification of giving subscription used for durability if supported.
func (sm *SaramaConsumingClient) ID() string {
	return sm.id
}

// Consume begins listening for new incoming message from provided stream.
func (sm *SaramaConsumingClient) Consume() error {
	client, err := sarama.NewClient(sm.config.Brokers, sm.kafkaConfig)
	if err != nil {
		err = errors.Wrap(err, "Failed to sarama client for topic %q", sm.topic)
		sm.logs.Emit(actorkit.ERROR, actorkit.LogMsgWithContext(err.Error(), "context", func(event actorkit.LogEvent) {
			event.String("topic", sm.topic)
			if sm.consumerGroup != "" {
				event.String("consumer_group", sm.consumerGroup)
			}
		}))
		return err
	}

	if sm.consumerGroup != "" {
		return sm.consumeByGroup(client)
	}
	return sm.consumeByPartitions(client)
}

func (sm *SaramaConsumingClient) consumeByPartitions(client sarama.Client) error {
	partitionConsumer, err := sarama.NewConsumerFromClient(client)
	if err != nil {
		err = errors.Wrap(err, "Failed to sarama client for topic %q", sm.topic)
		sm.logs.Emit(actorkit.ERROR, actorkit.LogMsgWithContext(err.Error(), "context", func(event actorkit.LogEvent) {
			event.String("topic", sm.topic)
			event.String("consumer_group", sm.consumerGroup)
		}))
		return err
	}

	partitions, err := partitionConsumer.Partitions(sm.topic)
	if err != nil {
		err = errors.Wrap(err, "Failed to partition lists for topic %q from client", sm.topic)
		sm.logs.Emit(actorkit.ERROR, actorkit.LogMsgWithContext(err.Error(), "context", func(event actorkit.LogEvent) {
			event.String("topic", sm.topic)
			event.String("consumer_group", sm.consumerGroup)
		}))
		return err
	}

	for _, partition := range partitions {
		ptConsumer, err := partitionConsumer.ConsumePartition(sm.topic, partition, sm.kafkaConfig.Consumer.Offsets.Initial)
		if err != nil {
			err = errors.Wrap(err, "Failed to create partition consumer for topic %q from client", sm.topic)
			sm.logs.Emit(actorkit.ERROR, actorkit.LogMsgWithContext(err.Error(), "context", func(event actorkit.LogEvent) {
				event.String("topic", sm.topic)
				event.String("consumer_group", sm.consumerGroup)
			}))
			return err
		}

		if err := sm.consumePartitionConsumer(client, ptConsumer); err != nil {
			err = errors.Wrap(err, "Failed to consume PartitionConsumer for topic %q from client", sm.topic)
			sm.logs.Emit(actorkit.ERROR, actorkit.LogMsgWithContext(err.Error(), "context", func(event actorkit.LogEvent) {
				event.String("topic", sm.topic)
				event.Int64("partition", int64(partition))
				event.String("consumer_group", sm.consumerGroup)
				event.ObjectJSON("consumer_settings", sm.kafkaConfig.Consumer)
			}))
			return err
		}
	}

	return nil
}

func (sm *SaramaConsumingClient) consumeByGroup(client sarama.Client) error {
	group, err := sarama.NewConsumerGroupFromClient(sm.consumerGroup, client)
	if err != nil {
		err = errors.Wrap(err, "Failed to sarama client for topic %q", sm.topic)
		sm.logs.Emit(actorkit.ERROR, actorkit.LogMsgWithContext(err.Error(), "context", func(event actorkit.LogEvent) {
			event.String("topic", sm.topic)
			event.String("consumer_group", sm.consumerGroup)
		}))
		return err
	}

	if sm.kafkaConfig.Consumer.Return.Errors && sm.config.LogConsumerErrors {
		sm.waiter.Add(1)
		go sm.logConsumerErrors(group.Errors())
	}

	if err := sm.consumeGroupConsumer(client, group); err != nil {
		err = errors.Wrap(err, "Failed to consume GroupConsumer for topic %q from client", sm.topic)
		sm.logs.Emit(actorkit.ERROR, actorkit.LogMsgWithContext(err.Error(), "context", func(event actorkit.LogEvent) {
			event.String("topic", sm.topic)
			event.String("consumer_group", sm.consumerGroup)
			event.ObjectJSON("consumer_settings", sm.kafkaConfig.Consumer)
		}))
		return err
	}
	return nil
}

func (sm *SaramaConsumingClient) consumeGroupConsumer(client sarama.Client, consumer sarama.ConsumerGroup) error {
	sm.waiter.Add(1)
	go func() {
		defer sm.waiter.Done()

		var groupConsumer groupConsumptionHandler
		groupConsumer.client = sm
		if err := consumer.Consume(sm.ctx, []string{sm.topic}, groupConsumer); err != nil {
			err = errors.Wrap(err, "Failed during message consumption for topic %q", sm.topic)
			sm.logs.Emit(actorkit.ERROR, actorkit.LogMsgWithContext(err.Error(), "context", func(event actorkit.LogEvent) {
				event.String("topic", sm.topic)
				if sm.consumerGroup != "" {
					event.String("consumer_group", sm.consumerGroup)
				}
			}))
		}

		if err := consumer.Close(); err != nil {
			err = errors.Wrap(err, "Failed closing group consumer for topic %q", sm.topic)
			sm.logs.Emit(actorkit.ERROR, actorkit.LogMsgWithContext(err.Error(), "context", func(event actorkit.LogEvent) {
				event.String("topic", sm.topic)
				if sm.consumerGroup != "" {
					event.String("consumer_group", sm.consumerGroup)
				}
			}))
		}
	}()
	return nil
}

func (sm *SaramaConsumingClient) consumePartitionConsumer(client sarama.Client, consumer sarama.PartitionConsumer) error {
	sm.waiter.Add(1)
	go func() {
		defer func() {
			if err := consumer.Close(); err != nil {
				err = errors.Wrap(err, "Failed closing group consumer for topic %q", sm.topic)
				sm.logs.Emit(actorkit.ERROR, actorkit.LogMsgWithContext(err.Error(), "context", func(event actorkit.LogEvent) {
					event.String("topic", sm.topic)
					if sm.consumerGroup != "" {
						event.String("consumer_group", sm.consumerGroup)
					}
				}))
			}

			sm.waiter.Done()
		}()

		if sm.kafkaConfig.Consumer.Return.Errors && sm.config.LogConsumerErrors {
			sm.waiter.Add(1)
			go sm.logConsumerErrorsFromMessages(consumer.Errors())
		}

		messages := consumer.Messages()
		for {
			select {
			case <-sm.ctx.Done():
				return
			case msg, ok := <-messages:
				if !ok {
					return
				}

				// if we fail to consume message, immediately stop.
				if err := sm.handleMessage(msg, nil); err != nil {
					err = errors.Wrap(err, "Failed to consume PartitionConsumer for topic %q from client", sm.topic)
					sm.logs.Emit(actorkit.ERROR, actorkit.LogMsgWithContext(err.Error(), "context", func(event actorkit.LogEvent) {
						event.String("topic", sm.topic)
						event.String("consumer_group", sm.consumerGroup)
						event.ObjectJSON("consumer_settings", sm.kafkaConfig.Consumer)
					}))
					return
				}
			}
		}
	}()
	return nil
}

func (sm *SaramaConsumingClient) handleMessage(msg *sarama.ConsumerMessage, sess sarama.ConsumerGroupSession) error {
	sm.logs.Emit(actorkit.INFO, actorkit.LogMsgWithContext("Received new message", "context", func(event actorkit.LogEvent) {
		event.ObjectJSON("msg", msg)
		event.String("topic", sm.topic)
		event.Int64("message.offset", msg.Offset)
		event.String("consumer_group", sm.consumerGroup)
		event.Int64("message.partition", int64(msg.Partition))
		event.ObjectJSON("consumer_settings", sm.kafkaConfig.Consumer)
	}))

	envMsg, err := sm.config.Unmarshaler.Unmarshal(msg)
	if err != nil {
		err = errors.Wrap(err, "Failed to unmarshal *sarama.ConsumerMessage for topic %q", sm.topic)
		sm.logs.Emit(actorkit.ERROR, actorkit.LogMsgWithContext(err.Error(), "context", func(event actorkit.LogEvent) {
			event.String("topic", sm.topic)
			event.String("consumer_group", sm.consumerGroup)
			event.ObjectJSON("consumer_settings", sm.kafkaConfig.Consumer)
		}))
		return err
	}

	sm.logs.Emit(actorkit.DEBUG, actorkit.LogMsgWithContext("Message unmarshalled successfully", "context", func(event actorkit.LogEvent) {
		event.String("topic", sm.topic)
		event.Int64("message.offset", msg.Offset)
		event.String("consumer_group", sm.consumerGroup)
		event.Int64("message.partition", int64(msg.Partition))
		event.ObjectJSON("consumer_settings", sm.kafkaConfig.Consumer)
	}))

	action, err := sm.receiver(envMsg)
	if err != nil {
		err = errors.Wrap(err, "Failed to process message successfully", sm.topic)
		sm.logs.Emit(actorkit.ERROR, actorkit.LogMsgWithContext(err.Error(), "context", func(event actorkit.LogEvent) {
			event.String("topic", sm.topic)
			event.String("consumer_group", sm.consumerGroup)
			event.ObjectJSON("consumer_settings", sm.kafkaConfig.Consumer)
		}))
		return err
	}

	switch action {
	case pubsubs.NACK:
		time.Sleep(sm.config.NackResendSleep)
	case pubsubs.ACK:
		if sess != nil {
			sess.MarkMessage(msg, "")
		}
	case pubsubs.NOPN:
		return errors.New("Failed to process message successfully", sm.topic)
	}

	return nil
}

func (sm *SaramaConsumingClient) logConsumerErrors(errs <-chan error) {
	defer sm.waiter.Done()
	for err := range errs {
		sm.logs.Emit(actorkit.ERROR, actorkit.LogMsgWithContext("SARAMA_CONSUMER_ERROR: "+err.Error(), "context", func(event actorkit.LogEvent) {
			event.String("topic", sm.topic)
			if sm.consumerGroup != "" {
				event.String("consumer_group", sm.consumerGroup)
			}
		}))
	}
}

func (sm *SaramaConsumingClient) logConsumerErrorsFromMessages(errs <-chan *sarama.ConsumerError) {
	defer sm.waiter.Done()
	for err := range errs {
		sm.logs.Emit(actorkit.ERROR, actorkit.LogMsgWithContext("SARAMA_CONSUMER_ERROR: "+err.Err.Error(), "context", func(event actorkit.LogEvent) {
			event.String("topic", err.Topic)
			event.ObjectJSON("consumer_err", err)
			event.Int64("partition", int64(err.Partition))
		}))
	}
}

type groupConsumptionHandler struct {
	client *SaramaConsumingClient
}

func (groupConsumptionHandler) Setup(_ sarama.ConsumerGroupSession) error   { return nil }
func (groupConsumptionHandler) Cleanup(_ sarama.ConsumerGroupSession) error { return nil }
func (h groupConsumptionHandler) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	messages := claim.Messages()

	for {
		select {
		case msg, ok := <-messages:
			if !ok {
				return nil
			}

			// if we fail to consume message, immediately stop.
			if err := h.client.handleMessage(msg, sess); err != nil {
				return err
			}
		case <-h.client.ctx.Done():
			return nil
		}
	}
	return nil
}

//****************************************************************************
// Kafka Publishers
//****************************************************************************

// AsyncPublisher implements a customized wrapper around the kafka AsyncPublisher.
// Delivery actorkit envelopes as messages to designated topics.
//
// The publisher is created to run with the time-life of it's context, once
// the passed in parent context expires, closes or get's cancelled. It
// will also self close.
type AsyncPublisher struct {
	topic       string
	config      *Config
	log         actorkit.Logs
	waiter      sync.WaitGroup
	context     context.Context
	canceler    func()
	kafkaConfig *sarama.Config
	producer    sarama.AsyncProducer
}

// NewAsyncPublisher returns a new instance of AsyncPublisher that uses a synchronouse kafka publisher.
func NewAsyncPublisher(ctx context.Context, config *Config, kafkaConfig *sarama.Config, topic string) (*AsyncPublisher, error) {
	var kap AsyncPublisher
	kap.topic = topic
	kap.config = config
	kap.log = config.Log
	kap.kafkaConfig = kafkaConfig

	rctx, can := context.WithCancel(ctx)
	kap.context = rctx
	kap.canceler = can

	config.Log.Emit(actorkit.DEBUG, actorkit.LogMsgWithContext("Creating new publisher", "context", nil).
		String("topic", topic))

	producer, err := sarama.NewAsyncProducer(config.Brokers, kap.kafkaConfig)
	if err != nil {
		err = errors.Wrap(err, "Failed to create new Producer")
		kap.log.Emit(actorkit.ERROR, actorkit.LogMsgWithContext(err.Error(), "context", nil).With(func(event actorkit.LogEvent) {
			event.String("topic", topic)
		}))
		return nil, err
	}

	kap.log.Emit(actorkit.DEBUG, actorkit.LogMsgWithContext("Created producer for topic", "context", nil).With(func(event actorkit.LogEvent) {
		event.String("topic", topic)
	}))

	kap.producer = producer

	// block until the context get's closed.
	kap.waiter.Add(1)
	go kap.blockUntil()

	return &kap, nil
}

// Close attempts to close giving underline producer.
func (ka *AsyncPublisher) Close() error {
	ka.canceler()
	return nil
}

// Wait blocks till the producer get's closed.
func (ka *AsyncPublisher) Wait() {
	ka.waiter.Wait()
}

// Publish sends giving message envelope  to given topic.
func (ka *AsyncPublisher) Publish(msg actorkit.Envelope) error {
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

	errChan := ka.producer.Errors()
	sendingChan := ka.producer.Input()
	successChan := ka.producer.Successes()

	select {
	case sendingChan <- &encoded:
		select {
		case <-successChan:
			ka.log.Emit(actorkit.DEBUG, actorkit.LogMsgWithContext("published new message", "context", nil).With(func(event actorkit.LogEvent) {
				event.String("topic", ka.topic).ObjectJSON("message", encoded)
			}))
		case err := <-errChan:
			ka.log.Emit(actorkit.ERROR, actorkit.LogMsgWithContext(err.Error(), "context", nil).With(func(event actorkit.LogEvent) {
				event.String("topic", ka.topic).ObjectJSON("message", msg)
			}))
		}
	case <-time.After(ka.config.MessageDeliveryTimeout):
		err := errors.New("timeout: failed to send message")
		ka.log.Emit(actorkit.ERROR, actorkit.LogMsgWithContext(err.Error(), "context", nil).With(func(event actorkit.LogEvent) {
			event.String("topic", ka.topic).ObjectJSON("message", msg)
		}))
		return err
	}

	return nil
}

func (ka *AsyncPublisher) blockUntil() {
	defer ka.waiter.Done()
	<-ka.context.Done()
	if err := ka.producer.Close(); err != nil {
		err = errors.Wrap(err, "Failed to close kafka producer")
		ka.log.Emit(actorkit.ERROR, actorkit.LogMsgWithContext(err.Error(), "context", nil))
	}
}

// SyncPublisher implements a customized wrapper around the kafka SyncPublisher.
// Delivery actorkit envelopes as messages to designated topics.
//
// The publisher is created to run with the time-life of it's context, once
// the passed in parent context expires, closes or get's cancelled. It
// will also self close.
type SyncPublisher struct {
	topic       string
	config      *Config
	log         actorkit.Logs
	waiter      sync.WaitGroup
	context     context.Context
	canceler    func()
	kafkaConfig *sarama.Config
	producer    sarama.SyncProducer
}

// NewSyncPublisher returns a new instance of SyncPublisher that uses a synchronouse kafka publisher.
func NewSyncPublisher(ctx context.Context, config *Config, kafkaConfig *sarama.Config, topic string) (*SyncPublisher, error) {
	var kap SyncPublisher
	kap.topic = topic
	kap.config = config
	kap.log = config.Log
	kap.kafkaConfig = kafkaConfig

	rctx, can := context.WithCancel(ctx)
	kap.context = rctx
	kap.canceler = can

	config.Log.Emit(actorkit.DEBUG, actorkit.LogMsgWithContext("Creating new publisher", "context", nil).
		String("topic", topic))

	producer, err := sarama.NewSyncProducer(config.Brokers, kap.kafkaConfig)
	if err != nil {
		err = errors.Wrap(err, "Failed to create new Producer")
		kap.log.Emit(actorkit.ERROR, actorkit.LogMsgWithContext(err.Error(), "context", nil).With(func(event actorkit.LogEvent) {
			event.String("topic", topic)
		}))
		return nil, err
	}

	kap.log.Emit(actorkit.DEBUG, actorkit.LogMsgWithContext("Created producer for topic", "context", nil).With(func(event actorkit.LogEvent) {
		event.String("topic", topic)
	}))

	kap.producer = producer

	// block until the context get's closed.
	kap.waiter.Add(1)
	go kap.blockUntil()

	return &kap, nil
}

// Close attempts to close giving underline producer.
func (ka *SyncPublisher) Close() error {
	ka.canceler()
	return nil
}

// Wait blocks till the producer get's closed.
func (ka *SyncPublisher) Wait() {
	ka.waiter.Wait()
}

// Publish sends giving message envelope  to given topic.
func (ka *SyncPublisher) Publish(msg actorkit.Envelope) error {
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

	_, _, err = ka.producer.SendMessage(&encoded)
	if err != nil {
		err = errors.Wrap(err, "failed to send message to producer")
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

func (ka *SyncPublisher) blockUntil() {
	defer ka.waiter.Done()
	<-ka.context.Done()
	if err := ka.producer.Close(); err != nil {
		err = errors.Wrap(err, "Failed to close kafka producer")
		ka.log.Emit(actorkit.ERROR, actorkit.LogMsgWithContext(err.Error(), "context", nil))
	}
}

//****************************************************************************
// Kafka PublisherConsumerFactory
//****************************************************************************

// PublisherConsumerFactory implements a central factory for creating publishers or consumers for
// topics for a underline kafka infrastructure.
type PublisherConsumerFactory struct {
	config Config

	waiter       sync.WaitGroup
	rootContext  context.Context
	rootCanceler func()

	cl        sync.RWMutex
	consumers []consumerContract
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

// NewPublisher returns a new Publisher for a giving topic.
func (ka *PublisherConsumerFactory) NewPublisher(topic string, userOverrides *sarama.Config) (*SyncPublisher, error) {
	if userOverrides == nil {
		userOverrides = generateSyncProducerConfig(&ka.config)
	}
	return NewSyncPublisher(ka.rootContext, &ka.config, userOverrides, topic)
}

// NewAsyncPublisher returns a new Publisher for a giving topic using an async publisher.
func (ka *PublisherConsumerFactory) NewAsyncPublisher(topic string, userOverrides *sarama.Config) (*AsyncPublisher, error) {
	if userOverrides == nil {
		userOverrides = generateAsyncProducerConfig(&ka.config)
	}
	return NewAsyncPublisher(ka.rootContext, &ka.config, userOverrides, topic)
}

// NewConsumer return a new consumer for a giving topic to be used for sarama.
// The provided id value if not empty will be used as the group.id.
func (ka *PublisherConsumerFactory) NewConsumer(topic string, id string, receiver pubsubs.Receiver, userOverrides *sarama.Config) (*SaramaConsumingClient, error) {
	return ka.NewGroupConsumer(topic, "", id, receiver, userOverrides)
}

// NewGroupConsumer return a new consumer for a giving topic to be used for sarama under a giving consuming group name using
// provided id and overrides configuration if provided.
func (ka *PublisherConsumerFactory) NewGroupConsumer(topic string, group string, id string, receiver pubsubs.Receiver, userOverrides *sarama.Config) (*SaramaConsumingClient, error) {
	if ka.config.ConsumerOverrides != nil && userOverrides == nil {
		userOverrides = ka.config.ConsumerOverrides
	}

	if userOverrides == nil {
		userOverrides = generateConsumerConfig(id, &ka.config)
	}

	consumer, err := NewSaramaConsumingClient(ka.rootContext, &ka.config, userOverrides, topic, group, id, receiver)
	if err != nil {
		return nil, err
	}

	var errRes = make(chan error, 1)
	var co = consumerContract{consumer: consumer}

	ka.waiter.Add(1)
	go func() {
		defer ka.waiter.Done()
		co.Consume(errRes)
	}()

	if err = <-errRes; err != nil {
		return nil, err
	}

	ka.cl.Lock()
	ka.consumers = append(ka.consumers, co)
	ka.cl.Unlock()

	return consumer, nil
}

type consumerContract struct {
	retries  int
	consumer *SaramaConsumingClient
}

// Consume implements retries logic for a giving SaramaConsumingClient.
func (cc *consumerContract) Consume(errs chan error) {
	// we must first attempt a initial connection and return on error.
	if err := cc.consumer.Consume(); err != nil {
		errs <- err
		return
	}

	errs <- nil

	for {
		cc.consumer.Wait()

		if cc.retries > 0 {
			cc.retries++
			time.Sleep(cc.consumer.config.ReconnectRetrySleep * time.Duration(cc.retries))
		}

		select {
		case <-cc.consumer.ctx.Done():
			return
		default:
		}

		if err := cc.consumer.Consume(); err != nil {
			continue
		}

		cc.retries = 0
	}
}

//****************************************************************************
// internal functions
//****************************************************************************

func generateConsumerConfig(id string, config *Config) *sarama.Config {
	sconfig := sarama.NewConfig()
	sconfig.Version = config.Version
	sconfig.ClientID = config.ProjectID
	sconfig.Consumer.Return.Errors = true
	return sconfig
}

func generateSyncProducerConfig(config *Config) *sarama.Config {
	sconfig := sarama.NewConfig()
	sconfig.Producer.Retry.Max = 10
	sconfig.Version = config.Version
	sconfig.ClientID = config.ProjectID
	sconfig.Producer.Return.Successes = true
	sconfig.Metadata.Retry.Backoff = time.Second * 2
	return sconfig
}

func generateAsyncProducerConfig(config *Config) *sarama.Config {
	sconfig := sarama.NewConfig()
	sconfig.Producer.Retry.Max = 10
	sconfig.Version = config.Version
	sconfig.ClientID = config.ProjectID
	sconfig.Producer.Return.Successes = true
	sconfig.Metadata.Retry.Backoff = time.Second * 2
	return sconfig
}
