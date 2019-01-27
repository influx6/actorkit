package segments

import (
	"context"
	"sync"
	"time"

	"github.com/gokit/actorkit"
	"github.com/gokit/actorkit/pubsubs"
	"github.com/gokit/errors"
	"github.com/gokit/xid"
	segment "github.com/segmentio/kafka-go"
	"golang.org/x/sync/errgroup"
)

//*****************************************************************************
// PubSubFactory
//*****************************************************************************

// PublisherHandler defines a function type which takes a giving PublisherFactory
// and a given topic, returning a new publisher with all related underline specific
// details added and instantiated.
type PublisherHandler func(*PublisherSubscriberFactory, string) (pubsubs.Publisher, error)

// SubscriberHandler defines a function type which takes a giving SubscriptionFactory
// and a given topic, returning a new subscription with all related underline specific
// details added and instantiated.
type SubscriberHandler func(p *PublisherSubscriberFactory, topic string, id string, r pubsubs.Receiver) (pubsubs.Subscription, error)

// QueueGroupSubscriberHandler defines a function type which will return a subscription for a queue group.
type QueueGroupSubscriberHandler func(p *PublisherSubscriberFactory, topic string, group string, id string, r pubsubs.Receiver) (pubsubs.Subscription, error)

// PubSubFactoryGenerator returns a function which taken a PublisherSubscriberFactory returning
// a factory for generating publishers and subscribers.
type PubSubFactoryGenerator func(factory *PublisherSubscriberFactory) pubsubs.PubSubFactory

// PubSubFactory provides a partial function for the generation of a pubsubs.PubSubFactory
// using the PubSubFactorGenerator function.
func PubSubFactory(publishers PublisherHandler, subscribers SubscriberHandler, groupSubscribers QueueGroupSubscriberHandler) PubSubFactoryGenerator {
	return func(factory *PublisherSubscriberFactory) pubsubs.PubSubFactory {
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

//*****************************************************************************
// Marshalers and Unmarshalers
//*****************************************************************************

var (
	_ Marshaler   = &MarshalerWrapper{}
	_ Unmarshaler = &UnmarshalerWrapper{}
)

// Marshaler defines a interface exposing method to transform a pubsubs.Message
// into a kafka message.
type Marshaler interface {
	Marshal(message pubsubs.Message) (segment.Message, error)
}

// Unmarshaler defines an interface who's implementer exposes said method to
// transform a kafka message into a pubsubs Message.
type Unmarshaler interface {
	Unmarshal(segment.Message) (pubsubs.Message, error)
}

// MarshalerWrapper implements the Marshaler interface.
type MarshalerWrapper struct {
	Envelope pubsubs.Marshaler
}

// Marshal implements the Marshaler interface.
func (kc MarshalerWrapper) Marshal(message pubsubs.Message) (segment.Message, error) {
	var newMessage segment.Message
	envelopeBytes, err := kc.Envelope.Marshal(message.Envelope)
	if err != nil {
		return newMessage, err
	}

	newMessage.Key = message.Envelope.Ref.Bytes()
	newMessage.Value = envelopeBytes
	return newMessage, nil
}

// UnmarshalerWrapper implements the Unmarshaler interface.
type UnmarshalerWrapper struct {
	Envelope pubsubs.Unmarshaler
}

// Unmarshal implements the Unmarshaler interface.
func (kc UnmarshalerWrapper) Unmarshal(message segment.Message) (pubsubs.Message, error) {
	var msg pubsubs.Message
	msg.Topic = message.Topic

	var err error
	if msg.Envelope, err = kc.Envelope.Unmarshal(message.Value); err != nil {
		return msg, err
	}

	if string(message.Key) != msg.Envelope.Ref.String() {
		return msg, errors.New("Kafka message ID does not matched envelope ref id")
	}

	return msg, nil
}

//*****************************************************************************
// Publisher
//*****************************************************************************

// Config provides a config struct for instantiating a PublishSubscribeFactory type.
type Config struct {
	Brokers                []string
	MinMessageSize         uint64
	MaxMessageSize         uint64
	AutoCommit             bool
	MessageDeliveryTimeout time.Duration
	MaxAckInterval         time.Duration
	Marshaler              Marshaler
	Unmarshaler            Unmarshaler
	Log                    actorkit.Logs
	Dialer                 *segment.Dialer
	Balancer               segment.Balancer
	Compression            segment.CompressionCodec

	// WriterConfigOverride can be provided to set default
	// configuration values for which will be used for creating writers.
	WriterConfigOverride *segment.WriterConfig

	// ReaderConfigOverride can be provided to set default
	// configuration values for which will be used for creating readers.
	ReaderConfigOverride *segment.ReaderConfig
}

func (c *Config) init() {
	if c.Log == nil {
		c.Log = &actorkit.DrainLog{}
	}
	if c.Balancer == nil {
		c.Balancer = &segment.LeastBytes{}
	}
	if c.MaxAckInterval == 0 {
		c.MaxAckInterval = time.Second
	}
	if c.MinMessageSize == 0 {
		c.MinMessageSize = 10e3
	}
	if c.MaxMessageSize == 0 {
		c.MaxMessageSize = 10e6
	}
	if c.MessageDeliveryTimeout <= 0 {
		c.MessageDeliveryTimeout = 1 * time.Second
	}
}

// PublisherSubscriberFactory implements a Google segment Publisher factory which handles
// creation of publishers for topic publishing and management.
type PublisherSubscriberFactory struct {
	id     xid.ID
	config Config
	waiter sync.WaitGroup

	ctx      context.Context
	canceler func()

	pl   sync.RWMutex
	pubs map[string]*Publisher

	// queue group subscription.
	sl    sync.RWMutex
	gsubs map[string]*Subscription
	subs  map[string]*Subscription
}

// NewPublisherSubscriberFactory returns a new instance of publisher factory.
func NewPublisherSubscriberFactory(ctx context.Context, config Config) (*PublisherSubscriberFactory, error) {
	config.init()

	var pb PublisherSubscriberFactory
	pb.id = xid.New()
	pb.config = config
	pb.pubs = map[string]*Publisher{}
	pb.subs = map[string]*Subscription{}
	pb.gsubs = map[string]*Subscription{}
	pb.ctx, pb.canceler = context.WithCancel(ctx)
	return &pb, nil
}

// Wait blocks till all generated publishers close and have being reclaimed.
func (pf *PublisherSubscriberFactory) Wait() {
	pf.waiter.Wait()
}

// Close closes giving publisher factory and all previous created publishers.
func (pf *PublisherSubscriberFactory) Close() error {
	pf.canceler()
	pf.waiter.Wait()
	return nil
}

// QueueSubscribe returns a new subscription for a giving topic in a given queue group which will be used for processing
// messages for giving topic from the nats streaming provider. If the topic already has a subscriber then
// a subscriber with a ever increasing _id is added and returned if a user defined group id is not set,
// the subscriber receives the giving id as it's queue group name for it's subscription.
//
// Implementation hold's no respect for the id value, it is lost once a subscription is lost.
func (pf *PublisherSubscriberFactory) QueueSubscribe(topic string, grp string, id string, receiver pubsubs.Receiver) (*Subscription, error) {
	if topic == "" {
		return nil, errors.New("topic value can not be empty")
	}

	if grp == "" {
		return nil, errors.New("grp value can not be empty")
	}

	if id == "" {
		return nil, errors.New("id value can not be empty")
	}

	var sub Subscription
	sub.id = id
	sub.group = grp
	sub.queue = true
	sub.topic = topic
	sub.config = &pf.config
	sub.log = pf.config.Log
	sub.receiver = receiver
	sub.ctx, sub.canceler = context.WithCancel(pf.ctx)

	if err := sub.init(); err != nil {
		return nil, errors.Wrap(err, "Failed to create subscription")
	}

	pf.sl.Lock()
	pf.subs[sub.id] = &sub
	pf.sl.Unlock()

	return &sub, nil
}

// Subscribe returns a new subscription for a giving topic which will be used for processing
// messages for giving topic from the nats streaming provider. If the topic already has a subscriber then
// a subscriber with a ever increasing _id is added. The id value is used as a durable name value for the giving
// subscription. If one exists then that is returned.
//
// Implementation hold's no respect for the id value, it is lost once a subscription is lost.
func (pf *PublisherSubscriberFactory) Subscribe(topic string, id string, receiver pubsubs.Receiver) (*Subscription, error) {
	var sub Subscription
	sub.id = id
	sub.topic = topic
	sub.config = &pf.config
	sub.log = pf.config.Log
	sub.receiver = receiver

	sub.ctx, sub.canceler = context.WithCancel(pf.ctx)
	if err := sub.init(); err != nil {
		return nil, errors.Wrap(err, "Failed to create subscription")
	}

	pf.sl.Lock()
	pf.subs[sub.id] = &sub
	pf.sl.Unlock()

	return &sub, nil
}

// Publisher returns giving publisher for giving topic, if provided config
// allows the creation of publisher if not present then a new publisher is created
// for topic and returned, else an error is returned if not found or due to some other
// issues.
func (pf *PublisherSubscriberFactory) Publisher(topic string) (*Publisher, error) {
	if pm, ok := pf.getPublisher(topic); ok {
		return pm, nil
	}

	var wconfig segment.WriterConfig

	if pf.config.WriterConfigOverride != nil {
		wconfig = *pf.config.WriterConfigOverride
	}

	wconfig.Topic = topic
	wconfig.Brokers = pf.config.Brokers

	if pf.config.Dialer != nil {
		wconfig.Dialer = pf.config.Dialer
	}

	if pf.config.Balancer != nil {
		wconfig.Balancer = pf.config.Balancer
	}

	if pf.config.Compression != nil {
		wconfig.CompressionCodec = pf.config.Compression
	}

	pctx, canceler := context.WithCancel(pf.ctx)
	pm := &Publisher{
		factory:  pf,
		ctx:      pctx,
		canceler: canceler,
		topic:    topic,
		cfg:      &pf.config,
		log:      pf.config.Log,
		m:        pf.config.Marshaler,
		actions:  make(chan func(), 0),
		writer:   segment.NewWriter(wconfig),
	}

	pm.waiter.Add(1)
	go pm.run()

	pf.addPublisher(pm)

	pf.waiter.Add(1)
	go func() {
		defer pf.waiter.Done()
		pm.Wait()
	}()

	return pm, nil
}

func (pf *PublisherSubscriberFactory) rmPublisher(pb *Publisher) {
	pf.pl.Lock()
	delete(pf.pubs, pb.topic)
	pf.pl.Unlock()
}

func (pf *PublisherSubscriberFactory) addPublisher(pb *Publisher) {
	pf.pl.Lock()
	pf.pubs[pb.topic] = pb
	pf.pl.Unlock()
}

func (pf *PublisherSubscriberFactory) getPublisher(topic string) (*Publisher, bool) {
	pf.pl.RLock()
	defer pf.pl.RUnlock()
	pm, ok := pf.pubs[topic]
	return pm, ok
}

func (pf *PublisherSubscriberFactory) hasPublisher(topic string) bool {
	pf.pl.RLock()
	defer pf.pl.RUnlock()
	_, ok := pf.pubs[topic]
	return ok
}

func (pf *PublisherSubscriberFactory) getSubscription(id string) (*Subscription, bool) {
	pf.sl.RLock()
	defer pf.sl.RUnlock()
	pm, ok := pf.subs[id]
	return pm, ok
}

func (pf *PublisherSubscriberFactory) hasSubscription(id string) bool {
	pf.sl.RLock()
	defer pf.sl.RUnlock()
	_, ok := pf.subs[id]
	return ok
}

//*****************************************************************************
// Publisher
//*****************************************************************************

// Publisher implements the topic publishing provider for the google segment
// layer.
type Publisher struct {
	topic    string
	canceler func()
	cfg      *Config
	m        Marshaler
	actions  chan func()
	waiter   sync.WaitGroup
	writer   *segment.Writer
	ctx      context.Context
	log      actorkit.Logs
	factory  *PublisherSubscriberFactory
}

// Close closes giving subscriber.
func (p *Publisher) Close() error {
	p.canceler()
	p.waiter.Wait()
	return nil
}

// Wait blocks till the publisher is closed.
func (p *Publisher) Wait() {
	p.waiter.Wait()
}

// Publish attempts to publish giving message into provided topic publisher returning an
// error for failed attempt.
func (p *Publisher) Publish(msg actorkit.Envelope) error {
	errs := make(chan error, 1)
	action := func() {
		p.log.Emit(actorkit.DEBUG, actorkit.LogMsgWithContext("Marshaling message to topic", "context", nil).
			String("topic", p.topic))

		marshaled, err := p.m.Marshal(pubsubs.Message{Topic: p.topic, Envelope: msg})
		if err != nil {
			err = errors.Wrap(err, "Failed to marshal incoming message: %%v", msg)
			p.log.Emit(actorkit.ERROR, actorkit.LogMsgWithContext(err.Error(), "context", nil).
				String("topic", p.topic))
			errs <- err
			return
		}

		p.log.Emit(actorkit.DEBUG, actorkit.LogMsgWithContext("Delivering message to topic", "context", nil).
			String("topic", p.topic).ObjectJSON("data", marshaled))

		pubErr := p.writer.WriteMessages(p.ctx, marshaled)
		if p.log != nil && pubErr != nil {
			p.log.Emit(actorkit.ERROR, actorkit.LogMsgWithContext(pubErr.Error(), "context", nil).
				String("topic", p.topic))
		}

		errs <- pubErr
		if pubErr == nil {
			p.log.Emit(actorkit.DEBUG, actorkit.LogMsgWithContext("Published message to topic", "context", nil).
				String("topic", p.topic))
		}
	}

	select {
	case p.actions <- action:
		return <-errs
	case <-time.After(p.cfg.MessageDeliveryTimeout):
		p.log.Emit(actorkit.ERROR, actorkit.LogMsgWithContext("Failed to deliver message to topic", "context", nil).
			String("topic", p.topic))
		return errors.Wrap(pubsubs.ErrPublishingFailed, "Topic %q", p.topic)
	}
}

// Run initializes publishing loop blocking till giving publisher is
// stop/closed or faces an occurred error.
func (p *Publisher) run() {
	defer func() {
		p.factory.rmPublisher(p)
		p.waiter.Done()
	}()

	for {
		select {
		case <-p.ctx.Done():
			p.log.Emit(actorkit.DEBUG, actorkit.LogMsgWithContext("Publisher routine is closing", "context", nil).
				String("topic", p.topic))
			return
		case action := <-p.actions:
			action()
		}
	}
}

//*****************************************************************************
// Subscriber
//*****************************************************************************

// Subscription implements a subscriber of a giving topic which is being subscribe to
// for. It implements the pubsubs.Subscription interface.
type Subscription struct {
	id       string
	topic    string
	group    string
	queue    bool
	config   *Config
	canceler func()
	ctx      context.Context
	reader   *segment.Reader
	log      actorkit.Logs
	m        Unmarshaler
	errg     errgroup.Group
	receiver pubsubs.Receiver
}

// Topic returns the topic name of giving subscription.
func (s *Subscription) Topic() string {
	return s.topic
}

// Group returns the group or queue group name of giving subscription.
func (s *Subscription) Group() string {
	return s.group
}

// ID returns the identification of giving subscription used for durability if supported.
func (s *Subscription) ID() string {
	return s.id
}

// Error returns any error which was the cause for the stopping of
// subscription, it will block till subscription ends to get error if
// not done, so use carefully.
func (s *Subscription) Error() error {
	return s.errg.Wait()
}

// Stop ends giving subscription and it's operation in listening to given topic.
func (s *Subscription) Stop() error {
	s.canceler()
	return s.errg.Wait()
}

func (s *Subscription) handle(msg segment.Message) {
	decoded, err := s.m.Unmarshal(msg)
	if err != nil {
		s.log.Emit(actorkit.ERROR, actorkit.LogMsgWithContext(err.Error(), "context", nil).
			String("topic", s.topic))
		return
	}

	action, err := s.receiver(decoded)
	if err != nil {
		s.log.Emit(actorkit.ERROR, actorkit.LogMsgWithContext(err.Error(), "context", nil).
			String("subject", msg.Topic).String("topic", s.topic))
	}

	switch action {
	case pubsubs.ACK:
		if err := s.reader.CommitMessages(s.ctx, msg); err != nil {
			err = errors.Wrap(err, "Failed to commit message offset")
			s.log.Emit(actorkit.ERROR, actorkit.LogMsgWithContext(err.Error(), "context", nil).
				String("subject", msg.Topic).String("topic", s.topic))
		}
	case pubsubs.NACK:
		return
	default:
		return
	}
}

func (s *Subscription) init() error {
	var rconfig segment.ReaderConfig

	if s.config.ReaderConfigOverride != nil {
		rconfig = *s.config.ReaderConfigOverride
	}

	rconfig.Topic = s.topic
	rconfig.Brokers = s.config.Brokers
	rconfig.MaxBytes = int(s.config.MaxMessageSize)
	rconfig.MinBytes = int(s.config.MinMessageSize)

	if s.queue {
		rconfig.GroupID = s.group
	}

	var reader = segment.NewReader(rconfig)
	s.reader = reader
	s.errg.Go(s.readLoop)

	return nil
}

func (s *Subscription) run() {
	<-s.ctx.Done()
	if err := s.reader.Close(); err != nil {
		err = errors.Wrap(err, "Failed to close kafka reader")
		s.log.Emit(actorkit.ERROR, actorkit.LogMsgWithContext(err.Error(), "context", nil).
			String("topic", s.topic))
	}
}

func (s *Subscription) readLoop() error {
	var err error
	var m segment.Message
	var autoCommit = s.config.AutoCommit

	for {
		select {
		case <-s.ctx.Done():
			return s.ctx.Err()
		default:
		}

		if autoCommit {
			m, err = s.reader.FetchMessage(s.ctx)
		} else {
			m, err = s.reader.ReadMessage(s.ctx)
		}

		if err != nil {
			return err
		}

		s.handle(m)
	}

	return nil
}
