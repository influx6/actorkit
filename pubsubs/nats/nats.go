package nats

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/gokit/actorkit"

	pubsub "github.com/nats-io/go-nats"

	"github.com/gokit/xid"

	"github.com/gokit/actorkit/pubsubs"
	"github.com/gokit/errors"
)

// Directive defines a int type for representing
// a giving action to be performed due to an error.
type Directive int

// set of possible directives.
const (
	Ack Directive = iota
	Nack
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
type SubscriberHandler func(*PublisherSubscriberFactory, string, string, pubsubs.Receiver) (actorkit.Subscription, error)

// PubSubFactoryGenerator returns a function which taken a PublisherSubscriberFactory returning
// a factory for generating publishers and subscribers.
type PubSubFactoryGenerator func(factory *PublisherSubscriberFactory) pubsubs.PubSubFactory

// PubSubFactory provides a partial function for the generation of a pubsubs.PubSubFactory
// using the PubSubFactorGenerator function.
func PubSubFactory(publishers PublisherHandler, subscribers SubscriberHandler) PubSubFactoryGenerator {
	return func(factory *PublisherSubscriberFactory) pubsubs.PubSubFactory {
		return &pubsubs.PubSubFactoryImpl{
			Publishers: func(topic string) (pubsubs.Publisher, error) {
				return publishers(factory, topic)
			},
			Subscribers: func(topic string, id string, receiver pubsubs.Receiver) (actorkit.Subscription, error) {
				return subscribers(factory, topic, id, receiver)
			},
		}
	}
}

//*****************************************************************************
// Publisher
//*****************************************************************************

// Config provides a config struct for instantiating a Publisher type.
type Config struct {
	URL                    string
	ProjectID              string
	MessageDeliveryTimeout time.Duration
	Options                []pubsub.Option
	Marshaler              pubsubs.Marshaler
	Unmarshaler            pubsubs.Unmarshaler
	Log                    actorkit.Logs
}

func (c *Config) init() error {
	if c.MessageDeliveryTimeout <= 0 {
		c.MessageDeliveryTimeout = 1 * time.Second
	}
	if c.Log == nil {
		c.Log = &actorkit.DrainLog{}
	}
	if c.ProjectID == "" {
		c.ProjectID = "actorkit"
	}
	return nil
}

// PublisherSubscriberFactory implements a Google pubsub Publisher factory which handles
// creation of publishers for topic publishing and management.
type PublisherSubscriberFactory struct {
	id     xid.ID
	config Config
	waiter sync.WaitGroup

	ctx      context.Context
	canceler func()

	c    *pubsub.Conn
	pl   sync.RWMutex
	pubs map[string]*Publisher

	sl     sync.RWMutex
	subs   map[string]*Subscription
	topics map[string]int
}

// NewPublisherSubscriberFactory returns a new instance of publisher factory.
func NewPublisherSubscriberFactory(ctx context.Context, config Config) (*PublisherSubscriberFactory, error) {
	if err := config.init(); err != nil {
		return nil, errors.Wrap(err, "failed to initialize Config instance")
	}

	var pb PublisherSubscriberFactory
	pb.id = xid.New()
	pb.config = config
	pb.topics = map[string]int{}
	pb.pubs = map[string]*Publisher{}
	pb.subs = map[string]*Subscription{}
	pb.ctx, pb.canceler = context.WithCancel(ctx)

	config.Log.Emit(actorkit.DEBUG, actorkit.LogMsgWithContext("Initiating GNATS client connection", "context", nil).String("url", config.URL).Write())
	client, err := pubsub.Connect(pb.config.URL, pb.config.Options...)
	if err != nil {
		return nil, errors.Wrap(err, "Failed to create nats client")
	}
	config.Log.Emit(actorkit.DEBUG, actorkit.LogMsgWithContext("Checking GNATS client connection status", "context", nil).Bool("connected", client.IsConnected()).String("url", config.URL).Write())

	pb.c = client
	return &pb, nil
}

// Wait blocks till all generated publishers close and have being reclaimed.
func (pf *PublisherSubscriberFactory) Wait() {
	pf.waiter.Wait()
}

// Close closes giving publisher factory and all previous created publishers.
func (pf *PublisherSubscriberFactory) Close() error {
	pf.config.Log.Emit(actorkit.DEBUG, actorkit.Message("Closing PublisherSubscriberFactory"))
	pf.canceler()
	pf.waiter.Wait()
	err := pf.c.Drain()
	pf.c.Close()
	pf.config.Log.Emit(actorkit.DEBUG, actorkit.Message("Closed PublisherSubscriberFactory"))
	return err
}

// Subscribe returns a new subscription for a giving topic which will be used for processing
// messages for giving topic from the NATS streaming provider. If the topic already has a subscriber then
// a subscriber with a ever increasing _id is added and returned if a user defined group id is not set,
// the subscriber receives the giving topic_id as durable name for it's subscription if supported.
//
// The id argument is optional and can be left empty.
func (pf *PublisherSubscriberFactory) Subscribe(topic string, id string, receiver pubsubs.Receiver, direction func(error) Directive) (*Subscription, error) {
	if sub, ok := pf.getSubscription(topic); ok {
		return sub, nil
	}

	pf.sl.RLock()
	last := pf.topics[topic]
	pf.sl.RUnlock()

	last++

	if id == "" {
		id = fmt.Sprintf("%d", last)
	}

	var sub Subscription
	sub.topic = topic
	sub.client = pf.c
	sub.log = pf.config.Log
	sub.receiver = receiver
	sub.direction = direction
	sub.m = pf.config.Unmarshaler
	sub.errs = make(chan error, 1)
	sub.id = fmt.Sprintf(pubsubs.SubscriberTopicFormat, pf.config.ProjectID, topic, id)

	sub.ctx, sub.canceler = context.WithCancel(pf.ctx)
	if err := sub.init(); err != nil {
		return nil, errors.Wrap(err, "Failed to create subscription")
	}

	pf.sl.Lock()
	pf.subs[sub.id] = &sub
	pf.topics[topic] = last
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

	pub := NewPublisher(pf.ctx, pf, topic, pf.c, &pf.config)
	pf.addPublisher(pub)

	pf.waiter.Add(1)
	go func() {
		defer pf.waiter.Done()
		pub.Wait()
	}()

	return pub, nil
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

func (pf *PublisherSubscriberFactory) getSubscription(topic string) (*Subscription, bool) {
	pf.sl.RLock()
	defer pf.sl.RUnlock()
	pm, ok := pf.subs[topic]
	return pm, ok
}

func (pf *PublisherSubscriberFactory) hasSubscription(topic string) bool {
	pf.sl.RLock()
	defer pf.sl.RUnlock()
	_, ok := pf.subs[topic]
	return ok
}

//*****************************************************************************
// Publisher
//*****************************************************************************

// Publisher implements the topic publishing provider for the google pubsub
// layer.
type Publisher struct {
	topic    string
	canceler func()
	waiter   sync.WaitGroup
	actions  chan func()
	cfg      *Config
	sink     *pubsub.Conn
	log      actorkit.Logs
	ctx      context.Context
	m        pubsubs.Marshaler
	factory  *PublisherSubscriberFactory
}

// NewPublisher returns a new instance of a Publisher.
func NewPublisher(ctx context.Context, factory *PublisherSubscriberFactory, topic string, sink *pubsub.Conn, config *Config) *Publisher {
	pctx, canc := context.WithCancel(ctx)
	pm := &Publisher{
		cfg:      config,
		ctx:      pctx,
		canceler: canc,
		sink:     sink,
		topic:    topic,
		factory:  factory,
		log:      config.Log,
		m:        config.Marshaler,
		actions:  make(chan func(), 0),
	}

	pm.waiter.Add(1)
	go pm.run()

	return pm
}

func (p *Publisher) Wait() {
	p.waiter.Wait()
}

// Close closes giving subscriber.
func (p *Publisher) Close() error {
	p.log.Emit(actorkit.DEBUG, actorkit.LogMsgWithContext("Closing publisher", "context", nil).
		String("topic", p.topic).Write())
	p.canceler()
	p.waiter.Wait()
	return nil
}

// Publish attempts to publish giving message into provided topic publisher returning an
// error for failed attempt.
func (p *Publisher) Publish(msg actorkit.Envelope) error {
	errs := make(chan error, 1)
	action := func() {
		p.log.Emit(actorkit.DEBUG, actorkit.LogMsgWithContext("Publishing message to topic", "context", nil).
			String("topic", p.topic).Write())

		marshaled, err := p.m.Marshal(msg)
		if err != nil {
			err = errors.Wrap(err, "Failed to marshal incoming message: %%v", msg)
			p.log.Emit(actorkit.ERROR, actorkit.LogMsgWithContext(err.Error(), "context", nil).Write())
			errs <- err
			return
		}

		p.log.Emit(actorkit.DEBUG, actorkit.LogMsgWithContext("Delivery message to topic", "context", nil).
			String("topic", p.topic).QBytes("data", marshaled).Write())

		pubErr := p.sink.Publish(p.topic, marshaled)
		if p.log != nil && pubErr != nil {
			p.log.Emit(actorkit.ERROR, pubsubs.PublishError{Err: errors.WrapOnly(pubErr), Data: marshaled, Topic: p.topic})
			errs <- pubErr
			return
		}

		errs <- nil
		p.log.Emit(actorkit.DEBUG, actorkit.LogMsgWithContext("Published new msg to topic", "context", nil).
			String("topic", p.topic).Write())
	}

	select {
	case p.actions <- action:
		return <-errs
	case <-time.After(p.cfg.MessageDeliveryTimeout):
		err := errors.Wrap(pubsubs.ErrPublishingFailed, "Topic %q", p.topic)
		p.log.Emit(actorkit.ERROR, actorkit.LogMsgWithContext("Failed to deliver message to topic", "context", nil).
			String("topic", p.topic).Write())
		return err
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
				String("topic", p.topic).Write())
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
// for. It implements the actorkit.Subscription interface.
type Subscription struct {
	id        string
	topic     string
	errs      chan error
	canceler  func()
	client    *pubsub.Conn
	ctx       context.Context
	log       actorkit.Logs
	m         pubsubs.Unmarshaler
	sub       *pubsub.Subscription
	direction func(error) Directive
	receiver  pubsubs.Receiver
}

// ID returns the giving durable name for giving subscription.
func (s *Subscription) ID() string {
	return s.id
}

// Stop ends giving subscription and it's operation in listening to given topic.
func (s *Subscription) Stop() {
	s.canceler()
}

func (s *Subscription) handle(msg *pubsub.Msg) {
	decoded, err := s.m.Unmarshal(msg.Data)
	if err != nil {
		s.log.Emit(actorkit.ERROR, actorkit.LogMsgWithContext(err.Error(), "context", nil).
			String("topic", s.topic).Write())
		return
	}

	if _, err := s.receiver(pubsubs.Message{Topic: msg.Subject, Envelope: decoded}); err != nil {
		s.log.Emit(actorkit.ERROR, actorkit.LogMsgWithContext(err.Error(), "context", nil).
			String("subject", msg.Subject).String("topic", s.topic).Write())
	}
}

func (s *Subscription) init() error {
	sub, err := s.client.Subscribe(s.topic, s.handle)
	if err != nil {
		return err
	}

	s.sub = sub
	return nil
}

func (s *Subscription) run() {
	<-s.ctx.Done()
	if err := s.sub.Unsubscribe(); err != nil {
		s.log.Emit(actorkit.ERROR, actorkit.LogMsgWithContext(err.Error(), "context", nil).
			String("topic", s.topic).Write())
	}
	if err := s.sub.Drain(); err != nil {
		if s.log != nil {
			s.log.Emit(actorkit.ERROR, actorkit.LogMsgWithContext(err.Error(), "context", nil).
				String("topic", s.topic).Write())
		}
	}
}
