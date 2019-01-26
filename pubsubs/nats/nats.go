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
type SubscriberHandler func(*PublisherSubscriberFactory, string, string, pubsubs.Receiver) (*Subscription, error)

// PubSubFactoryGenerator returns a function which taken a PublisherSubscriberFactory returning
// a factory for generating publishers and subscribers.
type PubSubFactoryGenerator func(factory *PublisherSubscriberFactory) pubsubs.PubSubFactory

// PubSubFactory provides a partial function for the generation of a pubsubs.PubSubFactory
// using the PubSubFactorGenerator function.
func PubSubFactory(publishers PublisherHandler, subscribers SubscriberHandler) PubSubFactoryGenerator {
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
		return &pbs
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

func (c *Config) init() {
	if c.MessageDeliveryTimeout <= 0 {
		c.MessageDeliveryTimeout = 1 * time.Second
	}
	if c.Log == nil {
		c.Log = &actorkit.DrainLog{}
	}
	if c.ProjectID == "" {
		c.ProjectID = actorkit.PackageName
	}
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

	sl   sync.RWMutex
	subs map[string]*Subscription
}

// NewPublisherSubscriberFactory returns a new instance of publisher factory.
func NewPublisherSubscriberFactory(ctx context.Context, config Config) (*PublisherSubscriberFactory, error) {
	config.init()

	var pb PublisherSubscriberFactory
	pb.id = xid.New()
	pb.config = config
	pb.pubs = map[string]*Publisher{}
	pb.subs = map[string]*Subscription{}
	pb.ctx, pb.canceler = context.WithCancel(ctx)

	config.Log.Emit(actorkit.DEBUG, actorkit.LogMsgWithContext("Initiating GNATS client connection", "context", nil).String("url", config.URL))
	client, err := pubsub.Connect(pb.config.URL, pb.config.Options...)
	if err != nil {
		return nil, errors.Wrap(err, "Failed to create nats client")
	}
	config.Log.Emit(actorkit.DEBUG, actorkit.LogMsgWithContext("Checking GNATS client connection status", "context", nil).Bool("connected", client.IsConnected()).String("url", config.URL))

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
// messages for giving topic from the NATS streaming provider. If the id already exists then
// the subscriber is returned.
func (pf *PublisherSubscriberFactory) Subscribe(topic string, id string, receiver pubsubs.Receiver) (*Subscription, error) {
	var subid = fmt.Sprintf(pubsubs.SubscriberTopicFormat, "nats", pf.config.ProjectID, topic, id)
	if sub, ok := pf.getSubscription(subid); ok {
		return sub, nil
	}

	var sub Subscription
	sub.topic = topic
	sub.id = subid
	sub.client = pf.c
	sub.log = pf.config.Log
	sub.receiver = receiver
	sub.m = pf.config.Unmarshaler
	sub.errs = make(chan error, 1)

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

// Wait blocks till the publisher has being closed.
func (p *Publisher) Wait() {
	p.waiter.Wait()
}

// Close closes giving subscriber.
func (p *Publisher) Close() error {
	p.log.Emit(actorkit.DEBUG, actorkit.LogMsgWithContext("Closing publisher", "context", nil).
		String("topic", p.topic))
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
			String("topic", p.topic))

		marshaled, err := p.m.Marshal(msg)
		if err != nil {
			err = errors.Wrap(err, "Failed to marshal incoming message: %%v", msg)
			p.log.Emit(actorkit.ERROR, actorkit.LogMsgWithContext(err.Error(), "context", nil))
			errs <- err
			return
		}

		p.log.Emit(actorkit.DEBUG, actorkit.LogMsgWithContext("Delivery message to topic", "context", nil).
			String("topic", p.topic).QBytes("data", marshaled))

		pubErr := p.sink.Publish(p.topic, marshaled)
		if p.log != nil && pubErr != nil {
			p.log.Emit(actorkit.ERROR, pubsubs.PublishError{Err: errors.WrapOnly(pubErr), Data: marshaled, Topic: p.topic})
			errs <- pubErr
			return
		}

		errs <- nil
		p.log.Emit(actorkit.DEBUG, actorkit.LogMsgWithContext("Published new msg to topic", "context", nil).
			String("topic", p.topic))
	}

	select {
	case p.actions <- action:
		return <-errs
	case <-time.After(p.cfg.MessageDeliveryTimeout):
		err := errors.Wrap(pubsubs.ErrPublishingFailed, "Topic %q", p.topic)
		p.log.Emit(actorkit.ERROR, actorkit.LogMsgWithContext("Failed to deliver message to topic", "context", nil).
			String("topic", p.topic))
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
// for. It implements the actorkit.Subscription interface.
type Subscription struct {
	id       string
	topic    string
	errs     chan error
	canceler func()
	client   *pubsub.Conn
	ctx      context.Context
	log      actorkit.Logs
	m        pubsubs.Unmarshaler
	sub      *pubsub.Subscription
	receiver pubsubs.Receiver
}

// Topic returns the topic name of giving subscription.
func (s *Subscription) Topic() string {
	return s.topic
}

// Group returns the group or queue group name of giving subscription.
func (s *Subscription) Group() string {
	return ""
}

// ID returns the identification of giving subscription used for durability if supported.
func (s *Subscription) ID() string {
	return s.id
}

// Stop ends giving subscription and it's operation in listening to given topic.
func (s *Subscription) Stop() error {
	s.canceler()
	return nil
}

func (s *Subscription) handle(msg *pubsub.Msg) {
	decoded, err := s.m.Unmarshal(msg.Data)
	if err != nil {
		s.log.Emit(actorkit.ERROR, actorkit.LogMsgWithContext(err.Error(), "context", nil).
			String("topic", s.topic))
		return
	}

	if _, err := s.receiver(pubsubs.Message{Topic: msg.Subject, Envelope: decoded}); err != nil {
		s.log.Emit(actorkit.ERROR, actorkit.LogMsgWithContext(err.Error(), "context", nil).
			String("subject", msg.Subject).String("topic", s.topic))
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
			String("topic", s.topic))
	}
	if err := s.sub.Drain(); err != nil {
		if s.log != nil {
			s.log.Emit(actorkit.ERROR, actorkit.LogMsgWithContext(err.Error(), "context", nil).
				String("topic", s.topic))
		}
	}
}
