package nats

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/gokit/actorkit/internal"

	"github.com/gokit/actorkit"

	pubsub "github.com/nats-io/go-nats"

	"github.com/gokit/xid"

	"github.com/gokit/actorkit/transit"
	"github.com/gokit/errors"
)

const (
	subIDFormat = "_actorkit_natsio_nats_%s_%d"
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
// Events
//*****************************************************************************

// NATEvent defines a message type for delivery event logs in operations for nats.
type NATEvent struct {
	Header string
	Data   interface{}
}

// Message returns a formatted message for the event.
func (ne NATEvent) Message() string {
	return fmt.Sprintf(ne.Header, ne.Data)
}

//*****************************************************************************
// PubSubFactory
//*****************************************************************************

// PublisherHandler defines a function type which takes a giving PublisherFactory
// and a given topic, returning a new publisher with all related underline specific
// details added and instantiated.
type PublisherHandler func(*PublisherSubscriberFactory, string) (transit.Publisher, error)

// SubscriberHandler defines a function type which takes a giving SubscriptionFactory
// and a given topic, returning a new subscription with all related underline specific
// details added and instantiated.
type SubscriberHandler func(*PublisherSubscriberFactory, string, string, transit.Receiver) (actorkit.Subscription, error)

// PubSubFactoryGenerator returns a function which taken a PublisherSubscriberFactory returning
// a factory for generating publishers and subscribers.
type PubSubFactoryGenerator func(factory *PublisherSubscriberFactory) transit.PubSubFactory

// PubSubFactory provides a partial function for the generation of a transit.PubSubFactory
// using the PubSubFactorGenerator function.
func PubSubFactory(publishers PublisherHandler, subscribers SubscriberHandler) PubSubFactoryGenerator {
	return func(factory *PublisherSubscriberFactory) transit.PubSubFactory {
		return &transit.PubSubFactoryImpl{
			Publishers: func(topic string) (transit.Publisher, error) {
				return publishers(factory, topic)
			},
			Subscribers: func(topic string, id string, receiver transit.Receiver) (actorkit.Subscription, error) {
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
	URL         string
	Options     []pubsub.Option
	Marshaler   transit.Marshaler
	Unmarshaler transit.Unmarshaler
	Log         actorkit.Logs
}

func (c *Config) init() error {
	if c.Log == nil {
		c.Log = &internal.TLog{}
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
		return nil, err
	}

	var pb PublisherSubscriberFactory
	pb.id = xid.New()
	pb.config = config
	pb.topics = map[string]int{}
	pb.pubs = map[string]*Publisher{}
	pb.subs = map[string]*Subscription{}
	pb.ctx, pb.canceler = context.WithCancel(ctx)

	client, err := pubsub.Connect(pb.config.URL, pb.config.Options...)
	if err != nil {
		return &pb, errors.Wrap(err, "Failed to create nats client")
	}

	config.Log.Emit(actorkit.INFO, NATEvent{Header: "Starting NATS client connection: %s", Data: client.ConnectedUrl()})
	config.Log.Emit(actorkit.INFO, NATEvent{Header: "IsConnected: %t", Data: client.IsConnected()})

	pb.c = client
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
	err := pf.c.Drain()
	pf.c.Close()
	return err
}

// Subscribe returns a new subscription for a giving topic which will be used for processing
// messages for giving topic from the NATS streaming provider. If the topic already has a subscriber then
// a subscriber with a ever increasing _id is added and returned, the subscriber receives the giving
// topic_id as durable name for it's subscription.
//
// The id argument is optional and can be left empty.
func (pf *PublisherSubscriberFactory) Subscribe(topic string, id string, receiver func(transit.Message) error, direction func(error) Directive) (*Subscription, error) {
	if sub, ok := pf.getSubscription(topic); ok {
		return sub, nil
	}

	pf.sl.RLock()
	last := pf.topics[topic]
	pf.sl.RUnlock()

	last++

	var sub Subscription
	sub.topic = topic
	sub.client = pf.c
	sub.log = pf.config.Log
	sub.receiver = receiver
	sub.direction = direction
	sub.m = pf.config.Unmarshaler
	sub.errs = make(chan error, 1)

	if id == "" {
		sub.id = fmt.Sprintf(subIDFormat, topic, last)
	} else {
		sub.id = id
	}

	sub.ctx, sub.canceler = context.WithCancel(pf.ctx)
	if err := sub.init(); err != nil {
		return nil, err
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

	pub := NewPublisher(pf.ctx, topic, pf.c, &pf.config)
	pf.addPublisher(pub)

	pf.waiter.Add(1)
	go func() {
		defer pf.waiter.Done()
		pf.config.Log.Emit(actorkit.INFO, NATEvent{Header: "Starting new topic publisher for topic: %q", Data: topic})
		pub.run()
		pf.config.Log.Emit(actorkit.INFO, NATEvent{Header: "Publisher ended for : %q", Data: topic})
	}()

	return pub, nil
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
	actions  chan func()
	sink     *pubsub.Conn
	log      actorkit.Logs
	ctx      context.Context
	m        transit.Marshaler
}

// NewPublisher returns a new instance of a Publisher.
func NewPublisher(ctx context.Context, topic string, sink *pubsub.Conn, config *Config) *Publisher {
	pctx, canceler := context.WithCancel(ctx)
	return &Publisher{
		ctx:      pctx,
		canceler: canceler,
		sink:     sink,
		topic:    topic,
		log:      config.Log,
		m:        config.Marshaler,
		actions:  make(chan func(), 0),
	}
}

// Close closes giving subscriber.
func (p *Publisher) Close() error {
	p.log.Emit(actorkit.INFO, NATEvent{Header: "Closing publisher for %q", Data: p.topic})
	p.canceler()
	return nil
}

// Publish attempts to publish giving message into provided topic publisher returning an
// error for failed attempt.
func (p *Publisher) Publish(msg actorkit.Envelope) error {
	errs := make(chan error, 1)
	action := func() {
		fmt.Printf("Running action\n")
		marshaled, err := p.m.Marshal(msg)
		if err != nil {
			em := errors.Wrap(err, "Failed to marshal incoming message: %%v", msg)
			if p.log != nil {
				p.log.Emit(actorkit.ERROR, transit.MarshalingError{Err: em, Data: msg})
			}
			errs <- em
			return
		}

		fmt.Printf("Sending message: %+q\n", marshaled)

		pubErr := p.sink.Publish(p.topic, marshaled)
		if p.log != nil && pubErr != nil {
			p.log.Emit(actorkit.ERROR, transit.PublishError{Err: errors.WrapOnly(pubErr), Data: marshaled, Topic: p.topic})
		}
		errs <- pubErr
	}

	select {
	case p.actions <- action:
		fmt.Printf("Sending action for execution \n")
		return <-errs
	case <-time.After(10 * time.Millisecond):
		return errors.New("message failed to be published")
	}
}

// Run initializes publishing loop blocking till giving publisher is
// stop/closed or faces an occurred error.
func (p *Publisher) run() {
	for {
		select {
		case <-p.ctx.Done():
			p.log.Emit(actorkit.INFO, NATEvent{Header: "Publisher run is ended %q", Data: p.topic})
			p.log.Emit(actorkit.INFO, NATEvent{Header: "Publisher run is ended %#v", Data: p.ctx.Err()})
			return
		case action := <-p.actions:
			fmt.Printf("Running action\n")
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
	m         transit.Unmarshaler
	sub       *pubsub.Subscription
	direction func(error) Directive
	receiver  func(transit.Message) error
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
		if s.log != nil {
			s.log.Emit(actorkit.ERROR, transit.UnmarshalingError{Err: errors.WrapOnly(err), Data: msg.Data})
		}
		return
	}

	if err := s.receiver(transit.Message{Topic: msg.Subject, Envelope: decoded}); err != nil {
		if s.log != nil {
			s.log.Emit(actorkit.ERROR, transit.MessageHandlingError{Err: errors.WrapOnly(err), Data: msg.Data, Topic: msg.Subject})
		}
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
		if s.log != nil {
			s.log.Emit(actorkit.ERROR, transit.DesubscriptionError{Err: errors.WrapOnly(err), Topic: s.topic})
		}
	}
	if err := s.sub.Drain(); err != nil {
		if s.log != nil {
			s.log.Emit(actorkit.ERROR, transit.OpError{Err: errors.WrapOnly(err), Topic: s.topic})
		}
	}
}

//*********************************************************
//
//*********************************************************
