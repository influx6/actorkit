package redis

import (
	"context"
	"fmt"
	"sync"

	redis "github.com/go-redis/redis"
	"github.com/gokit/actorkit"
	"github.com/gokit/actorkit/pubsubs"
	"github.com/gokit/errors"
	"github.com/gokit/xid"
)

const (
	subIDFormat = "_actorkit_redis_%s_%d"
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
type SubscriberHandler func(*PublisherSubscriberFactory, string, string, pubsubs.Receiver) (pubsubs.Subscription, error)

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
	URL         string
	Options     redis.Options
	Marshaler   pubsubs.Marshaler
	Unmarshaler pubsubs.Unmarshaler
	Log         actorkit.Logs
}

// PublisherSubscriberFactory implements a Google redis Publisher factory which handles
// creation of publishers for topic publishing and management.
type PublisherSubscriberFactory struct {
	id       xid.ID
	config   Config
	waiter   sync.WaitGroup
	client   *redis.Client
	ctx      context.Context
	canceler func()

	pl   sync.RWMutex
	pubs map[string]*Publisher

	sl     sync.RWMutex
	subs   map[string]*Subscription
	topics map[string]int
}

// NewPublisherSubscriberFactory returns a new instance of publisher factory.
func NewPublisherSubscriberFactory(ctx context.Context, config Config) (*PublisherSubscriberFactory, error) {
	var pb PublisherSubscriberFactory
	pb.id = xid.New()
	pb.config = config
	pb.topics = map[string]int{}
	pb.pubs = map[string]*Publisher{}
	pb.subs = map[string]*Subscription{}
	pb.ctx, pb.canceler = context.WithCancel(ctx)

	// create redis client
	client := redis.NewClient(&pb.config.Options)

	// verify that redis server is working with ping-pong.
	status := client.Ping()
	if err := status.Err(); err != nil {
		return nil, errors.Wrap(err, "Failed to create nats-streaming client")
	}

	fmt.Printf("REDIS-PUBF: %#v\n", client)

	pb.client = client
	fmt.Printf("REDIS-PUBF: %#v\n", pb.client)
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
	return pf.client.Close()
}

// Subscribe returns a new subscription for a giving topic which will be used for processing
// messages for giving topic from the NATS streaming provider. If the topic already has a subscriber then
// a subscriber with a ever increasing _id is added and returned, the subscriber receives the giving
// topic_id as durable name for it's subscription.
func (pf *PublisherSubscriberFactory) Subscribe(topic string, id string, receiver pubsubs.Receiver) (*Subscription, error) {
	//if sub, ok := pf.getSubscription(topic); ok {
	//	return sub, nil
	//}

	fmt.Printf("REDIS-PUBF: %#v\n", pf.client)

	pf.sl.RLock()
	last := pf.topics[topic]
	pf.sl.RUnlock()

	last++

	fmt.Printf("MEM: %#v\n", pf.client)

	var sub Subscription
	sub.topic = topic
	sub.client = pf.client
	sub.receiver = receiver
	sub.m = pf.config.Unmarshaler
	sub.errs = make(chan error, 1)
	sub.ctx, sub.canceler = context.WithCancel(pf.ctx)

	if id == "" {
		sub.id = fmt.Sprintf(subIDFormat, topic, last)
	} else {
		sub.id = id
	}

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

	pub := NewPublisher(pf.ctx, topic, pf.client, pf.config.Marshaler)
	pf.addPublisher(pub)

	pf.waiter.Add(1)
	go func() {
		defer pf.waiter.Done()
		pub.run()
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

// Publisher implements the topic publishing provider for the google redis
// layer.
type Publisher struct {
	topic    string
	error    chan error
	canceler func()
	actions  chan func()
	sink     *redis.Client
	ctx      context.Context
	m        pubsubs.Marshaler
	log      actorkit.Logs
}

// NewPublisher returns a new instance of a Publisher.
func NewPublisher(ctx context.Context, topic string, sink *redis.Client, marshaler pubsubs.Marshaler) *Publisher {
	pctx, canceler := context.WithCancel(ctx)
	return &Publisher{
		ctx:      pctx,
		canceler: canceler,
		sink:     sink,
		topic:    topic,
		error:    make(chan error, 1),
		actions:  make(chan func(), 0),
	}
}

// Close closes giving subscriber.
func (p *Publisher) Close() error {
	p.canceler()
	return nil
}

// Publish attempts to publish giving message into provided topic publisher returning an
// error for failed attempt.
func (p *Publisher) Publish(msg actorkit.Envelope) error {
	errs := make(chan error, 1)
	action := func() {
		marshaled, err := p.m.Marshal(msg)
		if err != nil {
			em := errors.Wrap(err, "Failed to marshal incoming message: %%v", msg)
			if p.log != nil {
				p.log.Emit(actorkit.ERROR, pubsubs.MarshalingError{Err: em, Data: msg})
			}
			errs <- em
			return
		}

		status := p.sink.Publish(p.topic, marshaled)
		if err := status.Err(); err != nil {
			sem := errors.Wrap(err, "Failed to publish message")
			if p.log != nil {
				p.log.Emit(actorkit.ERROR, pubsubs.PublishError{Err: sem, Data: marshaled, Topic: p.topic})
			}

			errs <- sem
			return
		}

		errs <- nil
	}

	select {
	case p.actions <- action:
		return <-errs
	default:
		return errors.New("message failed to be published")
	}
}

// Run initializes publishing loop blocking till giving publisher is
// stop/closed or faces an occured error.
func (p *Publisher) run() {
	for {
		select {
		case <-p.ctx.Done():
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
	errs     chan error
	canceler func()
	waiter   sync.WaitGroup
	client   *redis.Client
	sub      *redis.PubSub
	ctx      context.Context
	log      actorkit.Logs
	m        pubsubs.Unmarshaler
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

// Error returns the associated received error.
func (s *Subscription) Error() error {
	return <-s.errs
}

// Stop ends giving subscription and it's operation in listening to given topic.
func (s *Subscription) Stop() error {
	s.canceler()
	s.waiter.Wait()
	return nil
}

func (s *Subscription) handle(msg *redis.Message) {
	payload := []byte(msg.Payload)
	decoded, err := s.m.Unmarshal(payload)
	if err != nil {
		if s.log != nil {
			s.log.Emit(actorkit.ERROR, pubsubs.UnmarshalingError{Err: errors.Wrap(err, "Failed to marshal message"), Data: payload})
		}
		return
	}

	if _, err := s.receiver(pubsubs.Message{Topic: msg.Channel, Envelope: decoded}); err != nil {
		if s.log != nil {
			s.log.Emit(actorkit.ERROR, pubsubs.MessageHandlingError{Err: errors.Wrap(err, "Failed to process message"), Data: payload, Topic: msg.Channel})
		}
	}
}

func (s *Subscription) init() error {
	s.sub = s.client.Subscribe(s.topic)

	s.waiter.Add(1)
	go s.run()

	return nil
}

func (s *Subscription) stopSub() {
	if err := s.sub.Unsubscribe(s.topic); err != nil {
		if s.log != nil {
			s.log.Emit(actorkit.ERROR, pubsubs.DesubscriptionError{Err: errors.WrapOnly(err), Topic: s.topic})
		}
	}

	s.errs <- s.sub.Close()
}

func (s *Subscription) run() {
	defer s.waiter.Done()
	receiver := s.sub.Channel()
	for {
		select {
		case <-s.ctx.Done():
			s.stopSub()
		case msg, ok := <-receiver:
			if !ok {
				s.stopSub()
				return
			}

			s.handle(msg)
		}
	}
}
