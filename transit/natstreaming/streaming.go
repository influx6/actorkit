package natstreaming

import (
	"context"
	"fmt"
	"sync"

	"github.com/nats-io/go-nats"

	"github.com/gokit/xid"

	"github.com/gokit/actorkit/transit"
	"github.com/gokit/errors"
	pubsub "github.com/nats-io/go-nats-streaming"
)

const (
	subIDFormat  = "_actorkit_nats_%s_%d"
	subQIDFormat = "_actorkit_nats_group_%s_%d"
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
// Publisher
//*****************************************************************************

// PublisherConfig provides a config struct for instantiating a Publisher type.
type PublisherConfig struct {
	ClusterID   string
	Options     []pubsub.Option
	Marshaler   transit.Marshaler
	DefaultConn *nats.Conn
}

// PublisherSubscriberFactory implements a Google pubsub Publisher factory which handles
// creation of publishers for topic publishing and management.
type PublisherSubscriberFactory struct {
	id     xid.ID
	config PublisherConfig
	waiter sync.WaitGroup

	ctx      context.Context
	canceler func()

	c    pubsub.Conn
	pl   sync.RWMutex
	pubs map[string]*Publisher

	sl     sync.RWMutex
	subs   map[string]*Subscription
	topics map[string]int
}

// NewPublisherSubscriberFactory returns a new instance of publisher factory.
func NewPublisherSubscriberFactory(ctx context.Context, config PublisherConfig) (*PublisherSubscriberFactory, error) {
	var pb PublisherSubscriberFactory
	pb.id = xid.New()
	pb.config = config
	pb.topics = map[string]int{}
	pb.pubs = map[string]*Publisher{}
	pb.subs = map[string]*Subscription{}
	pb.ctx, pb.canceler = context.WithCancel(ctx)

	var ops []pubsub.Option

	if config.DefaultConn != nil {
		ops = append(ops, pubsub.NatsConn(config.DefaultConn))
	}

	ops = append(ops, pb.config.Options...)

	client, err := pubsub.Connect(pb.config.ClusterID, pb.id.String(), ops...)
	if err != nil {
		return &pb, errors.Wrap(err, "Failed to create nats-streaming client")
	}

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
	return pf.ctx.Err()
}

// QueueSubscribe returns a new subscription for a giving topic in a given queue group which will be used for processing
// messages for giving topic from the nats streaming provider. If the topic already has a subscriber then
// a subscriber with a ever increasing _id is added and returned, the subscriber receives the giving
// topc_id as durable name for it's subscription.
func (pf *PublisherSubscriberFactory) QueueSubscribe(topic string, grp string, receiver func(transit.Message) error, direction func(error) Directive, ops []pubsub.SubscriptionOption) (*Subscription, error) {
	if sub, ok := pf.getSubscription(topic); ok {
		return sub, nil
	}

	pf.sl.RLock()
	last := pf.topics[topic]
	pf.sl.RUnlock()

	last++

	var sub Subscription
	sub.ops = ops
	sub.group = grp
	sub.queue = true
	sub.topic = topic
	sub.client = pf.c
	sub.receiver = receiver
	sub.direction = direction
	sub.errs = make(chan error, 1)
	sub.id = fmt.Sprintf(subQIDFormat, topic, last)
	sub.ctx, sub.canceler = context.WithCancel(sub.ctx)

	if err := sub.init(); err != nil {
		return nil, err
	}

	pf.sl.Lock()
	pf.subs[sub.id] = &sub
	pf.topics[topic] = last
	pf.sl.Unlock()

	return &sub, nil
}

// Subscribe returns a new subscription for a giving topic which will be used for processing
// messages for giving topic from the nats streaming provider. If the topic already has a subscriber then
// a subscriber with a ever increasing _id is added and returned, the subscriber receives the giving
// topc_id as durable name for it's subscription.
func (pf *PublisherSubscriberFactory) Subscribe(topic string, receiver func(transit.Message) error, direction func(error) Directive, ops []pubsub.SubscriptionOption) (*Subscription, error) {
	if sub, ok := pf.getSubscription(topic); ok {
		return sub, nil
	}

	pf.sl.RLock()
	last := pf.topics[topic]
	pf.sl.RUnlock()

	last++

	var sub Subscription
	sub.ops = ops
	sub.topic = topic
	sub.client = pf.c
	sub.receiver = receiver
	sub.direction = direction
	sub.errs = make(chan error, 1)
	sub.id = fmt.Sprintf(subIDFormat, topic, last)
	sub.ctx, sub.canceler = context.WithCancel(sub.ctx)

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

	pub := NewPublisher(pf.ctx, topic, pf.c, pf.config.Marshaler)
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

// Publisher implements the topic publishing provider for the google pubsub
// layer.
type Publisher struct {
	topic    string
	error    chan error
	canceler func()
	actions  chan func()
	sink     pubsub.Conn
	ctx      context.Context
	m        transit.Marshaler
}

// NewPublisher returns a new instance of a Publisher.
func NewPublisher(ctx context.Context, topic string, sink pubsub.Conn, marshaler transit.Marshaler) *Publisher {
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
func (p *Publisher) Publish(msg transit.Message) error {
	if msg.Topic != p.topic {
		return errors.New("invalid message topic %q to publisher of topic %q", msg.Topic, p.topic)
	}

	errs := make(chan error, 1)
	action := func() {
		marshalled, err := p.m.Marshal(msg.Envelope)
		if err != nil {
			errs <- errors.Wrap(err, "Failed to marshal incoming message: %%v", msg)
			return
		}

		errs <- p.sink.Publish(msg.Topic, marshalled)
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
// for. It implements the actorkit.Subscription interface.
type Subscription struct {
	id        string
	topic     string
	group     string
	queue     bool
	errs      chan error
	canceler  func()
	ctx       context.Context
	client    pubsub.Conn
	ops       []pubsub.SubscriptionOption
	m         transit.Unmarshaler
	sub       pubsub.Subscription
	direction func(error) Directive
	receiver  func(transit.Message) error
}

// ID returns the giving durable name for giving subscription.
func (s *Subscription) ID() string {
	return s.id
}

// Error returns the associated received error.
func (s *Subscription) Error() error {
	return <-s.errs
}

// Stop ends giving subscription and it's operation in listening to given topic.
func (s *Subscription) Stop() {
	s.canceler()
}

func (s *Subscription) handle(msg *pubsub.Msg) {
	decoded, err := s.m.Unmarshal(msg.Data)
	if err != nil {
		switch s.direction(err) {
		case Ack:
			msg.Ack()
		case Nack:
		}
		return
	}

	if err := s.receiver(transit.Message{Topic: msg.Subject, Envelope: decoded}); err != nil {
		switch s.direction(err) {
		case Ack:
			msg.Ack()
		case Nack:
		}
		return
	}

	msg.Ack()
}

func (s *Subscription) init() error {
	ops := append(s.ops, pubsub.DurableName(s.id))

	var err error
	var sub pubsub.Subscription
	if s.queue {
		sub, err = s.client.QueueSubscribe(s.topic, s.group, s.handle, ops...)
	} else {
		sub, err = s.client.Subscribe(s.topic, s.handle, ops...)
	}

	if err != nil {
		return err
	}

	s.sub = sub
	return nil
}

func (s *Subscription) run() {
	<-s.ctx.Done()
	s.sub.Unsubscribe()
}
