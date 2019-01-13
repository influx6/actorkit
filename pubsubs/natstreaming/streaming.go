package natstreaming

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/gokit/actorkit"

	"github.com/nats-io/go-nats"

	"github.com/gokit/xid"

	"github.com/gokit/actorkit/pubsubs"
	"github.com/gokit/errors"
	pubsub "github.com/nats-io/go-nats-streaming"
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
// Publisher
//*****************************************************************************

// Config provides a config struct for instantiating a Publisher type.
type Config struct {
	URL                    string
	ClusterID              string
	ProjectID              string
	MessageDeliveryTimeout time.Duration
	MaxAckTimeout          time.Duration
	Log                    actorkit.Logs
	Options                []pubsub.Option
	Marshaler              pubsubs.Marshaler
	Unmarshaler            pubsubs.Unmarshaler
	DefaultConn            *nats.Conn
}

func (c *Config) init() {
	if c.Log == nil {
		c.Log = &actorkit.DrainLog{}
	}
	if c.MessageDeliveryTimeout <= 0 {
		c.MessageDeliveryTimeout = 1 * time.Second
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

	c    pubsub.Conn
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

	var ops []pubsub.Option

	if config.DefaultConn != nil {
		ops = append(ops, pubsub.NatsConn(config.DefaultConn))
	}

	if config.DefaultConn == nil && config.URL != "" {
		ops = append(ops, pubsub.NatsURL(config.URL))
	}

	ops = append(ops, pb.config.Options...)

	config.Log.Emit(actorkit.DEBUG, actorkit.LogMsg("Initiating NATS Streaming client connection").String("url", pb.config.URL))
	client, err := pubsub.Connect(pb.config.ClusterID, pb.id.String(), ops...)
	if err != nil {
		return nil, errors.Wrap(err, "Failed to create nats-streaming client")
	}
	config.Log.Emit(actorkit.DEBUG, actorkit.LogMsg("Requesting NATS Streaming client connection status").
		Bool("isConnected", client.NatsConn().IsConnected()).
		String("url", pb.config.URL))

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
	return pf.c.Close()
}

// QueueSubscribe returns a new subscription for a giving topic in a given queue group which will be used for processing
// messages for giving topic from the nats streaming provider. If the topic already has a subscriber then
// a subscriber with a ever increasing _id is added and returned if a user defined group id is not set,
// the subscriber receives the giving id as it's queue group name for it's subscription.
func (pf *PublisherSubscriberFactory) QueueSubscribe(topic string, grp string, id string, receiver pubsubs.Receiver, ops []pubsub.SubscriptionOption) (*Subscription, error) {
	if topic == "" {
		return nil, errors.New("topic value can not be empty")
	}

	if grp == "" {
		return nil, errors.New("grp value can not be empty")
	}

	if id == "" {
		return nil, errors.New("id value can not be empty")
	}

	var subid = fmt.Sprintf(pubsubs.QueueGroupSubscriberTopicFormat, "nats-streaming", pf.config.ProjectID, topic, grp, id)
	if sub, ok := pf.getSubscription(subid); ok {
		return sub, nil
	}

	var sub Subscription
	sub.ops = ops
	sub.id = subid
	sub.group = grp
	sub.queue = true
	sub.topic = topic
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

// Subscribe returns a new subscription for a giving topic which will be used for processing
// messages for giving topic from the nats streaming provider. If the topic already has a subscriber then
// a subscriber with a ever increasing _id is added. The id value is used as a durable name value for the giving
// subscription. If one exists then that is returned.
func (pf *PublisherSubscriberFactory) Subscribe(topic string, id string, receiver pubsubs.Receiver, ops []pubsub.SubscriptionOption) (*Subscription, error) {
	var subid = fmt.Sprintf(pubsubs.SubscriberTopicFormat, "nats-streaming", pf.config.ProjectID, topic, id)
	if sub, ok := pf.getSubscription(subid); ok {
		return sub, nil
	}

	var sub Subscription
	sub.ops = ops
	sub.id = subid
	sub.topic = topic
	sub.client = pf.c
	sub.receiver = receiver
	sub.errs = make(chan error, 1)
	sub.m = pf.config.Unmarshaler

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
	error    chan error
	canceler func()
	actions  chan func()
	waiter   sync.WaitGroup
	sink     pubsub.Conn
	cfg      *Config
	ctx      context.Context
	m        pubsubs.Marshaler
	log      actorkit.Logs
	factory  *PublisherSubscriberFactory
}

// NewPublisher returns a new instance of a Publisher.
func NewPublisher(ctx context.Context, factory *PublisherSubscriberFactory, topic string, sink pubsub.Conn, config *Config) *Publisher {
	pctx, canceler := context.WithCancel(ctx)
	pm := &Publisher{
		ctx:      pctx,
		canceler: canceler,
		sink:     sink,
		topic:    topic,
		factory:  factory,
		cfg:      config,
		log:      config.Log,
		m:        config.Marshaler,
		error:    make(chan error, 1),
		actions:  make(chan func(), 0),
	}

	pm.waiter.Add(1)
	go pm.run()
	return pm
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

		marshaled, err := p.m.Marshal(msg)
		if err != nil {
			err = errors.Wrap(err, "Failed to marshal incoming message: %%v", msg)
			p.log.Emit(actorkit.ERROR, actorkit.LogMsgWithContext(err.Error(), "context", nil).
				String("topic", p.topic))
			errs <- err
			return
		}

		p.log.Emit(actorkit.DEBUG, actorkit.LogMsgWithContext("Delivering message to topic", "context", nil).
			String("topic", p.topic).QBytes("data", marshaled))

		pubErr := p.sink.Publish(p.topic, marshaled)
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
	errs     chan error
	canceler func()
	ctx      context.Context
	client   pubsub.Conn
	log      actorkit.Logs
	ops      []pubsub.SubscriptionOption
	m        pubsubs.Unmarshaler
	sub      pubsub.Subscription
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

// Error returns the associated received error.
func (s *Subscription) Error() error {
	return <-s.errs
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

	action, err := s.receiver(pubsubs.Message{Topic: msg.Subject, Envelope: decoded})
	if err != nil {
		s.log.Emit(actorkit.ERROR, actorkit.LogMsgWithContext(err.Error(), "context", nil).
			String("subject", msg.Subject).String("topic", s.topic))
	}

	switch action {
	case pubsubs.ACK:
		if err := msg.Ack(); err != nil {
			s.log.Emit(actorkit.ERROR, actorkit.LogMsgWithContext(err.Error(), "context", nil).
				String("subject", msg.Subject).String("topic", s.topic))
		}
	case pubsubs.NACK:
		return
	default:
		return
	}
}

func (s *Subscription) init() error {
	ops := append(s.ops, pubsub.DurableName(s.id), pubsub.SetManualAckMode())

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
	if err := s.sub.Unsubscribe(); err != nil {
		s.log.Emit(actorkit.ERROR, actorkit.LogMsgWithContext(err.Error(), "context", nil).
			String("topic", s.topic))
	}
}
