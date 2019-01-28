package redispb

import (
	"context"
	"fmt"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"

	redis "github.com/go-redis/redis"
	"github.com/gokit/actorkit"
	"github.com/gokit/actorkit/pubsubs"
	"github.com/gokit/errors"
	"github.com/gokit/xid"
)

// ErrBusyPublisher is returned when publisher fails to send a giving message.
var ErrBusyPublisher = errors.New("publisher busy, try again")

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
	ProjectID   string
	Log         actorkit.Logs
	Host        *redis.Options
	Marshaler   pubsubs.Marshaler
	Unmarshaler pubsubs.Unmarshaler

	// MessageDeliveryTimeout is the timeout to wait before response
	// from the underline message broker before timeout.
	MessageDeliveryTimeout time.Duration
}

func (c *Config) init() error {
	if c.Log == nil {
		c.Log = &actorkit.DrainLog{}
	}
	if c.Host == nil {
		return errors.New("Config.Host must be provided")
	}
	if c.Unmarshaler == nil {
		return errors.New("Config.Unmarshaler must be provided")
	}
	if c.Marshaler == nil {
		return errors.New("Config.Marshaler must be provided")
	}
	if c.MessageDeliveryTimeout <= 0 {
		c.MessageDeliveryTimeout = 5 * time.Second
	}
	if c.ProjectID == "" {
		c.ProjectID = actorkit.PackageName
	}
	return nil
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

	// create redis client
	client := redis.NewClient(pb.config.Host)

	actorkit.LogMsg("Creating redis connection").
		String("url", pb.config.Host.Addr).WriteDebug(config.Log)

	// verify that redis server is working with ping-pong.
	status := client.Ping()
	if err := status.Err(); err != nil {
		return nil, errors.Wrap(err, "Failed to connect successfully redis client")
	}

	config.Log.Emit(actorkit.DEBUG, actorkit.LogMsg("Created redis connection").
		String("url", pb.config.Host.Addr))

	pb.client = client
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
	if topic == "" {
		return nil, errors.New("topic value can not be empty")
	}

	if id == "" {
		return nil, errors.New("id value can not be empty")
	}

	var subid = fmt.Sprintf(pubsubs.SubscriberTopicFormat, "redis", pf.config.ProjectID, topic, id)

	actorkit.LogMsg("Subscribing to redis topic").
		String("url", pf.config.Host.Addr).
		String("topic", topic).
		String("id", subid).
		Write(actorkit.DEBUG, pf.config.Log)

	var sub Subscription
	sub.id = subid
	sub.topic = topic
	sub.client = pf.client
	sub.receiver = receiver
	sub.config = &pf.config
	sub.errs = make(chan error, 1)
	sub.ctx, sub.canceler = context.WithCancel(pf.ctx)

	if err := sub.init(); err != nil {
		actorkit.LogMsgWithContext(err.Error(), "context", func(event *actorkit.LogEvent) {
			event.String("topic", topic).String("host", pf.config.Host.Addr)
		}).Write(actorkit.ERROR, pf.config.Log)
		return nil, err
	}

	actorkit.LogMsg("Subscribed to redis topic").
		String("topic", topic).
		String("url", pf.config.Host.Addr).
		String("id", subid).Write(actorkit.DEBUG, pf.config.Log)

	return &sub, nil
}

// Publisher returns giving publisher for giving topic, if provided config
// allows the creation of publisher if not present then a new publisher is created
// for topic and returned, else an error is returned if not found or due to some other
// issues.
func (pf *PublisherSubscriberFactory) Publisher(topic string) (*Publisher, error) {
	actorkit.LogMsg("Creating new publisher to redis topic").
		String("topic", topic).
		String("url", pf.config.Host.Addr).
		Write(actorkit.DEBUG, pf.config.Log)

	pub := NewPublisher(pf.ctx, &pf.config, topic, pf.client, pf.config.Marshaler)
	pf.waiter.Add(1)
	go func() {
		defer pf.waiter.Done()
		pub.run()
	}()

	actorkit.LogMsg("Created new publisher to redis topic").
		String("topic", topic).
		String("url", pf.config.Host.Addr).
		Write(actorkit.DEBUG, pf.config.Log)

	return pub, nil
}

//*****************************************************************************
// Publisher
//*****************************************************************************

// Publisher implements the topic publishing provider for the google redis
// layer.
type Publisher struct {
	topic    string
	config   *Config
	error    chan error
	canceler func()
	actions  chan func()
	sink     *redis.Client
	ctx      context.Context
	log      actorkit.Logs
}

// NewPublisher returns a new instance of a Publisher.
func NewPublisher(ctx context.Context, cfg *Config, topic string, sink *redis.Client, marshaler pubsubs.Marshaler) *Publisher {
	pctx, canceler := context.WithCancel(ctx)
	return &Publisher{
		config:   cfg,
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
		marshaled, err := p.config.Marshaler.Marshal(msg)
		if err != nil {
			err = errors.Wrap(err, "Failed to marshal incoming message: %%v", msg)
			actorkit.LogMsgWithContext(err.Error(), "context", func(event *actorkit.LogEvent) {
				event.String("topic", p.topic).String("", p.config.Host.Addr)
			}).Write(actorkit.ERROR, p.config.Log)

			errs <- err
			return
		}

		actorkit.LogMsg("Sending new message to topic").
			String("topic", p.topic).
			String("url", p.config.Host.Addr).
			QBytes("message", marshaled).
			Write(actorkit.DEBUG, p.config.Log)

		status := p.sink.Publish(p.topic, marshaled)
		if err := status.Err(); err != nil {
			err = errors.Wrap(err, "Failed to publish message")
			actorkit.LogMsgWithContext(err.Error(), "context", func(event *actorkit.LogEvent) {
				event.String("topic", p.topic).String("", p.config.Host.Addr)
			}).Write(actorkit.ERROR, p.config.Log)

			errs <- err
			return
		}

		actorkit.LogMsg("Sent message to topic").
			String("topic", p.topic).
			String("url", p.config.Host.Addr).
			QBytes("message", marshaled).
			Write(actorkit.DEBUG, p.config.Log)

		errs <- nil
	}

	select {
	case p.actions <- action:
		return <-errs
	case <-time.After(p.config.MessageDeliveryTimeout):
		return errors.WrapOnly(ErrBusyPublisher)
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
	config   *Config
	waiter   errgroup.Group
	client   *redis.Client
	sub      *redis.PubSub
	ctx      context.Context
	log      actorkit.Logs
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
	return s.waiter.Wait()
}

func (s *Subscription) handle(msg *redis.Message) {
	payload := []byte(msg.Payload)
	decoded, err := s.config.Unmarshaler.Unmarshal(payload)
	if err != nil {
		err = errors.Wrap(err, "Failed to marshal message")
		actorkit.LogMsgWithContext(err.Error(), "context", func(event *actorkit.LogEvent) {
			event.String("topic", s.topic).String("", s.config.Host.Addr)
		}).Write(actorkit.ERROR, s.config.Log)
		return
	}

	if _, err := s.receiver(pubsubs.Message{Topic: msg.Channel, Envelope: decoded}); err != nil {
		err = errors.Wrap(err, "Failed to process message")
		actorkit.LogMsgWithContext(err.Error(), "context", func(event *actorkit.LogEvent) {
			event.String("topic", s.topic).
				String("", s.config.Host.Addr).
				QBytes("payload", payload).
				String("channel", msg.Channel)
		}).Write(actorkit.ERROR, s.config.Log)
	}
}

func (s *Subscription) init() error {
	s.sub = s.client.Subscribe(s.topic)
	if err := s.sub.Ping(); err != nil {
		return err
	}
	if err := s.sub.Subscribe(s.topic); err != nil {
		return err
	}

	s.waiter.Go(s.run)

	// BUG: It seems we need to give redis a second to prepare,
	// else messages may not be received or be unstable.
	s.awaitReadiness()

	return nil
}

func (s *Subscription) awaitReadiness() {
	<-time.After(1 * time.Millisecond)
}

func (s *Subscription) stopSub() error {
	if err := s.sub.Unsubscribe(s.topic); err != nil {
		err = errors.Wrap(err, "Failed to unsubscribe from topic")
		actorkit.LogMsgWithContext(err.Error(), "context", func(event *actorkit.LogEvent) {
			event.String("topic", s.topic).
				String("", s.config.Host.Addr).
				String("topic", s.topic)
		}).Write(actorkit.ERROR, s.config.Log)
		return err
	}
	return nil
}

func (s *Subscription) run() error {
	receiver := s.sub.Channel()
	closer := s.ctx.Done()

	for {
		select {
		case <-closer:
			return s.stopSub()
		case msg, ok := <-receiver:
			if !ok {
				return s.stopSub()
			}

			s.handle(msg)
		}
	}
}
