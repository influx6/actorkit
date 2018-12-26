package google

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"time"

	"github.com/gokit/actorkit"

	"cloud.google.com/go/pubsub"
	"github.com/gokit/actorkit/pubsubs"
	"github.com/gokit/errors"
	"google.golang.org/api/option"
)

const (
	pubsubIDName    = "_actorkit_google_pubsub_id"
	pubsubTopicName = "_actorkit_google_pubsub_topic"
	subIDFormat     = "_actorkit_google_pubsub_%s_%d"
)

var (
	_ Marshaler   = &PubSubMarshaler{}
	_ Unmarshaler = &PubSubUnmarshaler{}
)

// Directive defines a int type for representing
// a giving action to be performed due to an error.
type Directive int

// set of possible directives.
const (
	Ack Directive = iota
	Nack
)

// Marshaler defines a interface exposing method to transform a pubsubs.Message
// into a kafka message.
type Marshaler interface {
	Marshal(message pubsubs.Message) (pubsub.Message, error)
}

// PubSubMarshaler implements the Marshaler interface.
type PubSubMarshaler struct {
	Marshaler pubsubs.Marshaler
}

// Marshal marshals giving message into a pubsub message.
func (ps PubSubMarshaler) Marshal(msg pubsubs.Message) (pubsub.Message, error) {
	var res pubsub.Message
	if msg.Envelope.Has(pubsubIDName) {
		return res, errors.New("key %q is reserved for internal use only", pubsubIDName)
	}

	if msg.Envelope.Has(pubsubTopicName) {
		return res, errors.New("key %q is reserved for internal use only", pubsubTopicName)
	}

	envData, err := ps.Marshaler.Marshal(msg.Envelope)
	if err != nil {
		return res, errors.Wrap(err, "Failed to marshal Envelope")
	}

	headers := map[string]string{
		pubsubTopicName: msg.Topic,
		pubsubIDName:    msg.Envelope.Ref.String(),
	}

	for k, v := range msg.Envelope.Header {
		headers[k] = v
	}

	res.Attributes = headers
	res.Data = envData

	return res, nil
}

// Unmarshaler defines an interface who's implementer exposes said method to
// transform a kafka message into a pubsubs Message.
type Unmarshaler interface {
	Unmarshal(*pubsub.Message) (pubsubs.Message, error)
}

// PubSubUnmarshaler implements the Unmarshaler interface.
type PubSubUnmarshaler struct {
	Unmarshaler pubsubs.Unmarshaler
}

// Unmarshal transforms giving pubsub.Message into a pubsubs.Message type.
func (ps *PubSubUnmarshaler) Unmarshal(msg *pubsub.Message) (pubsubs.Message, error) {
	var decoded pubsubs.Message
	if _, ok := msg.Attributes[pubsubIDName]; ok {
		return decoded, errors.New("message is not a actorkit encoded type")
	}

	decoded.Topic = msg.Attributes[pubsubTopicName]

	var err error
	decoded.Envelope, err = ps.Unmarshaler.Unmarshal(msg.Data)
	if err != nil {
		return decoded, errors.Wrap(err, "Failed to decode envelope from message data")
	}

	return decoded, nil
}

//*****************************************************************************
// PubSubFactory
//*****************************************************************************

// PublisherHandler defines a function type which takes a giving PublisherFactory
// and a given topic, returning a new publisher with all related underline specific
// details added and instantiated.
type PublisherHandler func(*PublisherFactory, string) (pubsubs.Publisher, error)

// SubscriberHandler defines a function type which takes a giving SubscriptionFactory
// and a given topic, returning a new subscription with all related underline specific
// details added and instantiated.
type SubscriberHandler func(*SubscriptionFactory, string, string, pubsubs.Receiver) (actorkit.Subscription, error)

// PubSubFactoryGenerator returns a function which taken a PublisherSubscriberFactory returning
// a factory for generating publishers and subscribers.
type PubSubFactoryGenerator func(pub *PublisherFactory, sub *SubscriptionFactory) pubsubs.PubSubFactory

// PubSubFactory provides a partial function for the generation of a pubsubs.PubSubFactory
// using the PubSubFactorGenerator function.
func PubSubFactory(publishers PublisherHandler, subscribers SubscriberHandler) PubSubFactoryGenerator {
	return func(pub *PublisherFactory, sub *SubscriptionFactory) pubsubs.PubSubFactory {
		return &pubsubs.PubSubFactoryImpl{
			Publishers: func(topic string) (pubsubs.Publisher, error) {
				return publishers(pub, topic)
			},
			Subscribers: func(topic string, id string, receiver pubsubs.Receiver) (actorkit.Subscription, error) {
				return subscribers(sub, topic, id, receiver)
			},
		}
	}
}

//*****************************************************************************
// Publisher
//*****************************************************************************

// PublisherConfig provides a config struct for instantiating a Publisher type.
type PublisherConfig struct {
	ProjectID          string
	CreateMissingTopic bool
	Marshaler          Marshaler
	Log                actorkit.Logs
	ClientOptions      []option.ClientOption
	PublishSettings    *pubsub.PublishSettings
}

// PublisherFactory implements a Google pubsub Publisher factory which handles
// creation of publishers for topic publishing and management.
type PublisherFactory struct {
	config PublisherConfig
	waiter sync.WaitGroup

	ctx      context.Context
	canceler func()

	c      *pubsub.Client
	pl     sync.RWMutex
	topics map[string]*Publisher
}

// NewPublisherFactory returns a new instance of publisher factory.
func NewPublisherFactory(ctx context.Context, config PublisherConfig) (*PublisherFactory, error) {
	var pb PublisherFactory
	pb.config = config
	pb.topics = map[string]*Publisher{}
	pb.ctx, pb.canceler = context.WithCancel(ctx)

	client, err := pubsub.NewClient(pb.ctx, pb.config.ProjectID, pb.config.ClientOptions...)
	if err != nil {
		return &pb, errors.Wrap(err, "Failed to create google pubsub client")
	}

	pb.c = client
	return &pb, nil
}

// Wait blocks till all generated publishers close and have being reclaimed.
func (pf *PublisherFactory) Wait() {
	pf.waiter.Wait()
}

// Close closes giving publisher factory and all previous created publishers.
func (pf *PublisherFactory) Close() error {
	pf.canceler()
	pf.waiter.Wait()
	return pf.ctx.Err()
}

// Publisher returns giving publisher for giving topic, if provided config
// allows the creation of publisher if not present then a new publisher is created
// for topic and returned, else an error is returned if not found or due to some other
// issues.
func (pf *PublisherFactory) Publisher(topic string, setting *pubsub.PublishSettings) (*Publisher, error) {
	if pm, ok := pf.getPublisher(topic); ok {
		return pm, nil
	}

	t := pf.c.Topic(topic)

	tExists, err := t.Exists(pf.ctx)
	if err != nil {
		return nil, errors.Wrap(err, "Failed to get topic %q", topic)
	}

	if !tExists && !pf.config.CreateMissingTopic {
		return nil, errors.Wrap(err, "topic %q does not exists", topic)
	}

	if !tExists {
		t, err = pf.c.CreateTopic(pf.ctx, topic)
		if err != nil {
			return nil, errors.Wrap(err, "Failed to create topic %q", topic)
		}
	}

	if setting != nil {
		t.PublishSettings = *setting
	}

	if setting == nil && pf.config.PublishSettings != nil {
		t.PublishSettings = *pf.config.PublishSettings
	}

	pub := NewPublisher(pf.ctx, topic, t, &pf.config)
	pf.addPublisher(pub)

	pf.waiter.Add(1)
	go func() {
		defer pf.waiter.Done()
		pub.run()
	}()

	return pub, nil
}

func (pf *PublisherFactory) addPublisher(pb *Publisher) {
	pf.pl.Lock()
	pf.topics[pb.topic] = pb
	pf.pl.Unlock()
}

func (pf *PublisherFactory) getPublisher(topic string) (*Publisher, bool) {
	pf.pl.RLock()
	defer pf.pl.RUnlock()
	pm, ok := pf.topics[topic]
	return pm, ok
}

func (pf *PublisherFactory) hasPublisher(topic string) bool {
	pf.pl.RLock()
	defer pf.pl.RUnlock()
	_, ok := pf.topics[topic]
	return ok
}

// Publisher implements the topic publishing provider for the google pubsub
// layer.
type Publisher struct {
	topic    string
	m        Marshaler
	error    chan error
	actions  chan func()
	sink     *pubsub.Topic
	ctx      context.Context
	log      actorkit.Logs
	canceler func()
}

// NewPublisher returns a new instance of a Publisher.
func NewPublisher(ctx context.Context, topic string, sink *pubsub.Topic, config *PublisherConfig) *Publisher {
	pctx, canceler := context.WithCancel(ctx)
	return &Publisher{
		ctx:      pctx,
		m:        config.Marshaler,
		log:      config.Log,
		canceler: canceler,
		sink:     sink,
		topic:    topic,
		error:    make(chan error, 1),
		actions:  make(chan func(), 0),
	}
}

// Publish attempts to publish giving message into provided topic publisher returning an
// error for failed attempt.
func (p *Publisher) Publish(msg actorkit.Envelope) error {
	errs := make(chan error, 1)
	action := func() {
		marshaled, err := p.m.Marshal(pubsubs.Message{Topic: p.topic, Envelope: msg})
		if err != nil {
			perr := errors.Wrap(err, "Failed to marshal incoming message: %%v", msg)
			if p.log != nil {
				p.log.Emit(actorkit.ERROR, pubsubs.MarshalingError{Err: perr, Data: msg})
			}
			errs <- perr
			return
		}

		result := p.sink.Publish(p.ctx, &marshaled)
		<-result.Ready()

		_, err2 := result.Get(p.ctx)
		if err2 != nil {
			perr2 := errors.Wrap(err2, "Failed to publish incoming message: %%v", msg)
			if p.log != nil {
				p.log.Emit(actorkit.ERROR, pubsubs.PublishError{Err: perr2, Data: marshaled, Topic: p.topic})
			}

			errs <- perr2
		}
	}

	select {
	case p.actions <- action:
		return <-errs
	default:
		return errors.New("message failed to be published")
	}
}

// Close closes giving publisher and returns any encountered error.
func (p *Publisher) Close() error {
	p.canceler()
	return nil
}

// Run initializes publishing loop blocking till giving publisher is
// stop/closed or faces an occurred error.
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

// SubscriberConfig defines giving configuration settings for google pubsub subscriber.
type SubscriberConfig struct {
	ConsumersCount            int
	ProjectID                 string
	MaxOutStandingMessage     int
	MaxOutStandingBytes       int
	Log                       actorkit.Logs
	MaxExtension              time.Duration
	Unmarshaler               Unmarshaler
	ClientOptions             []option.ClientOption
	DefaultSubscriptionConfig pubsub.SubscriptionConfig
}

// SubscriptionFactory implements a subscription generator which manages differnt
// subscription for given topics to a google pubsub entity.
type SubscriptionFactory struct {
	canceler  func()
	c         *pubsub.Client
	ctx       context.Context
	config    SubscriberConfig
	waiter    sync.WaitGroup
	subwaiter sync.WaitGroup

	actions chan func()
	counter map[string]int
	subs    []Subscription
}

// NewSubscriptionFactory returns a new instance of a SubscriptionFactory.
func NewSubscriptionFactory(ctx context.Context, config SubscriberConfig) (*SubscriptionFactory, error) {
	if config.ConsumersCount == 0 {
		config.ConsumersCount = runtime.NumCPU()
	}

	var sub SubscriptionFactory
	sub.config = config
	sub.actions = make(chan func(), 0)
	sub.ctx, sub.canceler = context.WithCancel(ctx)

	client, err := pubsub.NewClient(sub.ctx, config.ProjectID, config.ClientOptions...)
	if err != nil {
		return nil, errors.Wrap(err, "Failed to create google *pubsub.Client for subscription factory")
	}

	sub.c = client
	sub.waiter.Add(1)
	go sub.run()

	return &sub, nil
}

// Subscribe subscribes to a giving topic, if one exists then a new subscription with a ever incrementing id is assigned
// to new subscription.
func (sb *SubscriptionFactory) Subscribe(topic string, id string, config *pubsub.SubscriptionConfig, receiver func(pubsubs.Message) error, action func(error) Directive) (actorkit.Subscription, error) {
	return sb.createSubscription(topic, id, config, receiver, action)
}

// Wait blocks till all subscription and SubscriptionFactory is closed.
func (sb *SubscriptionFactory) Wait() {
	sb.waiter.Wait()
}

// Close ends giving subscription factory and it's attached subscription,
func (sb *SubscriptionFactory) Close() error {
	sb.canceler()

	err := sb.c.Close()
	sb.subwaiter.Wait()
	sb.waiter.Wait()
	return err
}

func (sb *SubscriptionFactory) createSubscription(topic string, id string, config *pubsub.SubscriptionConfig, receiver func(pubsubs.Message) error, direction func(error) Directive) (*Subscription, error) {
	errs := make(chan error, 1)
	subs := make(chan *Subscription, 1)

	action := func() {
		lastCount := sb.counter[topic]
		lastCount++

		newSubID := fmt.Sprintf(subIDFormat, topic, lastCount)

		var co pubsub.SubscriptionConfig

		if config != nil {
			co = *config
		} else {
			co = sb.config.DefaultSubscriptionConfig
		}

		var newSub Subscription
		newSub.subc = co
		newSub.topic = topic

		if id == "" {
			newSub.id = newSubID
		} else {
			newSub.id = id
		}

		newSub.log = sb.config.Log
		newSub.receiver = receiver
		newSub.config = &sb.config
		newSub.direction = direction
		newSub.ctx, newSub.canceler = context.WithCancel(sb.ctx)

		if err := newSub.init(); err != nil {
			errs <- err
			return
		}

		sb.subwaiter.Add(1)
		go func() {
			defer sb.subwaiter.Done()
			newSub.run()
		}()

		sb.counter[topic] = lastCount
		sb.subs = append(sb.subs, newSub)
		subs <- &newSub
	}

	select {
	case sb.actions <- action:
		select {
		case sub := <-subs:
			return sub, nil
		case err := <-errs:
			return nil, err
		}
	default:
		return nil, errors.New("failed to create subscription %q", topic)
	}
}

func (sb *SubscriptionFactory) hasSubscription(topic string) bool {
	exist := make(chan bool, 1)
	action := func() {
		_, ok := sb.counter[topic]
		exist <- ok
	}

	select {
	case sb.actions <- action:
		return <-exist
	default:
		return false
	}
}

func (sb *SubscriptionFactory) run() {
	defer sb.waiter.Done()

	for {
		select {
		case <-sb.ctx.Done():
			return
		case action := <-sb.actions:
			action()
		}
	}
}

// Subscription implements a subscriber of a giving topic which is being subscribe to
// for. It implements the actorkit.Subscription interface.
type Subscription struct {
	id        string
	topic     string
	canceler  func()
	log       actorkit.Logs
	tx        *pubsub.Topic
	client    *pubsub.Client
	ctx       context.Context
	config    *SubscriberConfig
	sub       *pubsub.Subscription
	direction func(error) Directive
	subc      pubsub.SubscriptionConfig
	receiver  func(pubsubs.Message) error
}

// Stop ends giving subscription and it's operation in listening to given topic.
func (s *Subscription) Stop() {
	s.canceler()
}

func (s *Subscription) init() error {
	sx := s.client.Subscription(s.id)

	sExists, err := sx.Exists(s.ctx)
	if err != nil {
		return errors.Wrap(err, "failed to check existence of subscription id %q", s.id)
	}

	if sExists {
		if s.config.MaxExtension > 0 {
			sx.ReceiveSettings.MaxExtension = s.config.MaxExtension
		}

		if s.config.MaxOutStandingBytes > 0 {
			sx.ReceiveSettings.MaxOutstandingBytes = s.config.MaxOutStandingBytes
		}

		if s.config.MaxOutStandingMessage > 0 {
			sx.ReceiveSettings.MaxOutstandingMessages = s.config.MaxOutStandingMessage
		}

		s.sub = sx
		return nil
	}

	tx := s.client.Topic(s.topic)

	tExists, err := tx.Exists(s.ctx)
	if err != nil {
		return errors.Wrap(err, "failed to check existence of topic %q", s.topic)
	}

	if !tExists {
		return errors.New("subscription topic %q does not exists", s.topic)
	}

	s.subc.Topic = tx
	sx, err = s.client.CreateSubscription(s.ctx, s.id, s.subc)
	if err != nil {
		return err
	}

	if s.config.MaxExtension > 0 {
		sx.ReceiveSettings.MaxExtension = s.config.MaxExtension
	}

	if s.config.MaxOutStandingBytes > 0 {
		sx.ReceiveSettings.MaxOutstandingBytes = s.config.MaxOutStandingBytes
	}

	if s.config.MaxOutStandingMessage > 0 {
		sx.ReceiveSettings.MaxOutstandingMessages = s.config.MaxOutStandingMessage
	}

	s.sub = sx
	return nil
}

func (s *Subscription) run() {
	if err := s.sub.Receive(s.ctx, func(ctx context.Context, message *pubsub.Message) {
		decoded, err := s.config.Unmarshaler.Unmarshal(message)
		if err != nil {
			if s.log != nil {
				s.log.Emit(actorkit.ERROR, pubsubs.UnmarshalingError{Err: errors.WrapOnly(err), Data: message.Data, Topic: s.topic})
			}
			switch s.direction(err) {
			case Ack:
				message.Ack()
			case Nack:
				message.Nack()
			}
			return
		}

		if err := s.receiver(decoded); err != nil {
			if s.log != nil {
				s.log.Emit(actorkit.ERROR, pubsubs.MessageHandlingError{Err: errors.WrapOnly(err), Data: message.Data, Topic: s.topic})
			}
			switch s.direction(err) {
			case Ack:
				message.Ack()
			case Nack:
				message.Nack()
			}
			return
		}

		message.Ack()
	}); err != nil {
		if s.log != nil {
			s.log.Emit(actorkit.ERROR, pubsubs.OpError{Err: errors.WrapOnly(err), Topic: s.topic})
		}
	}
}
