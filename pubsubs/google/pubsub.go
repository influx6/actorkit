package google

import (
	"context"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/gokit/actorkit"
	"github.com/gokit/actorkit/pubsubs"
	"github.com/gokit/errors"
	"golang.org/x/sync/errgroup"
	"google.golang.org/api/option"
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
	Now       func() time.Time
	Marshaler pubsubs.Marshaler
}

// Marshal marshals giving message into a pubsub message.
func (ps PubSubMarshaler) Marshal(msg pubsubs.Message) (pubsub.Message, error) {
	var res pubsub.Message
	envData, err := ps.Marshaler.Marshal(msg.Envelope)
	if err != nil {
		return res, errors.Wrap(err, "Failed to marshal Envelope")
	}

	res.ID = msg.Envelope.Ref.String()
	headers := map[string]string{
		"topic": msg.Topic,
		"ref":   msg.Envelope.Ref.String(),
	}

	for k, v := range msg.Envelope.Header {
		headers[k] = v
	}

	res.PublishTime = ps.Now()
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
	if _, ok := msg.Attributes["ref"]; !ok {
		return decoded, errors.New("message has no ref header")
	}

	if _, ok := msg.Attributes["topic"]; !ok {
		return decoded, errors.New("message has no topic header")
	}

	decoded.Topic = msg.Attributes["topic"]

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

// PublisherHandler defines a function type which takes a giving PublisherSubscriberFactory
// and a given topic, returning a new publisher with all related underline specific
// details added and instantiated.
type PublisherHandler func(*PublisherSubscriberFactory, string) (pubsubs.Publisher, error)

// SubscriberHandler defines a function type which takes a giving PublisherSubscriberFactory
// and a given topic, returning a new subscription with all related underline specific
// details added and instantiated.
type SubscriberHandler func(*PublisherSubscriberFactory, string, string, pubsubs.Receiver) (pubsubs.Subscription, error)

// PubSubFactoryGenerator returns a function which taken a PublisherSubscriberFactory returning
// a factory for generating publishers and subscribers.
type PubSubFactoryGenerator func(pub *PublisherSubscriberFactory) pubsubs.PubSubFactory

// PubSubFactory provides a partial function for the generation of a pubsubs.PubSubFactory
// using the PubSubFactorGenerator function.
func PubSubFactory(publishers PublisherHandler, subscribers SubscriberHandler) PubSubFactoryGenerator {
	return func(pub *PublisherSubscriberFactory) pubsubs.PubSubFactory {
		var pbs pubsubs.PubSubFactoryImpl
		if publishers != nil {
			pbs.Publishers = func(topic string) (pubsubs.Publisher, error) {
				return publishers(pub, topic)
			}
		}
		if subscribers != nil {
			pbs.Subscribers = func(topic string, id string, receiver pubsubs.Receiver) (pubsubs.Subscription, error) {
				return subscribers(pub, topic, id, receiver)
			}
		}
		return &pbs
	}
}

//*****************************************************************************
// Config
//*****************************************************************************

// Config provides a config struct for instantiating a Publisher type.
type Config struct {
	ProjectID   string
	Log         actorkit.Logs
	Marshaler   Marshaler
	Unmarshaler Unmarshaler

	// MessageDeliveryTimeout is the timeout to wait before response
	// from the underline message broker before timeout.
	MessageDeliveryTimeout time.Duration

	// CreateMissingTopic flags dictates if we will create a topic if
	// it does not already exists in the google cloud.
	CreateMissingTopic bool

	// PublishSettings provided customized publishing settings for google pubsub
	// publisher.
	PublishSettings *pubsub.PublishSettings

	// ClientOptions provide options to be applied to create topic subscribers.
	ClientOptions []option.ClientOption

	// ConsumersCount sets the default consumer count to be used during a subscribers
	// internal operations.
	ConsumersCount int

	// MaxOutStandingMessage defines the maximum allowed message awaiting confirmation
	// for subscriptions.
	MaxOutStandingMessages int

	// MaxOutStandingBytes defines the maximum allowed bytes size awaiting confirmation
	// for subscriptions.
	MaxOutStandingBytes int

	// MaxExtension sets the maximum duration to be provided for message delivery extension.
	MaxExtension time.Duration

	// DefaultSubscriptionConfig sets the default configuration to be used in creating subscriptions.
	// This allows setting default values to be used apart from custom set options during instantiation
	// of a subscription.
	DefaultSubscriptionConfig *pubsub.SubscriptionConfig
}

func (c *Config) init() error {
	if c.Log == nil {
		c.Log = &actorkit.DrainLog{}
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
	if c.ConsumersCount == 0 {
		c.ConsumersCount = runtime.NumCPU()
	}
	return nil
}

//*****************************************************************************
// PublisherSubscriberFactory
//*****************************************************************************

// PublisherSubscriberFactory implements a Google pubsub Publisher and Subscriber/Consumer factory
// which handles creation of publishers and subscribers for topic publishing and consumption.
type PublisherSubscriberFactory struct {
	config Config
	waiter sync.WaitGroup

	canceler func()
	ctx      context.Context
	c        *pubsub.Client
}

// NewPublisherSubscriberFactory returns a new instance of publisher factory.
func NewPublisherSubscriberFactory(ctx context.Context, config Config) (*PublisherSubscriberFactory, error) {
	if err := config.init(); err != nil {
		return nil, err
	}

	var pb PublisherSubscriberFactory
	pb.config = config
	pb.ctx, pb.canceler = context.WithCancel(ctx)

	actorkit.LogMsg("Creating google pubsub pubsub client").
		String("project_id", pb.config.ProjectID).
		WriteDebug(config.Log)

	client, err := pubsub.NewClient(pb.ctx, pb.config.ProjectID, pb.config.ClientOptions...)
	if err != nil {
		err = errors.Wrap(err, "Failed to create google pubsub client")
		actorkit.LogMsg(err.Error()).
			String("project_id", pb.config.ProjectID).
			WriteError(config.Log)
		return &pb, err
	}

	actorkit.LogMsg("Created google pubsub client connection successfully").
		String("project_id", pb.config.ProjectID).
		WriteDebug(config.Log)

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

// Publisher returns giving publisher for giving topic, if provided config
// allows the creation of publisher if not present then a new publisher is created
// for topic and returned, else an error is returned if not found or due to some other
// issues.
func (pf *PublisherSubscriberFactory) Publisher(topic string, setting *pubsub.PublishSettings) (*Publisher, error) {
	actorkit.LogMsg("Creating new publisher for topic").
		String("topic", topic).WriteDebug(pf.config.Log)

	t := pf.c.Topic(topic)
	tExists, err := t.Exists(pf.ctx)
	if err != nil {
		err = errors.Wrap(err, "Failed to get topic %q", topic)
		actorkit.LogMsg(err.Error()).
			String("project_id", pf.config.ProjectID).
			WriteError(pf.config.Log)
		return nil, err
	}

	if !tExists && !pf.config.CreateMissingTopic {
		err = errors.Wrap(err, "topic %q does not exists", topic)
		actorkit.LogMsg(err.Error()).
			String("project_id", pf.config.ProjectID).
			WriteError(pf.config.Log)
		return nil, err
	}

	if !tExists {
		t, err = pf.c.CreateTopic(pf.ctx, topic)
		if err != nil {
			err = errors.Wrap(err, "Failed to create topic %q", topic)
			actorkit.LogMsg(err.Error()).
				String("project_id", pf.config.ProjectID).
				WriteError(pf.config.Log)
			return nil, err
		}

		actorkit.LogMsg("Created new topic in queue").
			String("topic", topic).WriteDebug(pf.config.Log)
	}

	if setting != nil {
		t.PublishSettings = *setting
	}

	if setting == nil && pf.config.PublishSettings != nil {
		t.PublishSettings = *pf.config.PublishSettings
	}

	pub := NewPublisher(pf.ctx, topic, t, &pf.config)
	if err := pub.init(); err != nil {
		err = errors.Wrap(err, "Failed to create publisher %q", topic)
		actorkit.LogMsg(err.Error()).
			String("project_id", pf.config.ProjectID).
			WriteError(pf.config.Log)
		return nil, err
	}

	pf.waiter.Add(1)
	go func() {
		defer pf.waiter.Done()
		pub.Wait()
	}()

	actorkit.LogMsg("Created new publisher for topic").
		String("topic", topic).WriteDebug(pf.config.Log)

	return pub, nil
}

//*****************************************************************************
// Publisher
//*****************************************************************************

// Publisher implements the topic publishing provider for the google pubsub
// layer.
type Publisher struct {
	topic    string
	config   *Config
	m        Marshaler
	error    chan error
	actions  chan func()
	errg     errgroup.Group
	sink     *pubsub.Topic
	ctx      context.Context
	log      actorkit.Logs
	canceler func()
}

// NewPublisher returns a new instance of a Publisher.
func NewPublisher(ctx context.Context, topic string, sink *pubsub.Topic, config *Config) *Publisher {
	pctx, canceler := context.WithCancel(ctx)
	return &Publisher{
		ctx:      pctx,
		config:   config,
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
			err = errors.Wrap(err, "Failed to marshal new message: %%v", msg)
			actorkit.LogMsg(err.Error()).
				String("project_id", p.config.ProjectID).
				WriteError(p.log)
			errs <- err
			return
		}

		actorkit.LogMsg("Publishing new message for topic").
			ObjectJSON("message", marshaled).
			String("topic", p.topic).WriteDebug(p.log)

		result := p.sink.Publish(p.ctx, &marshaled)
		<-result.Ready()

		_, err2 := result.Get(p.ctx)
		if err2 != nil {
			err = errors.Wrap(err2, "Failed to publish new message: %%v", msg)
			actorkit.LogMsg(err.Error()).
				String("project_id", p.config.ProjectID).
				WriteError(p.log)

			errs <- err
			return
		}

		actorkit.LogMsg("Published new message for topic").
			ObjectJSON("message", marshaled).
			String("topic", p.topic).WriteDebug(p.log)

		errs <- nil
	}

	select {
	case p.actions <- action:
		return <-errs
	case <-time.After(p.config.MessageDeliveryTimeout):
		return errors.New("publisher busy, unable to handle message, please retry")
	}
}

// Close closes giving publisher and returns any encountered error.
func (p *Publisher) Close() error {
	p.canceler()
	p.errg.Wait()
	return nil
}

// Wait blocks till publisher is closed.
func (p *Publisher) Wait() {
	p.errg.Wait()
}

func (p *Publisher) init() error {
	p.errg.Go(p.run)
	return nil
}

// Run initializes publishing loop blocking till giving publisher is
// stop/closed or faces an occurred error.
func (p *Publisher) run() error {
	for {
		select {
		case <-p.ctx.Done():
			return nil
		case action, ok := <-p.actions:
			if !ok {
				return nil
			}
			action()
		}
	}
}

//*****************************************************************************
// Subscriber
//*****************************************************************************

// Subscribe subscribes to a giving topic, if one exists then a new subscription with a ever incrementing id is assigned
// to new subscription.
func (pf *PublisherSubscriberFactory) Subscribe(topic string, id string, config *pubsub.SubscriptionConfig, receiver pubsubs.Receiver) (pubsubs.Subscription, error) {
	if topic == "" {
		err := errors.New("topic value can not be empty")
		actorkit.LogMsg(err.Error()).
			String("project_id", pf.config.ProjectID).
			WriteError(pf.config.Log)
		return nil, err
	}

	if id == "" {
		err := errors.New("id value can not be empty")
		actorkit.LogMsg(err.Error()).
			String("project_id", pf.config.ProjectID).
			WriteError(pf.config.Log)
		return nil, err
	}

	return pf.createSubscription(topic, id, config, receiver)
}

func (pf *PublisherSubscriberFactory) createSubscription(topic string, id string, config *pubsub.SubscriptionConfig, receiver pubsubs.Receiver) (*Subscription, error) {
	if config == nil && pf.config.DefaultSubscriptionConfig != nil {
		config = &(*pf.config.DefaultSubscriptionConfig)
	}

	if config == nil && pf.config.DefaultSubscriptionConfig == nil {
		config = &pubsub.SubscriptionConfig{}
	}

	var mid = fmt.Sprintf(pubsubs.SubscriberTopicFormat, "google/pubsub", pf.config.ProjectID, topic, id)
	var pid = transformIDForGooglePubSubFormat(mid)

	actorkit.LogMsg("Creating new subscription for topic").
		String("project_id", pf.config.ProjectID).
		String("topic", topic).
		String("id", id).
		String("mid", mid).
		String("pid", pid).
		WriteDebug(pf.config.Log)

	var newSub Subscription
	newSub.id = mid
	newSub.pid = pid
	newSub.client = pf.c
	newSub.subc = config
	newSub.topic = topic
	newSub.log = pf.config.Log
	newSub.receiver = receiver
	newSub.config = &pf.config

	newSub.ctx, newSub.canceler = context.WithCancel(pf.ctx)
	if err := newSub.init(); err != nil {
		err = errors.Wrap(err, "Failed to create subscription")
		actorkit.LogMsg(err.Error()).
			String("project_id", pf.config.ProjectID).
			String("topic", topic).
			String("id", id).
			WriteError(pf.config.Log)
		return nil, err
	}

	pf.waiter.Add(1)
	go func() {
		defer pf.waiter.Done()
		newSub.Wait()
	}()

	return &newSub, nil
}

//*****************************************************************************
// Subscription
//*****************************************************************************

// Subscription implements a subscriber of a giving topic which is being subscribe to
// for. It implements the pubsubs.Subscription interface.
type Subscription struct {
	id       string
	pid      string
	topic    string
	canceler func()
	config   *Config
	waiter   errgroup.Group
	log      actorkit.Logs
	tx       *pubsub.Topic
	client   *pubsub.Client
	ctx      context.Context
	sub      *pubsub.Subscription
	subc     *pubsub.SubscriptionConfig
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

// Wait blocks till a giving subscription is closed.
func (s *Subscription) Wait() {
	s.waiter.Wait()
}

// Stop ends giving subscription and it's operation in listening to given topic.
func (s *Subscription) Stop() error {
	s.canceler()
	return s.waiter.Wait()
}

func (s *Subscription) init() error {
	actorkit.LogMsg("Initiating new subscription for topic").
		String("project_id", s.config.ProjectID).
		String("topic", s.topic).
		String("id", s.id).
		String("pid", s.pid).
		WriteDebug(s.config.Log)

	sx := s.client.Subscription(s.pid)
	sExists, err := sx.Exists(s.ctx)
	if err != nil {
		err = errors.Wrap(err, "failed to check existence of subscription ID")
		actorkit.LogMsg(err.Error()).
			String("project_id", s.config.ProjectID).
			String("topic", s.topic).
			String("id", s.id).
			String("pid", s.pid).
			WriteError(s.config.Log)
		return err
	}

	if sExists {
		if s.config.MaxExtension > 0 {
			sx.ReceiveSettings.MaxExtension = s.config.MaxExtension
		}

		if s.config.MaxOutStandingBytes > 0 {
			sx.ReceiveSettings.MaxOutstandingBytes = s.config.MaxOutStandingBytes
		}

		if s.config.MaxOutStandingMessages > 0 {
			sx.ReceiveSettings.MaxOutstandingMessages = s.config.MaxOutStandingMessages
		}

		actorkit.LogMsg("Found existing subscription for topic with id").
			String("project_id", s.config.ProjectID).
			String("topic", s.topic).
			String("id", s.id).
			String("pid", s.pid).
			WriteDebug(s.config.Log)

		s.sub = sx
		return nil
	}

	tx := s.client.Topic(s.topic)
	tExists, err := tx.Exists(s.ctx)
	if err != nil {
		err = errors.Wrap(err, "failed to check existence of topic %q", s.topic)
		actorkit.LogMsg(err.Error()).
			String("project_id", s.config.ProjectID).
			String("topic", s.topic).
			String("pid", s.pid).
			String("id", s.id).
			WriteError(s.config.Log)
		return err
	}

	if !tExists {
		err = errors.New("subscription topic %q does not exists", s.topic)
		actorkit.LogMsg(err.Error()).
			String("project_id", s.config.ProjectID).
			String("topic", s.topic).
			String("id", s.id).
			String("pid", s.pid).
			WriteError(s.config.Log)
		return err
	}

	s.subc.Topic = tx
	sx, err = s.client.CreateSubscription(s.ctx, s.pid, *s.subc)
	if err != nil {
		err = errors.Wrap(err, "Failed to create subscription for topic")
		actorkit.LogMsg(err.Error()).
			String("project_id", s.config.ProjectID).
			String("topic", s.topic).
			String("id", s.id).
			String("pid", s.pid).
			WriteError(s.config.Log)
		return err
	}

	if s.config.MaxExtension > 0 {
		sx.ReceiveSettings.MaxExtension = s.config.MaxExtension
	}

	if s.config.MaxOutStandingBytes > 0 {
		sx.ReceiveSettings.MaxOutstandingBytes = s.config.MaxOutStandingBytes
	}

	if s.config.MaxOutStandingMessages > 0 {
		sx.ReceiveSettings.MaxOutstandingMessages = s.config.MaxOutStandingMessages
	}

	s.sub = sx
	s.waiter.Go(s.run)

	actorkit.LogMsg("Created new subscription for topic ready").
		String("project_id", s.config.ProjectID).
		String("topic", s.topic).
		String("id", s.id).
		String("pid", s.pid).
		WriteDebug(s.config.Log)
	return nil
}

func (s *Subscription) run() error {
	if err := s.sub.Receive(s.ctx, func(ctx context.Context, message *pubsub.Message) {
		actorkit.LogMsg("Received new message for topic").
			String("project_id", s.config.ProjectID).
			String("topic", s.topic).
			String("id", s.id).
			String("pid", s.pid).
			WriteDebug(s.config.Log)

		decoded, err := s.config.Unmarshaler.Unmarshal(message)
		if err != nil {
			err = errors.Wrap(err, "Failed to unmarshal message")
			actorkit.LogMsg(err.Error()).
				String("project_id", s.config.ProjectID).
				String("topic", s.topic).
				String("id", s.id).
				WriteError(s.config.Log)

			message.Nack()
			return
		}

		actorkit.LogMsg("Successfully transformed message for reading").
			String("project_id", s.config.ProjectID).
			String("topic", s.topic).
			String("id", s.id).
			String("pid", s.pid).
			ObjectJSON("msg", decoded).
			WriteDebug(s.config.Log)

		action, err := s.receiver(decoded)
		if err != nil {
			err = errors.Wrap(err, "Failed to process message")
			actorkit.LogMsg(err.Error()).
				String("project_id", s.config.ProjectID).
				String("topic", s.topic).
				String("id", s.id).
				WriteError(s.config.Log)

			message.Nack()
			return
		}

		actorkit.LogMsg("Message processed with action").
			String("project_id", s.config.ProjectID).
			ObjectJSON("action", action.String()).
			String("topic", s.topic).
			String("id", s.id).
			String("pid", s.pid).
			WriteDebug(s.config.Log)

		switch action {
		case pubsubs.ACK:
			message.Ack()
		case pubsubs.NACK:
			message.Nack()
		case pubsubs.NOPN:
			return
		}

	}); err != nil {
		err = errors.Wrap(err, "Subscription failed and is unable to be recovered")
		actorkit.LogMsg(err.Error()).
			String("project_id", s.config.ProjectID).
			String("topic", s.topic).
			String("id", s.id).
			WriteError(s.config.Log)
		return err
	}
	return nil
}

func transformIDForGooglePubSubFormat(id string) string {
	return strings.Replace(id, "/", "-", -1)[1:]
}
