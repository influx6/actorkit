package pubsubs

import (
	"github.com/gokit/actorkit"
	"github.com/gokit/errors"
)

const (
	// SubscriberTopicFormat defines the expected format for a subscriber group name, queue name can be formulated.
	SubscriberTopicFormat = "/pubsub/%s/project/%s/topics/%s/subscriber/%s"

	// QueueGroupSubscriberTopicFormat defines the expected format for a subscriber queue group name, queue name can be formulated.
	QueueGroupSubscriberTopicFormat = "/pubsub/%s/project/%s/topics/%s/subscriber/%s/%s"
)

var (
	// ErrNotSupported is returned when a giving feature or method has no implementation
	// support.
	ErrNotSupported = errors.New("method not supported")
)

// Message defines a type which embodies a topic to be published to and the associated
// envelope for that topic.
type Message struct {
	Topic    string
	Envelope actorkit.Envelope
}

// NewMessage returns a new instance of a Message with a given topic and envelope.
func NewMessage(topic string, env actorkit.Envelope) Message {
	return Message{
		Topic:    topic,
		Envelope: env,
	}
}

// Marshaler exposes a method to turn an envelope into a byte slice.
type Marshaler interface {
	Marshal(actorkit.Envelope) ([]byte, error)
}

// Unmarshaler exposes a method to turn an byte slice into a envelope.
type Unmarshaler interface {
	Unmarshal([]byte) (actorkit.Envelope, error)
}

//*********************************************************
//  PubSubFactory
//*********************************************************

// PubSubFactory defines an interface which embodies the methods
// exposed for the publishing and subscription of topics and their
// corresponding messages.
type PubSubFactory interface {
	PublisherFactory
	SubscriptionFactory
	QueueGroupSubscriptionFactory
}

//*****************************************************************************
// PubSubFactoryImpl
//*****************************************************************************

// Subscription expects the implementer to provide methods to identify the topic,
// id and group/queueGroup name of giving subscription and a method to stop or end it.
type Subscription interface {
	actorkit.Subscription

	ID() string
	Topic() string
	Group() string
}

// PublisherHandler defines a function type which takes a giving PublisherFactory
// and a given topic, returning a new publisher with all related underline specific
// details added and instantiated.
type PublisherHandler func(string) (Publisher, error)

// SubscriberHandler defines a function type which takes a giving SubscriptionFactory
// and a given topic, returning a new subscription with all related underline specific
// details added and instantiated.
type SubscriberHandler func(topic string, id string, r Receiver) (Subscription, error)

// QueueGroupSubscriberHandler defines a function type which takes a giving SubscriptionFactory
// and a given topic, returning a new subscription for a giving queue group name.
type QueueGroupSubscriberHandler func(group string, topic string, id string, r Receiver) (Subscription, error)

// PubSubFactoryImpl implements the PubSubFactory interface, allowing providing
// custom generator functions which will returning appropriate Publishers and Subscribers
// for some underline platform.
type PubSubFactoryImpl struct {
	Publishers            PublisherHandler
	Subscribers           SubscriberHandler
	QueueGroupSubscribers QueueGroupSubscriberHandler
}

// NewPublisher returns a new Publisher using the Publishers handler function provided.
func (p PubSubFactoryImpl) NewPublisher(topic string) (Publisher, error) {
	if p.Publishers == nil {
		return nil, errors.New("NewPublisher is not supported")
	}
	return p.Publishers(topic)
}

// NewSubscriber returns a new Subscriber using the Subscribers handler function provided.
func (p PubSubFactoryImpl) NewSubscriber(topic string, id string, r Receiver) (Subscription, error) {
	if p.Publishers == nil {
		return nil, errors.New("NewSubscriber is not supported")
	}
	return p.Subscribers(topic, id, r)
}

// NewQueueGroupSubscriber returns a new Subscriber using the Subscribers handler function provided.
func (p PubSubFactoryImpl) NewQueueGroupSubscriber(group string, topic string, id string, r Receiver) (Subscription, error) {
	if p.Publishers == nil {
		return nil, errors.New("NewQueueGroupSubscriber is not supported")
	}
	return p.QueueGroupSubscribers(group, topic, id, r)
}

//*********************************************************
//  PublisherFactory
//*********************************************************

// Publisher exposes a method  for the publishing of a provided message.
type Publisher interface {
	Close() error
	Publish(actorkit.Envelope) error
}

// PublisherFactory exposes a single method for the return of a
// giving publisher for a provided topic.
type PublisherFactory interface {
	NewPublisher(string) (Publisher, error)
}

//*********************************************************
//  SubscriptionFactory
//*********************************************************

// Action defines a giving response to be provided by the processing of
// a message by a Receiver function type.
type Action uint8

func (a Action) String() string {
	switch a {
	case ACK:
		return "ACK"
	case NACK:
		return "NACK"
	case NOPN:
		return "NOPN"
	}
	return "UNKNOWN"
}

// constants of action types
const (
	// ACK is for acknowledging a message received.
	ACK Action = 1 << iota

	// NACK is to not acknowledge or reject a message received.
	NACK

	// NOPN is to request a severe action as dictated by the implementation
	// detail as a action to a giving response/request.
	NOPN
)

// Receiver defines a function type to be used for processing of an incoming message.
type Receiver func(Message) (Action, error)

// SubscriptionFactory exposes a given method for the creation of a subscription.
type SubscriptionFactory interface {
	NewSubscriber(topic string, id string, r Receiver) (Subscription, error)
}

// QueueGroupSubscriptionFactory exposes a given method for the creation of a subscription.
type QueueGroupSubscriptionFactory interface {
	NewQueueGroupSubscriber(string, string, string, Receiver) (Subscription, error)
}

//*********************************************************
//  Error Types
//*********************************************************

// MarshalingError to be used for errors corresponding with marshaling of data.
type MarshalingError struct {
	Topic string
	Err   error
	Data  interface{}
}

// Message implements the actorkit.Logs interface.
func (m MarshalingError) Message() string {
	return m.Err.Error()
}

// UnmarshalingError is to be used for errors relating to deserialization of
// serialized data.
type UnmarshalingError struct {
	Topic string
	Err   error
	Data  interface{}
}

// Message implements the actorkit.Logs interface.
func (m UnmarshalingError) Message() string {
	return m.Err.Error()
}

// OpError is to be used for errors related to publishing giving data.
type OpError struct {
	Topic string
	Err   error
}

// Message implements the actorkit.Logs interface.
func (m OpError) Message() string {
	return m.Err.Error()
}

// PublishError is to be used for errors related to publishing giving data.
type PublishError struct {
	Topic string
	Err   error
	Data  interface{}
}

// Message implements the actorkit.Logs interface.
func (m PublishError) Message() string {
	return m.Err.Error()
}

// MessageHandlingError is to be used for errors related to handling received messages.
type MessageHandlingError struct {
	Topic string
	Err   error
	Data  interface{}
}

// Message implements the actorkit.Logs interface.
func (m MessageHandlingError) Message() string {
	return m.Err.Error()
}

// SubscriptionError defines a giving error struct for subscription error.
type SubscriptionError struct {
	Topic string
	Err   error
}

// Message implements the actorkit.Logs interface.
func (m SubscriptionError) Message() string {
	return m.Err.Error()
}

// DesubscriptionError defines a giving error struct for subscription error.
type DesubscriptionError struct {
	Topic string
	Err   error
}

// Message implements the actorkit.Logs interface.
func (m DesubscriptionError) Message() string {
	return m.Err.Error()
}
