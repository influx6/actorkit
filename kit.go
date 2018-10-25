package actorkit

import (
	"time"

	"github.com/gokit/xid"
)

//***************************************************************************
// Header
//***************************************************************************

// Header defines a map type to hold meta information associated with a Envelope.
type Header map[string]string

// Get returns the associated value from the map within the map.
func (m Header) Get(n string) string {
	return m[n]
}

// Map returns a map with contents of header.
func (m Header) Map() map[string]string {
	mv := make(map[string]string, len(m))
	for k, v := range m {
		mv[k] = v
	}
	return mv
}

// Len returns the length of records within the meta.
func (m Header) Len() int {
	return len(m)
}

// Has returns true/false value if key is present.
func (m Header) Has(n string) bool {
	_, ok := m[n]
	return ok
}

//***************************************************************************
// Envelope
//***************************************************************************

// Envelope defines a message to be delivered to a giving
// target destination from another giving source with headers
// and data specific to giving message.
type Envelope struct {
	Header
	Sender Addr
	Ref    xid.ID
	Data   interface{}
}

// CreateEnvelope returns a new instance of an envelope with provided arguments.
func CreateEnvelope(sender Addr, header Header, data interface{}) Envelope {
	return Envelope{
		Data:   data,
		Ref:    xid.New(),
		Header: header,
		Sender: sender,
	}
}

//***********************************
//  Actor
//***********************************

// Actor defines a entity which is the single unit of work/computation.
// It embodies the idea of processing, storage and communication. It is the
// means for work to be done.
//
// Actors as axioms/rules which are:
//
// 1. It can receive a message and create an actor to process giving message.
// 2. It can send messages to actors it has addresses it has before.
// 3. It can designate what to do will the next message to be received.
//
// Actors are defined by the behaviour they embody and use, their are simply
// the management husk for this user defined behaviour and by this behaviors
// all the operations a actor can perform is governed. Usually an actors behaviour
// is represented by it's address, which means an actor can in one instance
// usually have similar address with only difference be their unique id when
// within the same tree ancestry and only differ in the service they offer or
// can be be the same actor offering different services based on the behaviour
// it provides.
type Actor interface {
	Waiter
	Spawner
	Ancestry
	Watchable
	Receiver
	Identity
	Escalator
	Stoppable
	Destroyable
	Discovery
	Startable
	Descendants
	Restartable
	Addressable
	MailboxOwner
}

//***********************************
//  Descendants
//***********************************

// Descendants exposes a single method which returns
// addresses of all children address of implementer.
type Descendants interface {
	Children() []Addr
}

//***********************************
//  Waiter
//***********************************

// Waiter exposes a single method which blocks
// till a given condition is met.
type Waiter interface {
	Wait()
}

//***********************************
//  ErrWaiter
//***********************************

// ErrWaiter exposes a single method which blocks
// till a given condition is met or an error occurs that
// causes it to stop blocking and will return the error
// encountered.
type ErrWaiter interface {
	Wait() error
}

//***********************************
//  MailboxOwner
//***********************************

// MailboxOwner exposes a single method to retrieve an implementer's Mailbox.
type MailboxOwner interface {
	Mailbox() Mailbox
}

//***********************************
//  Ancestor
//***********************************

// Ancestry defines a single method to get the parent actor
// of a giving actor.
type Ancestry interface {
	// Parent is supposed to return the immediate parent of giving
	// Actor.
	Parent() Actor

	// Ancestor is supposed to return the root parent of all actors
	// within chain.
	Ancestor() Actor
}

//***********************************
//  Identity
//***********************************

// Identity provides a method to return the ID of a process.
type Identity interface {
	ID() string
}

//***********************************
//  Addressable
//***********************************

// Addressable defines an interface which exposes a method for retrieving
// associated address of implementer.
type Addressable interface {
	Addr() string
}

// AddressService exposes a single method to locate given address for a target
// value, service or namespace.
type AddressService interface {
	AddressOf(string, bool) (Addr, error)
}

//***********************************
//  Behaviour
//***********************************

// Behaviour defines an interface that exposes a method
// that indicate a giving action to be done.
type Behaviour interface {
	Action(Addr, Envelope)
}

//***********************************
//  Receiver
//***********************************

// Receiver defines an interface that exposes methods
// to receive envelopes and it's own used address.
type Receiver interface {
	Receive(Addr, Envelope) error
}

//***********************************
//  Escalator
//***********************************

// Escalator defines an interface defines a method provided
// specifically for handle two cases of error:
//
// 1. Normal errors which occur as process operation life cycle
// 2. Critical errors which determine stability of system and ops.
//
// Normal errors will be raised while critical errors will get escalated.
// this means that escalated errors will be parsed up the tree to an actors
// supervisor and parent.
type Escalator interface {
	Escalate(interface{}, Addr)
}

// Escalatable exposes a single method to escalate a given value up the implementers
// handling tree.
type Escalatable interface {
	Escalate(interface{})
}

//***********************************
//  Watchers
//***********************************

// Subscription defines a method which exposes a single method
// to remove giving subscription.
type Subscription interface {
	Stop()
}

// Watchable defines a in interface that exposes methods to add
// functions to be called on some status change of the implementing
// instance.
type Watchable interface {
	Watch(func(interface{})) Subscription
}

//***********************************
//  Discovery
//***********************************

// Discovery defines an interface that resolves
// a giving address to it's target Actor returning
// actor if found. It accepts a flag which can be
// used to indicate wiliness to search ancestral
// trees.
type Discovery interface {
	Discover(service string, ancestral bool) (Addr, error)
}

// DiscoveryService defines an interface which will return
// a giving Actor for a desired service.
//
// DiscoveryServices provides a great way for adding service or actor discovery
// patterns where we can hook in means of communicating and creating references
// to remote actors without any complicated setup.
//
// DiscoveryServices also provide the means of templated actors, where actors with
// behaviors is already defined by a generating function called 'Templated Functions'.
// Templated functions always return a new actor when called and provide a nice means
// of having a guaranteed behaviour produced for a giving service namespace,
//
//
type DiscoveryService interface {
	Discover(service string) (Actor, error)
}

// DiscoveryChain defines a method which adds giving
// Discovery into underline chain else returns an
// error if not possible.
// Discovery has a very important rule, whoever has
// record of giving actor is parent and supervisor
// of said actor. Even if discovery was requested
// at the lowest end, if ancestral search was enabled
// and a higher parent provided such actor, then that
// parent should naturally be supervisor of that actor.
type DiscoveryChain interface {
	AddDiscovery(service DiscoveryService) error
}

//***********************************
//  Destroyable
//***********************************

// Destroyable defines an interface that exposes a method
// which destroys it's internal process.
type Destroyable interface {
	Destroy(interface{}) ErrWaiter
}

//***********************************
//  Startable
//***********************************

// Startable defines an interface that exposes a method
// which returns a ErrWaiter to indicate completion of
// start process.
type Startable interface {
	Start(interface{}) ErrWaiter
}

// StartState defines a set of methods to be called on
// a start.
type StartState interface {
	PreStart(interface{}) error
	PostStart(interface{}) error
}

//***********************************
//  Restartable
//***********************************

// Restartable defines an interface that exposes a method
// which returns a ErrWaiter to indicate completion of
// restart.
type Restartable interface {
	Restart(interface{}) ErrWaiter
}

// RestartState defines a set of methods to be called on
// a restart.
type RestartState interface {
	PreRestart(interface{}) error
	PostRestart(interface{}) error
}

//***********************************
//  Stoppable
//***********************************

// Stoppable defines an interface
type Stoppable interface {
	// Stopped returns true/false if giving value had stopped.
	Stopped() bool

	// Stop will immediately stop the target process regardless of
	// pending operation.
	Kill(interface{}) error

	// Stop will immediately stop the target regardless of
	// pending operation and returns a ErrWaiter to await end.
	Stop(interface{}) ErrWaiter
}

// StopState defines a set of methods to be called on
// a stop.
type StopState interface {
	PreStop(interface{}) error
	PostStop(interface{}) error
}

//***********************************
// Spawner
//***********************************

// Spawner exposes a single method to spawn an underline actor returning
// the address for spawned actor.
type Spawner interface {
	Spawn(string, Behaviour) (Addr, error)
}

//***********************************
// Addr
//***********************************

// Addr represent a advertised capability and behavior which an actor provides, it is
// possible for one actor to exhibit ability of processing multiple operations/behaviors
// by being able to be expressed using different service addresses. Address simply express
// a reference handle by which an actor able to provide said service can be communicated with.
//
// Interaction of one service to another is always through an address, which makes them a common
// concept that can be transferable between zones, distributed system and networks.
//
// Addr by their nature can have a one-to-many and many-to-many relations with actors.
//
// A single actor can have multiple addresses pointing to it, based on different services it can render
// or even based on same service type, more so one address can be a means of communicating with multiple
// actors in the case of clustering or distributing messaging through a proxy address.
//
type Addr interface {
	Sender
	Service
	Spawner
	Identity
	Watchable
	Descendants
	Addressable
	Escalatable
	AncestralAddr
	AddressService
}

//***********************************
//  Addressable
//***********************************

// Service defines an interface which exposes a method for retrieving
// service name.
type Service interface {
	Service() string
}

//***********************************
//  Supervisor
//***********************************

// Supervisor defines a single method which takes
// an occurred error with addr and actor which are related
// to error and also the parent of giving actor which then handles
// the error based on giving criteria and criticality.
type Supervisor interface {
	Handle(err interface{}, targetAddr Addr, target Actor, parent Actor)
}

//***********************************
//  Mailbox
//***********************************

// Mailbox defines a underline queue which provides the
// ability to adequately push and release a envelope
// received for later processing. Usually a mailbox is
// associated with a actor and managed by a distributor.
type Mailbox interface {
	// Wait will block till a message or set of messages are available.
	Wait()

	// Signal will broadcast to all listeners to attempt checking for
	// new messages from blocking state.
	Signal()

	// Cap should returns maximum capacity for mailbox else -1 if unbounded.
	Cap() int

	// Total should return current total message counts in mailbox.
	Total() int

	// Empty should return true/false if mailbox is empty.
	Empty() bool

	// Unpop should add giving addr and envelope to head/start of mailbox
	// ensuring next retrieved message is this added envelope and address.
	Unpop(Addr, Envelope)

	// Push adds giving address and envelope to the end of the mailbox.
	Push(Addr, Envelope) error

	// Pop gets next messages from the top of the mailbox, freeing space
	// for more messages.
	Pop() (Addr, Envelope, error)
}

//***********************************
//  AncestralAddr
//***********************************

// AncestralAddr defines an interface which exposes method to retrieve
// the address of a giving parent of an implementing type.
type AncestralAddr interface {
	Parent() Addr
	Ancestor() Addr
}

//***********************************
//  Sender
//***********************************

// Sender defines an interface that exposes methods
// to sending messages.
type Sender interface {
	// Forward forwards giving envelope to actor.
	Forward(Envelope) error

	// Send will deliver a message to the underline actor
	// with Addr set as sender .
	Send(interface{}, Header, Addr) error

	// SendFuture will deliver given message to Addr's actor inbox
	// for processing and returns a Addr for Future actor has destination of response.
	SendFuture(interface{}, Header, Addr) Future

	// SendFutureTimeout will deliver given message to Addr's actor inbox
	// for processing and returns a Future has destination of response.
	SendFutureTimeout(interface{}, Header, Addr, time.Duration) Future
}

//***********************************
//  Invokers
//***********************************

// MailInvoker defines an interface that exposes methods
// to signal status of a mailbox.
type MailInvoker interface {
	InvokedFull()
	InvokedEmpty()
	InvokedDropped(Addr, Envelope)
	InvokedReceived(Addr, Envelope)
	InvokedDispatched(Addr, Envelope)
}

// MessageInvoker defines a interface that exposes
// methods to signal different state of a process
// for external systems to plugin.
type MessageInvoker interface {
	InvokedRequest(Addr, Envelope)
	InvokedProcessed(Addr, Envelope)
	InvokedProcessing(Addr, Envelope)
}

// StateInvoker defines an interface which signals an invocation of state
// of it's implementer.
type StateInvoker interface {
	InvokedDestroyed(interface{})
	InvokedStarted(interface{})
	InvokedStopped(interface{})
	InvokedKilled(interface{})
	InvokedRestarted(interface{})
	InvokedPanic(Addr, ActorPanic)
}

//***********************************
//  Future
//***********************************

// Future represents the address of a computation ongoing awaiting
// completion but will be completed in the future. It can be sent
// messages and can deliver events in accordance with it's state to
// all listeners. It can also be used to pipe it's resolution to
// other addresses.
type Future interface {
	Addr
	ErrWaiter

	// Pipe adds giving address as a receiver of the result
	// of giving future result or error.
	Pipe(...Addr)

	// Err returns an error if processing failed or if the timeout elapsed
	// or if the future was stopped.
	Err() error

	// Result returns the response received from the actors finished work.
	Result() Envelope
}

//***********************************
//  Actor System Message
//***********************************

// TerminatedActor is sent when an Mask processor has already
// being shutdown/stopped.
type TerminatedActor struct {
	ID   string
	Addr string
}

// ActorStartRequested defines message sent to indicate starting request or in process
// starting request to an actor.
type ActorStartRequested struct {
	ID   string
	Addr string
	Data interface{}
}

// ActorRestartRequested indicates giving message sent to actor to
// initiate stopping.
type ActorRestartRequested struct {
	ID   string
	Addr string
	Data interface{}
}

// ActorStopRequested indicates giving message sent to actor to
// initiate stopping.
type ActorStopRequested struct {
	ID   string
	Addr string
	Data interface{}
}

// ActorStarted is sent when an actor has begun it's operation or has
// completely started.
type ActorStarted struct {
	ID   string
	Addr string
	Data interface{}
}

// ActorRestarted indicates giving message sent by actor after
// restart.
type ActorRestarted struct {
	ID   string
	Addr string
	Data interface{}
}

// ActorStopped is sent when an actor is in the process of shutdown or
// has completely shutdown.
type ActorStopped struct {
	ID   string
	Addr string
}

// ActorDestroyed is sent when an actor is absolutely stopped and is removed
// totally from network. It's operation will be not allowed to run as it
// as become un-existent.
type ActorDestroyed struct {
	ID   string
	Addr string
}

// ActorPanic is sent when an actor panics internally.
type ActorPanic struct {
	ID            string
	Addr          string
	CausedAddr    Addr
	CausedMessage Envelope
	Panic         interface{}
}
