package actorkit

import (
	"fmt"
	"time"

	"github.com/gokit/es"

	"github.com/gokit/xid"
)

const (
	stackSize = 1 << 16
)

var (
	deadLetterID = xid.New()
	deadLetters  = es.New()
)

// DeadMail defines the type of event triggered by the deadletters
// event pipeline.
type DeadMail struct {
	To      Addr
	Message Envelope
}

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
		Header: header,
		Sender: sender,
		Ref:    xid.New(),
	}
}

//***********************************
//  ProcRegistry
//***********************************

// ProcRegistry implementers implement a giving registry which allows
// collating giving actors within a searching entity for access and retrieval.
// Registries should be able to update themselves based on actor's state.
type ProcRegistry interface {
	Register(Actor) error
	UnRegister(Actor) error
	GetID(string) (Actor, error)
	GetAddr(string) (Actor, error)
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
	Killable
	Startable
	Stoppable
	Restartable
	Destroyable

	Identity
	Namespace
	Addressable
	ProtocolAddr

	Ancestry
	Descendants

	Spawner
	Discovery
	DiscoveryChain

	Running
	Receiver
	Escalator

	Waiter
	Watchable
	Stats

	MailboxOwner
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
	Futures
	Watchable
	Descendants
	Addressable
	ProtocolAddr
	DiscoveryChain
	Escalatable
	Namespace
	AddressActor
	AncestralAddr
	AddressService
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

// ProtocolAddr defines a self named function which returns a giving value
// representing it's protocol address.
type ProtocolAddr interface {
	ProtocolAddr() string
}

// Namespace exposes a self named method to get a giving value for namespace of
// implementer.
type Namespace interface {
	Namespace() string
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
// Spawner
//***********************************

// Spawner exposes a single method to spawn an underline actor returning
// the address for spawned actor.
type Spawner interface {
	Spawn(service string, bh Behaviour) (Addr, error)
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

//**************************************************************
//  DiscoveryFor: Service discovery with function generators
//*************************************************************

// DiscoveryServiceFunction defines a function type which when
// giving a underline service will return a suitable actor for
// said service.
type DiscoveryServiceFunction func(service string) (Actor, error)

// DiscoveryFor returns a new DiscoveryService which calls giving function
// with service name for returning an actor suitable for handling a giving service.
func DiscoveryFor(fn DiscoveryServiceFunction) DiscoveryService {
	return &fnDiscoveryService{Fn: fn}
}

type fnDiscoveryService struct {
	Fn DiscoveryServiceFunction
}

func (dn fnDiscoveryService) Discover(service string) (Actor, error) {
	return dn.Fn(service)
}

//***********************************
//  Running
//***********************************

// Running defines an interface which exposes a method for
// getting the running state of it's implementer.
type Running interface {
	Running() bool
}

//***********************************
//  Stats
//***********************************

// Stat holds count and time details for a giving target or holder
// of stat.
type Stat struct {
	Death          time.Time
	Creation       time.Time
	Killed         int64
	Stopped        int64
	Delivered      int64
	Processed      int64
	Restarted      int64
	FailedRestarts int64
	FailedDelivery int64
}

// Stats exposes a method which returns a giving
// ActorState entity for it's implementer.
type Stats interface {
	Stats() Stat
}

//***********************************
//  Destroyable
//***********************************

// Destroyable defines an interface that exposes methods
// for the total shutdown and removal of an actor from
// all processes.
type Destroyable interface {
	Destroy() error
	DestroyChildren() error
}

// PreDestroy defines a function to be called after the destruction
// of an actor. It is called after stopping routine.
type PreDestroy interface {
	PreDestroy(Addr)
}

// PostDestroy defines a function to be called after the destruction
// of an actor. It is called after stopping routine.
type PostDestroy interface {
	PostDestroy(Addr)
}

//***********************************
//  Stoppable and Killable
//***********************************

// PreStop defines a function to be called before the stopping
// of an actor. It is called before initiating the stop routine.
type PreStop interface {
	PreStop(Addr)
}

// PostStop defines a function to be called after the stopping
// of an actor. It is called after stopping routine.
type PostStop interface {
	PostStop(Addr)
}

// Stoppable defines an interface that provides sets of method to gracefully
// stop the operation of a actor.
type Stoppable interface {
	Stop() error
	StopChildren() error
}

// Killable  defines an interface that provides set of method to
// abruptly stop and end the operation of an actor ungracefully.
type Killable interface {
	Kill() error
	KillChildren() error
}

//***********************************
//  Startable
//***********************************

// Startable defines an interface that exposes a method
// which returns a ErrWaiter to indicate completion of
// start process.
type Startable interface {
	Start() error
}

// PreStart exposes a method which gets called before the start
// of an actor.
//
// If any error is returned, it will cause the actor to stop and shutdown.
type PreStart interface {
	PreStart(Addr) error
}

// PostStart exposes a method which gets called after the start
// of an actor.
//
// If any error is returned, it will cause the actor to stop and shutdown.
type PostStart interface {
	PostStart(Addr) error
}

//***********************************
//  Restartable
//***********************************

// Restartable defines an interface that exposes a method
// which returns a ErrWaiter to indicate completion of
// restart.
type Restartable interface {
	Restart() error
	RestartChildren() error
}

// PreRestart exposes a method which gets called before the restart
// of an actor.
//
// If any error is returned, it will cause the actor to stop and shutdown.
type PreRestart interface {
	PreRestart(Addr) error
}

// PostRestart exposes a method which gets called after the restart
// of an actor.
//
// If any error is returned, it will cause the actor to stop and shutdown.
type PostRestart interface {
	PostRestart(Addr) error
}

//***********************************
//  AddressActor
//***********************************

// AddressActor defines an interface which exposes a method to retrieve
// the actor of an Address.
type AddressActor interface {
	Actor() Actor
}

//***********************************
//  Futures
//***********************************

// Futures defines an interface which exposes methods creating
// futures from a source
type Futures interface {
	// Future returns a new future instance from giving source.
	Future() Future

	// TimedFuture returns a new timed future instance from giving source.
	TimedFuture(time.Duration) Future
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
//  Supervisor and Directives
//***********************************

// Directive defines a int type which represents a giving action to be taken
// for an actor.
type Directive int

// directive sets...
const (
	IgnoreDirective Directive = iota
	PanicDirective
	DestroyDirective
	KillDirective
	StopDirective
	RestartDirective
	EscalateDirective
)

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

	// Clear resets and empties all pending elements of queue.
	Clear()

	// Signal will broadcast to all listeners to attempt checking for
	// new messages from blocking state.
	Signal()

	// Cap should returns maximum capacity for mailbox else -1 if unbounded.
	Cap() int

	// Total should return current total message counts in mailbox.
	Total() int

	// IsEmpty should return true/false if mailbox is empty.
	IsEmpty() bool

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
	Send(interface{}, Addr) error

	// SendWithHeader will deliver a message to the underline actor
	// with Addr set as sender with a Header.
	SendWithHeader(interface{}, Header, Addr) error
}

//***********************************
//  Resolvables
//***********************************

// Resolvables defines an interface which exposes methods for
// resolving an implementation
type Resolvables interface {
	Reject(error)
	Resolve(Envelope)
}

//***********************************
//  Invokers
//***********************************

// SupervisionInvoker defines a invocation watcher, which reports
// giving action taken for a giving error.
type SupervisionInvoker interface {
	InvokedStop(cause interface{}, stat Stat, addr Addr, target Actor)
	InvokedKill(cause interface{}, stat Stat, addr Addr, target Actor)
	InvokedDestroy(cause interface{}, stat Stat, addr Addr, target Actor)
	InvokedRestart(cause interface{}, stat Stat, addr Addr, target Actor)
}

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

// SystemMessage defines a type to identify giving message
// data as a system message.
type SystemMessage interface {
	SystemMessage()
}

// TerminatedActor is sent when an processor has already
// being shutdown/stopped.
type TerminatedActor struct {
	ID   string
	Addr string
}

// SystemMessage identifies giving type as a system message.
func (TerminatedActor) SystemMessage() {}

// FutureResolved indicates the resolution of a giving future.
type FutureResolved struct {
	ID   string
	Data interface{}
}

// SystemMessage identifies giving type as a system message.
func (FutureResolved) SystemMessage() {}

// FutureRejected indicates the rejection of a giving future.
type FutureRejected struct {
	ID  string
	Err error
}

// Error implements the error interface.
func (f FutureRejected) Error() string {
	return f.Err.Error()
}

// Unwrap returns the original error for giving rejection,
// unravelling any further FutureRejected struct if gets.
func (f *FutureRejected) Unwrap() error {
	if fm, ok := f.Err.(FutureRejected); ok {
		return fm.Unwrap()
	}
	return f.Err
}

// SystemMessage identifies giving type as a system message.
func (FutureRejected) SystemMessage() {}

// ActorStartRequested defines message sent to indicate starting request or in process
// starting request by an actor.
type ActorStartRequested struct {
	ID   string
	Addr string
	Data interface{}
}

// SystemMessage identifies giving type as a system message.
func (ActorStartRequested) SystemMessage() {}

// ActorRestartRequested indicates giving message sent by actor to
// initiate stopping.
type ActorRestartRequested struct {
	ID   string
	Addr string
}

// SystemMessage identifies giving type as a system message.
func (ActorRestartRequested) SystemMessage() {}

// ActorKillRequested indicates giving message sent by actor to
// initiate stopping.
type ActorKillRequested struct {
	ID   string
	Addr string
}

// SystemMessage identifies giving type as a system message.
func (ActorKillRequested) SystemMessage() {}

// ActorDestroyRequested indicates giving message sent by actor to
// initiate destruction.
type ActorDestroyRequested struct {
	ID   string
	Addr string
}

// SystemMessage identifies giving type as a system message.
func (ActorDestroyRequested) SystemMessage() {}

// ActorStopRequested indicates giving message sent to actor to
// initiate stopping.
type ActorStopRequested struct {
	ID   string
	Addr string
}

// SystemMessage identifies giving type as a system message.
func (ActorStopRequested) SystemMessage() {}

// ActorStarted is sent when an actor has begun it's operation or has
// completely started.
type ActorStarted struct {
	ID   string
	Addr string
}

// SystemMessage identifies giving type as a system message.
func (ActorStarted) SystemMessage() {}

// ActorRestarted indicates giving message sent by actor after
// restart.
type ActorRestarted struct {
	ID   string
	Addr string
}

// SystemMessage identifies giving type as a system message.
func (ActorRestarted) SystemMessage() {}

// ActorStopped is sent when an actor is in the process of shutdown or
// has completely shutdown.
type ActorStopped struct {
	ID   string
	Addr string
}

// SystemMessage identifies giving type as a system message.
func (ActorStopped) SystemMessage() {}

// ActorKilled indicates giving message sent by actor to
// initiate stopping.
type ActorKilled struct {
	ID   string
	Addr string
}

// SystemMessage identifies giving type as a system message.
func (ActorKilled) SystemMessage() {}

// ActorDestroyed is sent when an actor is absolutely stopped and is removed
// totally from network. It's operation will be not allowed to run as it
// as become un-existent.
type ActorDestroyed struct {
	ID   string
	Addr string
}

// SystemMessage identifies giving type as a system message.
func (ActorDestroyed) SystemMessage() {}

// ActorPanic is sent when an actor panics internally.
type ActorPanic struct {
	ID            string
	Addr          string
	CausedAddr    Addr
	CausedMessage Envelope
	Panic         interface{}
	Stack         []byte
}

// Error returns giving error string for panic details.
func (a *ActorPanic) Error() string {
	return fmt.Sprintf(`ActorPanic:
		Actor: %q
		Address: %q
		CausedAddr: %q
		CauseEnvelope: %#v
		Panic: %#v
		Stack:
		%s
`, a.ID, a.Addr, a.CausedAddr.Addr(), a.CausedMessage, a.Panic, string(a.Stack))
}

// SystemMessage identifies giving type as a system message.
func (ActorPanic) SystemMessage() {}

// ActorFailedStart indicates giving message sent by actor after
// failed to start due to error.
type ActorFailedStart struct {
	Err  error
	ID   string
	Addr string
}

// SystemMessage identifies giving type as a system message.
func (ActorFailedStart) SystemMessage() {}

// ActorFailedRestart indicates giving message sent by actor after
// failed to restart due to error.
type ActorFailedRestart struct {
	Err  error
	ID   string
	Addr string
}

// SystemMessage identifies giving type as a system message.
func (ActorFailedRestart) SystemMessage() {}

// ActorRoutineError is sent when a actor internal routine encountered an error
// either during stop, kill, destruction commands.
type ActorRoutineError struct {
	ID   string
	Addr string
	Err  error
}

// SystemMessage identifies giving type as a system message.
func (ActorRoutineError) SystemMessage() {}

// Error returns giving error string for panic details.
func (a *ActorRoutineError) Error() string {
	return fmt.Sprintf(`ActorRoutineError:
		Actor: %q
		Address: %q
		Error: %+q
`, a.ID, a.Addr, a.Err.Error())
}

// ActorRoutinePanic is sent when a actor internal routine panics.
type ActorRoutinePanic struct {
	ID    string
	Addr  string
	Panic interface{}
	Stack []byte
}

// SystemMessage identifies giving type as a system message.
func (ActorRoutinePanic) SystemMessage() {}

// Error returns giving error string for panic details.
func (a *ActorRoutinePanic) Error() string {
	return fmt.Sprintf(`ActorRoutinePanic:
		Actor: %q
		Address: %q
		Panic: %#v
		Stack:
		%s
`, a.ID, a.Addr, a.Panic, string(a.Stack))
}
