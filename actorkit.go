package actorkit

import (
	"time"
	"github.com/gokit/es"
)

//***********************************
// Subscribe And Unsubscribe
//***********************************

var events = es.New()

// Subscribe adds handler into global subscription.
func Subscribe(h es.EventHandler) es.Subscription {
	return events.Subscribe(h)
}

// Publish publishes to all subscribers provided value.
func Publish(h interface{})  {
	events.Publish(h)
}

//***********************************
// Envelope And Header
//***********************************

// ReadOnlyHeader defines an interface for a header
// type which exposes methods to get header values but
// cam mpt be changed in anyway.
type ReadOnlyHeader interface{
	Len() int
	Has(string) bool
	Get(string) string
	Map() map[string]string
}

var _ ReadOnlyHeader = &Header{}

// Header defines a map type to hold meta information associated with a Envelope.
type Header map[string]string

// Get returns the associated value from the map within the map.
func (m Header) Get(n string) string {
	return m[n]
}

// Map returns a map with contents of header.
func (m Header) Map() map[string]string {
	mv := make(map[string]string, len(m))
	for k, v := range m {mv[k] = v}
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

// Envelope defines an interface representing a received message.
type Envelope interface{
	ReadOnlyHeader
	ID() string
	Sender() Mask
	Data() interface{}
}

// NEnvelope returns a new Envelope from provided arguments.
func NEnvelope(id string, header Header, sender Mask, data interface{}) Envelope{
	return &localEnvelope{
		id:id,
		data: data,
		sender: sender,
		Header: header,
	}
}

type localEnvelope struct{
	Header
	id string
	sender Mask
	data interface{}
}

func (le *localEnvelope) Data() interface{} {
	return le.data
}

func (le *localEnvelope) Sender() Mask {
	return le.sender
}

func (le *localEnvelope) ID() string {
	return le.id
}

//***********************************
// Mask
//***********************************

// Mask defines an interface to represent the associated address of the underline
// Actor. Multiple actors are allowed to have same service name but must have
// specifically different service ports. Where the network represents the
// zone of the actor be it local or remote.
type Mask interface{
	Stoppable
	Sender

	// ID returns the unique id value for the given
	// actor which the mask points to.
	ID() string

	// Service returns the associated service group, tag
	// which actor wishes to represent itself under.
	// This allows the distributor to group actors.
	Service() string

	// Address returns the associated address of actor pointed
	// to my mask.
	Address() string

	// String returns the full representation of associated
	// Mask.
	String() string
}

//***********************************
//  Invokers
//***********************************

// MailInvoker defines an interface that exposes methods
// to signal status of a mailbox.
type MailInvoker interface{
	InvokeFull()
	InvokeEmpty()
	InvokeReceived(Envelope)
	InvokeDispatched(Envelope)
}

// MessageInvoker defines a interface that exposes
// methods to signal different state of a process
// for external systems to plugin.
type MessageInvoker interface{
	InvokeRequest(Envelope)
	InvokeMessageProcessed(Envelope)
	InvokeMessageProcessing(Envelope)
	InvokeSystemRequest(Envelope)
	InvokePanicked(Envelope, interface{})
	InvokeSystemMessageProcessing(Envelope)
	InvokeSystemMessageProcessed(Envelope)
	InvokeEscalateFailure(Mask, Envelope, interface{})
}

//***********************************
//  Receiver
//***********************************

// Receiver defines an interface that exposes methods
// to receive envelopes.
type Receiver interface{
	Receive(envelope Envelope)
}

//***********************************
//  Sender
//***********************************

// Sender defines an interface that exposes methods
// to sending messages.
type Sender interface{
	// Send will deliver a message to the underline actor
	// will destination address.
	Send(interface{}, Mask)

	// Forward forwards giving envelope to actor.
	Forward(Envelope)

	// SendFuture will deliver given message to Mask's actor inbox
	// for processing and returns a Future has destination of response.
	SendFuture(interface{}, time.Duration) Future
}

//***********************************
//  Waiter
//***********************************

// Waiter defines a in interface that exposes a wait method
// to signal end of a giving operation after blocking call
// of Waiter.Wait().
type Waiter interface{
	Wait()
}

//***********************************
//  Future
//***********************************

// Future represents a computation ongoing awaiting
// able to provide a future response.
type Future interface{
	Waiter

	// Addr returns given address of resolving actor.
	Addr() Mask

	// Err returns an error if processing failed or if the timeout elapsed
	// or if the future was stopped.
	Err() error

	// Result returns the response received from the actors finished work.
	Result() Envelope
}

//***********************************
//  Stoppable
//***********************************

// Stoppable defines an interface
type Stoppable interface{
	// Stop will immediately stop the target regardless of
	// pending operation.
	Stop()

	// GracefulStop will immediately stop the target regardless of
	// pending operation and returns a Waiter to await end.
	GracefulStop() Waiter
}

//***********************************
//  Actor
//***********************************

// Actor represents a indivisible unit of computation.
// Encapsulating itself and it's internal from the outside
// as a black-box.
type Actor interface{
	Respond(Envelope, Distributor)
}

// ActorFunc defines a function type representing
// the argument for a behaviour.
type ActorFunc func(Envelope, Distributor)

// FromFunc returns a Actor that uses the function has a
// resolver.
func FromFunc(b ActorFunc) Actor {
	return beFunc{b}
}

type beFunc struct{
	b ActorFunc
}

// Respond implements the Actor interface.
func (b beFunc) Respond(e Envelope, d Distributor){
	b.b(e,d)
}

//***********************************
//  Mailbox
//***********************************

// Mailbox defines a underline queue which provides the
// ability to adequately push and release a envelope
// received for later processing. Usually a mailbox is
// associated with a actor and managed by a distributor.
type Mailbox interface{
	Cap() int
	Total() int
	Empty() bool
	Push(Envelope)
	Pop() Envelope
	UnPop(Envelope)
}

//***********************************
//  Escalator
//***********************************

// Escalator defines an interface that exposes a means to escalate
// giving failure.
type Escalator interface{
	EscalateFailure(by Mask, envelope Envelope, reason interface{})
}

//***********************************
//  Identity
//***********************************

// Identity provides a method to return the ID of a process.
type Identity interface{
	ID() string
}

//***********************************
//  Process
//***********************************

// Process defines a type which embodies the methods of
// Stoppable and Sender.
type Process interface{
	Identity
	Stoppable
	Receiver
}

//***********************************
//  ProcessRegistry
//***********************************

// ProcessRegistry defines an interface that exposes
// a method to register an existing Process with its
// associate service and id.
type ProcessRegistry interface{
	Register(p Process, service string) error
}

//***********************************
//  Resolver And FleetResolver
//***********************************

// Resolver defines an interface that resolves
// a giving Mask address into a Maskable.
type Resolver interface{
	Resolve(Mask) (Process, bool)
}

// FleetResolver defines a interface that returns
// a list of processes that match a giving service.
type FleetResolver interface{
	Fleets(service string) ([]Process, error)
}

//***********************************
//  Distributor
//***********************************

// Distributor implements a central router which
// handles distribution of messages to and from actors.
// It provides ability to identify giving actors based on
// associated service name.
type Distributor interface{
	Escalator

	// Deadletter returns the address associated with the
	// deadletter inbox of the distributor.
	Deadletter() Mask

	// AddEscalator adds provided escalator into distributor.
	AddEscalator(Escalator)

	// AddResolver provides a method to add given Resolver into
	// the distributor.
	AddResolver(Resolver)

	// AddFleet provides a method to add given FleetResolver into
	// the distributor.
	AddFleet(FleetResolver)

	// FindAny returns a giving actor associated with giving service.
	// If service is not found then it's expected that the Mask for
	// dead letter be returned.
	FindAny(service string)  Mask

	// FindAll returns all actors address providing the required service. This
	// is to allow discovery of other actors. It should use the FleetResolver
	// underneath.
	FindAll(service string)  []Mask
}

