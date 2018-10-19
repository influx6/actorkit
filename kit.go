package actorkit

import (
	"time"

	"github.com/rs/xid"
)

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
type Actor interface {
	Watchable
	Receiver
	Identity
	Escalator
	Stoppable
	Addressable
}

//***********************************
//  Discovery
//***********************************

// Discovery defines an interface that resolves
// a giving address to it's target Actor returning
// actor if found.
type Discovery interface {
	Discover(Addr) (Actor, error)
}

//***********************************
//  Addressable
//***********************************

// Addressable defines an interface which exposes a method for retrieving
// associated address of implementer.
type Addressable interface {
	Addr() string
}

//***********************************
//  Identity
//***********************************

// Identity provides a method to return the ID of a process.
type Identity interface {
	ID() string
}

//***********************************
//  Mailbox
//***********************************

// Mailbox defines a underline queue which provides the
// ability to adequately push and release a envelope
// received for later processing. Usually a mailbox is
// associated with a actor and managed by a distributor.
type Mailbox interface {
	Wait()
	Cap() int
	Total() int
	Empty() bool
	UnPop(Envelope)
	Push(Envelope) error
	Pop() (Envelope, error)
	WaitUntilPush(Envelope)
	TryPushUntil(Envelope, int, time.Duration) error
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
//  Stoppable
//***********************************

// Stoppable defines an interface
type Stoppable interface {
	// Stopped returns true/false if giving value had stopped.
	Stopped() bool

	// Stop will immediately stop the target process regardless of
	// pending operation.
	Kill()

	// Stop will immediately stop the target regardless of
	// pending operation and returns a Waiter to await end.
	Stop() Waiter
}

//***********************************
//  Sender
//***********************************

// Sender defines an interface that exposes methods
// to sending messages.
type Sender interface {
	// Forward forwards giving envelope to actor.
	Forward(Envelope)

	// Send will deliver a message to the underline actor
	// with Addr set as sender .
	Send(interface{}, Addr)

	// SendFuture will deliver given message to Addr's actor inbox
	// for processing and returns a Future has destination of response.
	SendFuture(interface{}, time.Duration) Future
}

//***********************************
// Addr
//***********************************

// Addr defines an interface to represent the associated address of the underline
// Actor process based of the capability and function of giving actor. Addr are like
// capabilities references in that they represent the service provided of the underline
// process and it's capabilities. They are the means of communication, the only line of
// interaction of one service to another. Their common concept of Addr should be transferable
// between zones, distributed system and networks.
//
// Addr has a one-to-many and many-to-many address for actors.
type Addr interface {
	Sender
	Identity
	Escalator
	Stoppable
	Watchable
	Addressable
}

//***********************************
//  Escalator
//***********************************

// Escalator defines an interface with a single method is to deliver an failure escalation signal
// to it's underline provider. The idea is others can call this to send a terminal signal indicate serious failure
// to implementer.
type Escalator interface {
	Escalate(interface{}, Addr) Future
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
	Watch(Addr) Subscription
	Fn(func(interface{})) Subscription
}

//***********************************
//  Waiter
//***********************************

// Waiter defines a in interface that exposes a wait method
// to signal end of a giving operation after blocking call
// of Waiter.Wait().
type Waiter interface {
	Wait()
}

//***********************************
//  Invokers
//***********************************

// MailInvoker defines an interface that exposes methods
// to signal status of a mailbox.
type MailInvoker interface {
	InvokeFull()
	InvokeEmpty()
	InvokeReceived(Envelope)
	InvokeDispatched(Envelope)
}

// MessageInvoker defines a interface that exposes
// methods to signal different state of a process
// for external systems to plugin.
type MessageInvoker interface {
	InvokeRequest(Addr, Envelope)
	InvokeMessageProcessed(Envelope)
	InvokeMessageProcessing(Envelope)
	InvokePanicked(Envelope, interface{})
	InvokeEscalateFailure(Addr, Envelope, interface{})
}

//***********************************
//  Future
//***********************************

// Future represents a computation ongoing awaiting
// able to provide a future response.
type Future interface {
	Waiter

	// Addr returns given address of resolving actor.
	Addr() Addr

	// Err returns an error if processing failed or if the timeout elapsed
	// or if the future was stopped.
	Err() error

	// Result returns the response received from the actors finished work.
	Result() Envelope
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
		Ref:    xid.New(),
		Header: header,
		Sender: sender,
	}
}
