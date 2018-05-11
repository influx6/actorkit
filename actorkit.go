package actorkit

import "time"


//***********************************
// Envelope And Meta
//***********************************

// Meta defines a map type to hold meta information associated with a Envelope.
type Meta map[string]interface{}

// Get returns the associated value from the map within the map.
func (m Meta) Get(n string)  interface{} {
	return m[n]
}

// Len returns the length of records within the meta.
func (m Meta) Len() int {
	return len(m)
}

// Has returns true/false value if key is present.
func (m Meta) Has(n string) bool {
	_, ok := m[n]
	return ok
}

// Envelope defines an struct representing a received message.
type Envelope struct{
	ID int64
	Meta Meta
	Src Mask
	Dst Mask
	Data interface{}
	Future Future
}

//***********************************
// Mask
//***********************************

// Mask defines an interface to represent the associated address of the underline
// Actor. Multiple actors are allowed to have same service name but must have
// specifically different service ports. Where the network represents the
// zone of the actor be it local or remote.
type Mask interface{
	Port() int
	String() string
	Network() string
	Service() string

	Send(interface{}) error
}

//***********************************
//  Future
//***********************************

// Future represents a computation ongoing awaiting
// able to provide a future response.
type Future interface{
	// Wait will block current goroutine till the future has being
	// resolved.
	Wait()

	// Stop ends the block from execution to and stops blocking any
	// calls maid to wait, also the response will be considered unhandled
	// and will be sent to the dead letter.
	Stop()

	// Mask returns the target address mask of the processing actor.
	Mask() Mask

	// Err returns an error if processing failed or if the timeout elapsed
	// or if the future was stopped.
	Err() error

	// Resolve the future will be resolved with giving value.
	Resolve(interface{})

	// Response returns the response received from the actors finished work.
	Response() interface{}
}

//***********************************
// Supervisor
//***********************************

// SupervisorStats provides a basic stats report regarding
// a supervisors state.
type SupervisorStats interface{
	// Throughput returns the total count of delivered messages.
	Throughput() int

	// Received returns the total messages received by supervisor.
	Received() int

	// Processed returns the total messages handled by supervisor's actor.
	Processed() int
}

// Supervisor handles the supervision of a giving actor and
// associated mailbox.
type Supervisor interface{
	// Stop ends the operation of the supervisor, making the actor in
	// effect non-working.
	Stop() error

	// Stats returns the stats associated with a supervisor.
	Stats() SupervisorStats

	// Deliver adds a message to the mailbox for delivery to the actor for processing.
	Deliver(envelope Envelope) error

	// Supervise initializes the supervisor to begin handling of action operation.
	// It is expected to return an error if the supervisor is already handling
	// a actor process, if called twice.
	Supervise(actor Actor, mailbox Mailbox) error
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
//  Distributor
//***********************************

// Distributor implements a central router which
// handles distribution of messages to and from actors.
// It provides ability to identify giving actors based on
// associated service name.
type Distributor interface{
	// Stop sends a signal to stop a giving actor and returns
	// an error if failed.
	Stop(Mask) error

	// Escalator returns the distributors escalator.
	Escalator() Escalator

	// Deadletter returns the address associated with the
	// deadletter inbox of the distributor.
	Deadletter() Mask

	// Send delivers giving message to actor on giving ID.
	Send(Mask,  interface{})

	// SendFuture delivers message to giving actor but times it with giving
	// duration where the surpassing of said duration will cancel the Future with
	// a timeout.
	SendFuture(Mask,  interface{}, time.Duration) Future

	// ForwardEnvelope delivers giving Envelope to another actor
	// for processing.
	// Note that the envelope details will not be changed
	// and the response will be delivered to the original Mask i.e Envelope.Src.
	ForwardEnvelope(Mask,  Envelope)

	// ForwardFuture delivers giving envelop to an actor on the associated
	// mask. The response of the actor is timed with giving duration.
	// Note that the envelope details will not be changed
	// and the response will be delivered to the original Mask i.e Envelope.Src.
	ForwardFuture(Mask,  Envelope, time.Duration) Future

	// FindAny returns a giving actor associated with giving service.
	FindAny(service string)  (Mask, error)

	// FindAll returns all actors provided services for giving service name. This
	// is needed to allow discovery of all instance of a actor type that are providing
	// processing for a associated service name.
	FindAll(service string)  ([]Mask, error)

	// Borrow provides a one-time actor which function will be called on response
	// after sending message to another actor.
	Borrow(func(Envelope, Distributor)) Mask

	// Supervise will register giving actor as an instance to be referenced by the
	// Mask address. It will receive it's own supervisor which will manage it's
	// mailbox and message delivery.
	Supervise(Mask, Actor)
}

//***********************************
//  Actor
//***********************************

// Actor represents a indivisible unit of computation.
// Encapsulating itself and it's internal from the outside
// as a black-box.
type Actor interface{
	Receive(Envelope, Distributor)
}

//***********************************
//  Mailbox
//***********************************

// Mailbox defines a underline queue which provides the
// ability to adequately push and release a envelope
// received for later processing. Usually a mailbox is
// associated with a actor and managed by a distributor.
type Mailbox interface{
	Empty() bool
	Push(*Envelope)
	Pop() *Envelope
	UnPop(*Envelope)
}
