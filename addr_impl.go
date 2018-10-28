package actorkit

import (
	"time"

	"github.com/gokit/errors"
)

var _ Addr = &AddrImpl{}

// Destroy returns a ErrWaiter which provides a means of forceful shutdown
// and removal of giving actor of address from the system basically making
// the actor and it's children non existent.
func Destroy(addr Addr, data interface{}) ErrWaiter {
	if stopper, ok := addr.(Stoppable); ok {
		return stopper.Kill(data)
	}
	return NewWaiterImpl(errors.New("Addr implementer does not support killing"))
}

// Kill returns a ErrWaiter which provides a means of shutdown and clearing
// all pending messages of giving actor through it's address. It also kills
// actors children.
func Kill(addr Addr, data interface{}) ErrWaiter {
	if stopper, ok := addr.(Stoppable); ok {
		return stopper.Kill(data)
	}
	return NewWaiterImpl(errors.New("Addr implementer does not support killing"))
}

// Restart restarts giving actor through it's address, the messages are maintained and kept
// safe, the children of actor are also restarted.
func Restart(addr Addr, data interface{}) ErrWaiter {
	if stopper, ok := addr.(Restartable); ok {
		return stopper.Restart(data)
	}
	return NewWaiterImpl(errors.New("Addr implementer does not support restarting"))
}

// Poison stops the actor referenced by giving address, this also causes a restart of actor's children.
func Poison(addr Addr, data interface{}) ErrWaiter {
	if stopper, ok := addr.(Stoppable); ok {
		return stopper.Stop(data)
	}
	return NewWaiterImpl(errors.New("Addr implementer does not support stopping"))
}

// AddrImpl implements the Addr interface providing an addressable reference
// to an existing actor.
type AddrImpl struct {
	Root       Addressable
	actor      Actor
	service    string
	deadletter bool
}

// AccessOf returns a default "actor:access" service name, it's
// expected to be used when desiring a default address for an
// actor.
func AccessOf(actor Actor) *AddrImpl {
	return AddressOf(actor, "actor:access")
}

// DeadLetters returns a new instance of AddrImpl which directly delivers
// responses and messages to the deadletter event pipeline.
func DeadLetters() *AddrImpl {
	var addr AddrImpl
	addr.deadletter = true
	addr.service = "deadletters"
	return &addr
}

// AddressOf returns a new instance of AddrImpl which directly uses the provided
// process as it's underline target for messages.
func AddressOf(actor Actor, service string) *AddrImpl {
	var addr AddrImpl
	addr.actor = actor
	addr.service = service
	return &addr
}

// Parent returns the address of parent if giving underline actor
// is not the same as the actor of this address else returning this
// actor.
func (a *AddrImpl) Parent() Addr {
	if a.deadletter {
		return a
	}

	if parent := a.actor.Parent(); parent != nil && parent != a.actor {
		return AddressOf(parent, "access")
	}
	return a
}

// Future returns a new future instance from giving source.
func (a *AddrImpl) Future() Future {
	return NewFuture(a)
}

// TimedFuture returns a new future instance from giving source.
func (a *AddrImpl) TimedFuture(d time.Duration) Future {
	return TimedFuture(a, d)
}

// Ancestor returns the address of the root ancestor. If giving underline
// ancestor is the same as this address actor then we return address.
func (a *AddrImpl) Ancestor() Addr {
	if a.deadletter {
		return a
	}

	if parent := a.actor.Ancestor(); parent != nil && parent != a.actor {
		return AddressOf(parent, "access")
	}
	return a
}

// Children returns address of all children actors of this address actor.
func (a *AddrImpl) Children() []Addr {
	if a.deadletter {
		return nil
	}
	return a.actor.Children()
}

// Spawn creates a new actor based on giving service name by requesting all
// discovery services registered to giving underline address actor.
func (a *AddrImpl) Spawn(service string, rec Behaviour, initial interface{}) (Addr, error) {
	if a.deadletter {
		return nil, errors.New("not possible from a deadletter address")
	}

	return a.actor.Spawn(service, rec, initial)
}

// AddressOf returns the address of giving actor matching giving service name.
func (a *AddrImpl) AddressOf(service string, ancestral bool) (Addr, error) {
	if a.deadletter {
		return nil, errors.New("not possible from a deadletter address")
	}
	return a.actor.Discover(service, ancestral)
}

// Forward delivers provided envelope to the current mask process.
func (a *AddrImpl) Forward(e Envelope) error {
	if a.deadletter {
		deadLetters.Publish(DeadMail{
			To:      a,
			Message: e,
		})
		return nil
	}
	return a.actor.Receive(a, e)
}

// Send delivers provided raw data to this mask process providing destination/reply mask.
func (a *AddrImpl) Send(data interface{}, h Header, sender Addr) error {
	if a.deadletter {
		deadLetters.Publish(DeadMail{
			To:      a,
			Message: CreateEnvelope(sender, h, data),
		})
		return nil
	}
	return a.actor.Receive(a, CreateEnvelope(sender, h, data))
}

// Stopped returns true/false if giving process of Addr as being stopped.
func (a *AddrImpl) Stopped() bool {
	if a.deadletter {
		return false
	}
	return a.actor.Stopped()
}

// Kill sends a kill signal to the underline process to stop all operations and to close immediately.
func (a *AddrImpl) Kill(data interface{}) ErrWaiter {
	if a.deadletter {
		return NewWaiterImpl(nil)
	}

	return a.actor.Kill(data)
}

// Stop returns a ErrWaiter for the stopping of the underline actor for giving address.
func (a *AddrImpl) Stop(data interface{}) ErrWaiter {
	if a.deadletter {
		return NewWaiterImpl(nil)
	}
	return a.actor.Stop(data)
}

// Restart returns a ErrWaiter for the restart of the underline actor for giving address.
func (a *AddrImpl) Restart(data interface{}) ErrWaiter {
	if a.deadletter {
		return NewWaiterImpl(nil)
	}
	return a.actor.Restart(data)
}

// Destroy returns a ErrWaiter for the termination and destruction of the underline
// actor for giving address.
func (a *AddrImpl) Destroy(data interface{}) ErrWaiter {
	if a.deadletter {
		return NewWaiterImpl(nil)
	}
	return a.actor.Destroy(data)
}

// Escalate implements the Escalator interface.
func (a *AddrImpl) Escalate(v interface{}) {
	if a.deadletter {
		deadLetters.Publish(v)
		return
	}
	a.actor.Escalate(v, a)
}

// Watch adds  a giving function into the subscription
// listeners of giving address events.
func (a *AddrImpl) Watch(fn func(interface{})) Subscription {
	if a.deadletter {
		return deadLetters.Subscribe(fn)
	}
	return a.actor.Watch(fn)
}

// ID returns unique identification value for underline process of Addr.
func (a *AddrImpl) ID() string {
	if a.deadletter {
		return deadLetterID.String()
	}
	return a.actor.ID()
}

// Service returns the service name which the giving address represent as it's
// capability and functionality for giving actor.
func (a *AddrImpl) Service() string {
	return a.service
}

// Addr returns the unique address which this mask points to both the actor
// and service the address is presenting as the underline actor capability.
//
// Address uses a format: ActorAddress/ServiceName
//
func (a *AddrImpl) Addr() string {
	if a.deadletter {
		return "kit://localhost/" + a.ID() + "/" + a.service
	}
	return a.actor.Addr() + "/" + a.service
}

// String returns address string of giving AddrImpl.
func (a *AddrImpl) String() string {
	return a.Addr()
}
