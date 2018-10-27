package actorkit

import "time"

var _ Addr = &AddrImpl{}

// AddrImpl implements the Addr interface providing an addressable reference
// to an existing actor.
type AddrImpl struct {
	Root    Addressable
	actor   Actor
	service string
}

// AccessOf returns a default "actor:access" service name, it's
// expected to be used when desiring a default address for an
// actor.
func AccessOf(actor Actor) *AddrImpl {
	return AddressOf(actor, "actor:access")
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
	if parent := a.actor.Parent(); parent != nil && parent != a.actor {
		return AddressOf(parent, "access")
	}
	return a
}

// Ancestor returns the address of the root ancestor. If giving underline
// ancestor is the same as this address actor then we return address.
func (a *AddrImpl) Ancestor() Addr {
	if parent := a.actor.Ancestor(); parent != nil && parent != a.actor {
		return AddressOf(parent, "access")
	}
	return a
}

// Children returns address of all children actors of this address actor.
func (a *AddrImpl) Children() []Addr {
	return a.actor.Children()
}

// Spawn creates a new actor based on giving service name by requesting all
// discovery services registered to giving underline address actor.
func (a *AddrImpl) Spawn(service string, rec Behaviour, initial interface{}) (Addr, error) {
	return a.actor.Spawn(service, rec, initial)
}

// AddressOf returns the address of giving actor matching giving service name.
func (a *AddrImpl) AddressOf(service string, ancestral bool) (Addr, error) {
	return a.actor.Discover(service, ancestral)
}

// Forward delivers provided envelope to the current mask process.
func (a *AddrImpl) Forward(e Envelope) error {
	return a.actor.Receive(a, e)
}

// Send delivers provided raw data to this mask process providing destination/reply mask.
func (a *AddrImpl) Send(data interface{}, h Header, sender Addr) error {
	return a.actor.Receive(a, CreateEnvelope(sender, h, data))
}

// SendFuture returns a Future which will be fulfilled when message is delivered and process.
func (a *AddrImpl) SendFuture(data interface{}, h Header, sender Addr) Future {
	panic("implement me")
}

// SendFutureTimeout returns a Future which will be fulfilled when message is delivered and processed
// else will be rejected after provided duration.
func (a *AddrImpl) SendFutureTimeout(data interface{}, h Header, sender Addr, d time.Duration) Future {
	panic("implement me")
}

// Stopped returns true/false if giving process of Addr as being stopped.
func (a *AddrImpl) Stopped() bool {
	panic("implement me")
}

// Kill sends a kill signal to the underline process to stop all operations and to close immediately.
func (a *AddrImpl) Kill() {
	panic("implement me")
}

// Escalate implements the Escalator interface.
func (a *AddrImpl) Escalate(v interface{}) {
	a.actor.Escalate(v, a)
}

// Watch adds  a giving function into the subscription
// listeners of giving address events.
func (a *AddrImpl) Watch(fn func(interface{})) Subscription {
	return a.actor.Watch(fn)
}

// ID returns unique identification value for underline process of Addr.
func (a *AddrImpl) ID() string {
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
	return a.actor.Addr() + "/" + a.service
}

// String returns address string of giving AddrImpl.
func (a *AddrImpl) String() string {
	return a.Addr()
}
