package actorkit

import "time"

var _ Addr = &AddrImpl{}

// AddrImpl implements the Addr interface providing an addressable reference
// to an existing actor.
type AddrImpl struct {
	Root    Addressable
	actor   Actor
	Machine string
	Service string
}

// AddressOf returns a new instance of AddrImpl which directly uses the provided
// process as it's underline target for messages.
func AddressOf(actor Actor, service string) *AddrImpl {
	var addr AddrImpl
	addr.actor = actor
	addr.Service = service
	return &addr
}

// Forward delivers provided envelope to the current mask process.
func (AddrImpl) Forward(Envelope) {
	panic("implement me")
}

// Send delivers provided raw data to this mask process providing destination/reply mask.
func (AddrImpl) Send(interface{}, Addr) {
	panic("implement me")
}

// SendFuture returns a Future which will be fufilled when message is delivered and processed
// else will be rejected after provided duration.
func (AddrImpl) SendFuture(interface{}, time.Duration) Future {
	panic("implement me")
}

// Stopped returns true/false if giving process of Addr as being stopped.
func (AddrImpl) Stopped() bool {
	panic("implement me")
}

// Kill sends a kill signal to the underline process to stop all operations and to close immediately.
func (AddrImpl) Kill() {
	panic("implement me")
}

// Escalate implements the Escalator interface. It returns a Future which can be used to
// listen for signal on delivery of escalation message.
func (AddrImpl) Escalate(v interface{}, addr Addr) Future {
	return nil
}

// Stop sends a KILL/INTERRUPT signal to the underline process to gracefully stop all operations
// and to close once current work is done.
func (AddrImpl) Stop() Waiter {
	panic("implement me")
}

// Watch adds giving Addr as sender of notifications from current Address.
func (AddrImpl) Watch(Addr) Subscription {
	return nil
}

// Fn adds  a giving function into the subscription listeners of giving address.
func (AddrImpl) Fn(fn func(interface{})) Subscription {
	return nil
}

// ID returns unique identification value for underline process of Addr.
func (AddrImpl) ID() string {
	panic("implement me")
}

// Addr returns the unique address which this mask points to for giving process.
func (AddrImpl) Addr() string {
	panic("implement me")
}

// RemoveWatcher removes giving Addr from watching list.
func (m *AddrImpl) RemoveWatcher(w Addr) {

}

// AddWatcher calls underline watcher  to add Addr and callback function.
func (m *AddrImpl) AddWatcher(w Addr, fn func(interface{})) {

}

// String returns the address of mask.
func (m *AddrImpl) String() string {
	return m.Addr()
}
