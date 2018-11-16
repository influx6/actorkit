package actorkit

import (
	"time"

	"github.com/gokit/errors"
)

const (
	defaultSize = 16
)

//*********************************************
// AddrSet
//*********************************************

// ServiceSet implements a grouping of giving addresses using
// sets based on their service offered which is represented by the
// Addr.Addr(). It allows indexing, checking availability of giving
// address within set.
//
// This is not safe for concurrent access.
type ServiceSet struct {
	set   map[string]int
	addrs []Addr
}

// ForEach iterates through all available address against provided
// function. It expects the function to return true if it wishes to
// continue iteration or to stop by returning false.
func (ad *ServiceSet) ForEach(fx func(Addr, int) bool) {
	for ind, elem := range ad.addrs {
		if !fx(elem, ind) {
			break
		}
	}
}

// Has returns true/false if giving underline address (string version) already exists in
// set.
func (ad *ServiceSet) Has(addr string) bool {
	if ad.set != nil {
		if _, ok := ad.set[addr]; ok {
			return true
		}
	}
	return false
}

// HasAddr returns true/false if giving underline address already exists in
// set.
func (ad *ServiceSet) HasAddr(addr Addr) bool {
	if ad.set != nil {
		if _, ok := ad.set[addr.Addr()]; ok {
			return true
		}
	}
	return false
}

// Index returns giving index of address (string version) if within
// set else returns -1.
func (ad *ServiceSet) Index(addr string) int {
	if ad.set != nil {
		if index, ok := ad.set[addr]; ok {
			return index
		}
	}
	return -1
}

// IndexOf returns the giving index of address if in set else returns -1.
func (ad *ServiceSet) IndexOf(addr Addr) int {
	if ad.set != nil {
		if index, ok := ad.set[addr.Addr()]; ok {
			return index
		}
	}
	return -1
}

// Remove removes giving address (string version from underline set).
func (ad *ServiceSet) Remove(service string) bool {
	if ad.set != nil {
		if index, ok := ad.set[service]; ok {
			addr := ad.addrs[index]
			if swap := len(ad.addrs) - 1; swap > 0 {
				ad.addrs[index] = ad.addrs[swap]
				ad.addrs = ad.addrs[:swap]
				delete(ad.set, addr.Addr())
				return true
			}

			ad.addrs = ad.addrs[:0]
			delete(ad.set, addr.Addr())
			return true
		}
	}

	return false
}

// RemoveAddr removes giving address from set.
func (ad *ServiceSet) RemoveAddr(addr Addr) bool {
	if ad.set != nil {
		if index, ok := ad.set[addr.Addr()]; ok {
			if swap := len(ad.addrs) - 1; swap > 0 {
				ad.addrs[index] = ad.addrs[swap]
				ad.addrs = ad.addrs[:swap]
				delete(ad.set, addr.Addr())
				return true
			}

			ad.addrs = ad.addrs[:0]
			delete(ad.set, addr.Addr())
			return true
		}
	}

	return false
}

// Add adds giving address into address set.
func (ad *ServiceSet) Add(addr Addr) bool {
	if ad.set == nil {
		ad.set = map[string]int{}
		ad.addrs = make([]Addr, 0, defaultSize)
	}

	if _, ok := ad.set[addr.Addr()]; !ok {
		ind := len(ad.addrs)
		ad.addrs = append(ad.addrs, addr)
		ad.set[addr.Addr()] = ind
		return true
	}

	return false
}

// Set exposes the provided underline list of Addr, this slice is only
// valid for use until the next call to Add or Remove. Hence you
// must be adequately careful here.
func (ad *ServiceSet) Set() []Addr {
	return ad.addrs[:len(ad.addrs)]
}

//*********************************************
// IDSet
//*********************************************

// IDSet implements a grouping of giving actor addresses using
// sets based on the Addr.ID(). It allows indexing, checking availability of giving
// address within set.
//
// This is not safe for concurrent access.
type IDSet struct {
	set   map[string]int
	addrs []Addr
}

// ForEach iterates through all available address against provided
// function. It expects the function to return true if it wishes to
// continue iteration or to stop by returning false.
func (ad *IDSet) ForEach(fx func(Addr, int) bool) {
	for ind, elem := range ad.addrs {
		if !fx(elem, ind) {
			break
		}
	}
}

// Has returns true/false if giving underline address (string version) already exists in
// set.
func (ad *IDSet) Has(id string) bool {
	if ad.set != nil {
		if _, ok := ad.set[id]; ok {
			return true
		}
	}
	return false
}

// HasAddr returns true/false if giving underline address already exists in
// set.
func (ad *IDSet) HasAddr(addr Addr) bool {
	if ad.set != nil {
		if _, ok := ad.set[addr.ID()]; ok {
			return true
		}
	}
	return false
}

// Index returns giving index of address (string version) if within
// set else returns -1.
func (ad *IDSet) Index(id string) int {
	if ad.set != nil {
		if index, ok := ad.set[id]; ok {
			return index
		}
	}
	return -1
}

// IndexOf returns the giving index of address if in set else returns -1.
func (ad *IDSet) IndexOf(addr Addr) int {
	if ad.set != nil {
		if index, ok := ad.set[addr.ID()]; ok {
			return index
		}
	}
	return -1
}

// Remove removes giving address (string version from underline set).
func (ad *IDSet) Remove(id string) bool {
	if ad.set != nil {
		if index, ok := ad.set[id]; ok {
			addr := ad.addrs[index]
			if swap := len(ad.addrs) - 1; swap > 0 {
				ad.addrs[index] = ad.addrs[swap]
				ad.addrs = ad.addrs[:swap]
				delete(ad.set, addr.ID())
				return true
			}

			ad.addrs = ad.addrs[:0]
			delete(ad.set, addr.ID())
			return true
		}
	}

	return false
}

// RemoveAddr removes giving address from set.
func (ad *IDSet) RemoveAddr(addr Addr) bool {
	if ad.set != nil {
		if index, ok := ad.set[addr.ID()]; ok {
			if swap := len(ad.addrs) - 1; swap > 0 {
				ad.addrs[index] = ad.addrs[swap]
				ad.addrs = ad.addrs[:swap]
				delete(ad.set, addr.ID())
				return true
			}

			ad.addrs = ad.addrs[:0]
			delete(ad.set, addr.ID())
			return true
		}
	}

	return false
}

// Set exposes the provided underline list of Addr, this slice is only
// valid for use until the next call to Add or Remove. Hence you
// must be adequately careful here.
func (ad *IDSet) Set() []Addr {
	return ad.addrs[:len(ad.addrs)]
}

// Add adds giving address into address set.
func (ad *IDSet) Add(addr Addr) bool {
	if ad.set == nil {
		ad.set = map[string]int{}
		ad.addrs = make([]Addr, 0, defaultSize)
	}

	if _, ok := ad.set[addr.ID()]; !ok {
		ind := len(ad.addrs)
		ad.addrs = append(ad.addrs, addr)
		ad.set[addr.ID()] = ind
		return true
	}

	return false
}

//*********************************************
// AddrImpl
//*********************************************

var (
	// ErrHasNoActor is returned when actor implementer has no actor underline
	// which is mostly occuring with futures.
	ErrHasNoActor = errors.New("Addr implementer has no underline actor")
)

var _ Addr = &AddrImpl{}

// Destroy returns a error which provides a means of forceful shutdown
// and removal of giving actor of address from the system basically making
// the actor and it's children non existent.
func Destroy(addr Addr) error {
	if actor := addr.Actor(); actor != nil {
		return actor.Destroy()
	}
	return errors.WrapOnly(ErrHasNoActor)
}

// Kill returns a error which provides a means of shutdown and clearing
// all pending messages of giving actor through it's address. It also kills
// actors children.
func Kill(addr Addr) error {
	if actor := addr.Actor(); actor != nil {
		return actor.Kill()
	}
	return errors.WrapOnly(ErrHasNoActor)
}

// Restart restarts giving actor through it's address, the messages are maintained and kept
// safe, the children of actor are also restarted.
func Restart(addr Addr) error {
	if actor := addr.Actor(); actor != nil {
		return actor.Restart()
	}
	return errors.WrapOnly(ErrHasNoActor)
}

// Poison stops the actor referenced by giving address, this also causes a restart of actor's children.
func Poison(addr Addr) error {
	if actor := addr.Actor(); actor != nil {
		return actor.Stop()
	}
	return errors.WrapOnly(ErrHasNoActor)
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

// Actor returns associated actor of Address.
func (a *AddrImpl) Actor() Actor {
	return a.actor
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
func (a *AddrImpl) Spawn(service string, rec Behaviour) (Addr, error) {
	if a.deadletter {
		return nil, errors.New("not possible from a deadletter address")
	}

	return a.actor.Spawn(service, rec)
}

// AddDiscovery adds discovery service to giving underline actor if possible.
// It returns an error if not possible or failed.
func (a *AddrImpl) AddDiscovery(service DiscoveryService) error {
	if a.deadletter {
		return errors.New("not possible from a deadletter address")
	}
	return a.actor.AddDiscovery(service)
}

// AddressOf returns the address of giving actor matching giving service name.
func (a *AddrImpl) AddressOf(service string, ancestral bool) (Addr, error) {
	if a.deadletter {
		return nil, errors.New("not possible from a deadletter address")
	}
	return a.actor.Discover(service, ancestral)
}

// Forward delivers provided envelope to the current process.
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

// Send delivers provided raw data to this process providing destination/reply address.
func (a *AddrImpl) Send(data interface{}, sender Addr) error {
	if a.deadletter {
		deadLetters.Publish(DeadMail{
			To:      a,
			Message: CreateEnvelope(sender, Header{}, data),
		})
		return nil
	}
	return a.actor.Receive(a, CreateEnvelope(sender, Header{}, data))
}

// SendWithHeader delivers provided raw data to this process providing destination/reply address.
func (a *AddrImpl) SendWithHeader(data interface{}, h Header, sender Addr) error {
	if a.deadletter {
		deadLetters.Publish(DeadMail{
			To:      a,
			Message: CreateEnvelope(sender, h, data),
		})
		return nil
	}
	return a.actor.Receive(a, CreateEnvelope(sender, h, data))
}

// Running returns true/false if actor is running.
func (a *AddrImpl) Running() bool {
	if a.deadletter {
		return true
	}
	return a.actor.Running()
}

// Kill sends a kill signal to the underline process to stop all operations and to close immediately.
func (a *AddrImpl) Kill() error {
	if a.deadletter {
		return nil
	}

	return a.actor.Kill()
}

// Stop returns a error for the stopping of the underline actor for giving address.
func (a *AddrImpl) Stop() error {
	if a.deadletter {
		return nil
	}
	return a.actor.Stop()
}

// Restart returns a error for the restart of the underline actor for giving address.
func (a *AddrImpl) Restart() error {
	if a.deadletter {
		return nil
	}
	return a.actor.Restart()
}

// Destroy returns a error for the termination and destruction of the underline
// actor for giving address.
func (a *AddrImpl) Destroy() error {
	if a.deadletter {
		return nil
	}
	return a.actor.Destroy()
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

// Addr returns the unique address which this address points to both the actor
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
