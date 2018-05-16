package actorkit

import (
	"errors"
	"github.com/rs/xid"
	"sync"
)

const (
	// AnyNetworkAddr is used to represent any actor on any network
	// local or remote.
	AnyNetworkAddr = "any:0"
)

var (
	// ErrFleetsNotFound is returned when no fleet is found for service.
	ErrFleetsNotFound = errors.New("fleets for service not found")
)

//***********************************
// deadletter
//***********************************

var (
	deadletter   = deadletterProcess{newNoActorProcess()}
	deadletterId = xid.ID{0x4d, 0x88, 0xe1, 0x5b, 0x60, 0xf4, 0x86, 0xe4, 0x28, 0x41, 0x2d, 0xc9}
	deadMask     = newMask(AnyNetworkAddr, "deadletter", ResolveAlways(deadletter))
)

type deadletterProcess struct {
	*noActorProcess
}

func (d deadletterProcess) ID() string {
	return deadletterId.String()
}

// DeadletterEvent is sent when a envelope arrives to a deadletter.
type DeadletterEvent struct {
	Sender      Mask
	SentThrough Mask
	Message     Envelope
}

// Receive will publish a DeadletterEvent with envelope.
func (d deadletterProcess) Receive(m Mask, en Envelope) {
	events.Publish(DeadletterEvent{
		Message:     en,
		SentThrough: m,
		Sender:      en.Sender(),
	})
}

//***********************************
// processDistributor
//***********************************

var (
	_               Distributor = &processDistributor{}
	rootDistributor             = newProcessDistributor()
)

// GetDeadletter returns the package dead letter processor's Mask address.
func GetDeadletter() Mask {
	return deadMask
}

// GetDistributor returns the package-level distributor.
func GetDistributor() Distributor {
	return rootDistributor
}

type processDistributor struct {
	el         sync.RWMutex
	escalators []Escalator

	rl        sync.RWMutex
	resolvers []Resolver

	fl     sync.RWMutex
	fleets []FleetResolver
}

func newProcessDistributor() *processDistributor {
	pd := &processDistributor{}
	pd.AddResolver(defaultResolver)
	return pd
}

// Deadletter returns the address of the deadletter processor.
func (pb *processDistributor) Deadletter() Mask {
	return deadMask
}

// Fleets returns all Processes providing said services.
func (pb *processDistributor) Fleets(service string) ([]Process, error) {
	pb.fl.RLock()
	defer pb.fl.RUnlock()

	var fleets []Process
	for _, flt := range pb.fleets {
		if procs, err := flt.Fleets(service); err == nil {
			fleets = append(fleets, procs...)
		}
	}

	if len(fleets) == 0 {
		return fleets, ErrFleetsNotFound
	}

	return fleets, nil
}

// Resolve returns the processor for given Mask address.
func (pb *processDistributor) Resolve(in Mask) (Process, bool) {
	pb.rl.RLock()
	defer pb.rl.RUnlock()

	for _, rsv := range pb.resolvers {
		if proc, found := rsv.Resolve(in); found {
			return proc, true
		}
	}

	return deadletter, false
}

// AddEscalator adds escalator into distributors list.
func (pb *processDistributor) AddEscalator(e Escalator) {
	if e == nil {
		return
	}

	pb.el.Lock()
	pb.escalators = append(pb.escalators, e)
	pb.el.Unlock()
}

// AddFleet adds fleet into distributors list.
func (pb *processDistributor) AddFleet(f FleetResolver) {
	if f == nil {
		return
	}

	pb.fl.Lock()
	pb.fleets = append(pb.fleets, f)
	pb.fl.Unlock()
}

// AddResolver adds resolver into distributors list.
func (pb *processDistributor) AddResolver(r Resolver) {
	if r == nil {
		return
	}

	pb.rl.Lock()
	pb.resolvers = append(pb.resolvers, r)
	pb.rl.Unlock()

	pb.fl.Lock()
	if fr, ok := r.(FleetResolver); ok {
		pb.fleets = append(pb.fleets, fr)
	}
	pb.fl.Unlock()
}

// FindAny returns Mask of processor handling service.
// If not found then deadletter Mask is returned.
func (pb *processDistributor) FindAny(service string) Mask {
	pb.rl.RLock()
	defer pb.rl.RUnlock()

	wanted := newMaskWithP(AnyNetworkAddr, service, nil)
	for _, rsv := range pb.resolvers {
		if proc, found := rsv.Resolve(wanted); found {
			wanted.m = proc
			return wanted
		}
	}

	return deadMask
}

// FindAll returns all Mask of processors handling service.
// If not found then deadletter Mask is returned.
func (pb *processDistributor) FindAll(service string) []Mask {
	pb.rl.RLock()
	defer pb.rl.RUnlock()

	addrs := make([]Mask, 0, 1)

	if fleets, err := pb.Fleets(service); err == nil {
		addrs = make([]Mask, 0, len(fleets))
		for _, proc := range fleets {
			wanted := newMaskWithP(AnyNetworkAddr, service, proc)
			addrs = append(addrs, wanted)
		}
	} else {
		wanted := newMaskWithP(AnyNetworkAddr, service, &deadletterProcess{})
		addrs = append(addrs, wanted)
	}

	return addrs
}

// EscalateFailure will escalate giving Envelope and Mask with provided reason.
func (pb *processDistributor) EscalateFailure(by Mask, en Envelope, reason interface{}) {
	pb.el.RLock()
	defer pb.el.RUnlock()
	for _, es := range pb.escalators {
		es.EscalateFailure(by, en, reason)
	}
}
