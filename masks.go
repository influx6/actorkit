package actorkit

import (
	"errors"
	"fmt"
	"github.com/rs/xid"
	"time"
)

// errors ...
var (
	ErrDeadDoesNotStopp    = errors.New("deadletter process does not stop")
	ErrUnresolveableByProc = errors.New("future unresolvable by stopped process")
)

//***********************************
//  Process
//***********************************

// NewMask returns a new Mask with provided address and srv string.
// The Mask processor will be resolved by the root distributor.
func NewMask(addr string, srv string) Mask {
	return newMask(addr, srv, rootDistributor)
}

// GetMask provides a method to create a Mask address for a Process.
// Use this method to create Mask ID for giving targets.
// IDs are generated from a global increasing id generator.
// Uses https://github.com/rs/xid for generating id.
func GetMask(addr string, srv string, m Resolver) Mask {
	return newMask(addr, srv, m)
}

// ForceMaskWithProcess returns a mask which gets routed to provided
// process and address. Generally it's desired to always obtain a
// Mask from a Resolver.
func ForceMaskWithProcess(addr string, srv string, m Process) Mask {
	return newMaskWithP(addr, srv, m)
}

//***********************************
//  localMask implements Mask
//***********************************

var _ Mask = &localMask{}

// localMask defines an address implementation to represent the associated address of the underline
// Actor. Multiple actors are allowed to have same service name but must have
// specifically different service ports. Where the network represents the
// zone of the actor be it local or remote.
type localMask struct {
	srv     string
	address string

	rsv Resolver
	m   Process
}

// newMaskWithP returns a new instance of localMask using the provided process.
func newMaskWithP(addr string, srv string, m Process) *localMask {
	return &localMask{
		m:       m,
		srv:     srv,
		address: addr,
	}
}

// newMaskWithP returns a new instance of localMask using the provided resolver
// to resolve the exact process.
func newMask(addr string, srv string, r Resolver) *localMask {
	return &localMask{
		rsv:     r,
		srv:     srv,
		address: addr,
	}
}

func (lm *localMask) proc(update bool) Process {
	if lm.m == nil {
		var ok bool
		if lm.m, ok = lm.rsv.Resolve(lm); !ok {
			lm.m = deadletter
		}
	}

	// if we must update due to process not offering
	// our service anymore, then swap to using to nil,
	// return it and let localMask, resolve again for
	// another process offering service.
	// This allows seamless handover.
	if update {
		last := lm.m
		lm.m = nil
		return last
	}

	return lm.m
}

func (lm *localMask) Unwatch(m Mask) {
	m.RemoveWatcher(lm)
}

func (lm *localMask) Watch(m Mask) {
	if m.Stopped() {
		lm.Send(&TerminatedProcess{
			ID: m.ID(),
		}, deadMask)
		return
	}

	m.AddWatcher(lm, func(ev interface{}) {
		if _, ok := ev.(*ProcessFinishedShutDown); ok {
			lm.Send(&TerminatedProcess{
				ID: m.ID(),
			}, deadMask)

			go m.RemoveWatcher(lm)
			return
		}
	})
}

func (lm *localMask) RemoveWatcher(m Mask) {
	lm.proc(false).RemoveWatcher(m)
}

// AddWatcher registers a function into this Mask watcher list
// indicating that the provided Mask wishes to be informed
// of certain things.
func (lm *localMask) AddWatcher(m Mask, fn func(interface{})) {
	lm.proc(false).AddWatcher(m, fn)
}

func (lm *localMask) GracefulStop() Waiter {
	return lm.proc(false).GracefulStop()
}

func (lm *localMask) Stopped() bool {
	return lm.proc(false).Stopped()
}

func (lm *localMask) Stop() {
	lm.proc(false).Stop()
}

func (lm *localMask) Forward(v Envelope) {
	lm.proc(false).Receive(lm, v)
}

func (lm *localMask) Send(v interface{}, dest Mask) {
	env := LocalEnvelope(xid.New().String(), Header{}, dest, v)
	lm.proc(false).Receive(lm, env)
}

func (lm *localMask) SendFuture(v interface{}, d time.Duration) Future {
	if _, ok := lm.proc(false).(*deadletterProcess); ok {
		return resolvedFutureWithError(ErrDeadDoesNotStopp, lm)
	}

	if lm.proc(false).Stopped() {
		return resolvedFutureWithError(ErrUnresolveableByProc, lm)
	}

	future := newFutureActor(d, lm)
	env := LocalEnvelope(xid.New().String(), Header{}, future.Mask(), v)
	lm.proc(false).Receive(lm, env)
	future.start()
	return future
}

func (lm *localMask) String() string {
	return fmt.Sprintf("actor://%s/%s/%s", lm.address, lm.srv, lm.ID())
}

func (lm *localMask) ID() string {
	return lm.proc(false).ID()
}

func (lm *localMask) Address() string {
	return lm.address
}

func (lm *localMask) Service() string {
	return lm.srv
}

func (lm *localMask) RemoveService() {
	defaultResolver.Unregister(lm.proc(true), lm.srv)
}
