package actorkit

import (
	"github.com/rs/xid"
	"time"
	"fmt"
)

//***********************************
//  Process
//***********************************

// NewMask returns a new Mask with provided address and srv string.
// The Mask processor will be resolved by the root distributor.
func NewMask(addr string, srv string) Mask {
	return newMask(addr,srv, rootDistributor)
}

// GetMask provides a method to create a Mask address for a Process.
// Use this method to create Mask ID for giving targets.
// IDs are generated from a global increasing id generator.
// Uses https://github.com/rs/xid for generating id.
func GetMask(addr string, srv string, m Resolver) Mask {
	return newMask(addr,srv, m)
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
type localMask struct{
	srv string
	address string

	rsv Resolver
	m Process
}

// newMaskWithP returns a new instance of localMask using the provided process.
func newMaskWithP(addr string, srv string, m Process) *localMask {
	return &localMask{
		m: m,
		srv: srv,
		address: addr,
	}
}

// newMaskWithP returns a new instance of localMask using the provided resolver
// to resolve the exact process.
func newMask(addr string, srv string, r Resolver) *localMask {
	return &localMask{
		rsv: r,
		srv: srv,
		address: addr,
	}
}

func (lm *localMask) proc() Process {
	if lm.m == nil {
		var ok bool
		if lm.m, ok = lm.rsv.Resolve(lm); !ok {
			lm.m = deadletter
		}
	}
	return lm.m
}

func (lm *localMask) GracefulStop() Waiter {
	return lm.proc().GracefulStop()
}

func (lm *localMask) Stop() {
	if lm.proc() != nil {return}
	lm.proc().Stop()
}

func (lm *localMask) Forward(v Envelope)  {
	lm.proc().Receive(v)
}

func (lm *localMask) Send(v interface{}, dest Mask)  {
	env := NEnvelope(xid.New().String(),Header{},dest, v)
	lm.proc().Receive(env)
}

func (lm *localMask) SendFuture(v interface{}, d time.Duration) Future  {
	future := newFutureActor(d, lm)

	env := NEnvelope(xid.New().String(),Header{},future.Mask(), v)
	lm.proc().Receive(env)

	future.start()
	return future
}

func (lm *localMask) String() string {
	return fmt.Sprintf("actor://%s/%s/%s", lm.address, lm.srv, lm.ID())
}

func (lm *localMask) ID() string {
	return lm.proc().ID()
}

func (lm *localMask) Address() string {
	return lm.address
}

func (lm *localMask) Service() string {
	return lm.srv
}
