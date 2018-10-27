package actorkit

import (
	"sync"
	"time"

	"github.com/gokit/es"

	"github.com/gokit/errors"
	"github.com/gokit/xid"
)

// errors ...
var (
	ErrFutureTimeout          = errors.New("Future timed out")
	ErrFutureResolved         = errors.New("Future is resolved")
	ErrFutureEscalatedFailure = errors.New("Future failed due to escalated error")
)

// FutureImpl defines an implementation for the Future actor type.
type FutureImpl struct {
	id     xid.ID
	parent Addr
	timer  *time.Timer
	events *es.EventStream

	ac    sync.Mutex
	pipes []Addr

	cw     *sync.Mutex
	cond   *sync.Cond
	err    error
	result *Envelope
}

// NewFuture returns a new instance of giving future.
func NewFuture(parent Addr) *FutureImpl {
	var ft FutureImpl
	ft.parent = parent

	var condMutex sync.Mutex
	ft.cw = &condMutex
	ft.cond = sync.NewCond(&condMutex)

	return &ft
}

// TimedFuture returns a new instance of giving future.
func TimedFuture(parent Addr, dur time.Duration) *FutureImpl {
	var ft FutureImpl
	ft.parent = parent
	ft.timer = time.NewTimer(dur)

	var condMutex sync.Mutex
	ft.cw = &condMutex
	ft.cond = sync.NewCond(&condMutex)

	go ft.timedResolved()
	return &ft
}

// Forward delivers giving envelope into Future actor which if giving
// future is not yet resolved will be the resolution of future.
func (f *FutureImpl) Forward(reply Envelope) error {
	if f.resolved() {
		return errors.Wrap(ErrFutureResolved, "Future %q already resolved", f.Addr())
	}

	f.cond.L.Lock()
	f.result = &reply
	f.cond.L.Unlock()
	f.cond.Broadcast()

	f.broadcast()
	return nil
}

// Send delivers giving data to Future as the resolution of
// said Future. The data provided will be used as the resolved
// value of giving future, if it's not already resolved.
func (f *FutureImpl) Send(data interface{}, h Header, addr Addr) error {
	if f.resolved() {
		return errors.Wrap(ErrFutureResolved, "Future %q already resolved", f.Addr())
	}

	env := CreateEnvelope(addr, h, data)

	f.cond.L.Lock()
	f.result = &env
	f.cond.L.Unlock()
	f.cond.Broadcast()

	f.broadcast()
	return nil
}

// SendFuture will return a Future which will be finalized when present
// future as resolved and finalized it's response. It simply creates a
// future chain where each get's resolved automatically by the resolution of
// it's previous chain.
func (f *FutureImpl) SendFuture(data interface{}, h Header, addr Addr) Future {
	if f.resolved() {
		return f
	}

	f.Send(data, h, addr)
	return f
}

// SendFutureTimeout will return a Future which will be finalized either by the
// resolution of it's previous chain or the failure of it's previous chain to
// reach a resolution before a giving duration.
// Not only the returned Future and it's own chain after itself will be affected.
func (f *FutureImpl) SendFutureTimeout(data interface{}, h Header, addr Addr, dur time.Duration) Future {
	if f.resolved() {
		return f
	}

	timedFuture := TimedFuture(f, dur)
	f.Pipe(timedFuture)
	f.Send(data, h, addr)
	return timedFuture
}

// Watch adds giving function into event system for future.
func (f *FutureImpl) Watch(fn func(interface{})) Subscription {
	return f.events.Subscribe(fn)
}

// Parent returns the address of the parent of giving Future.
func (f *FutureImpl) Parent() Addr {
	return f.parent
}

// Ancestor returns the root parent address of the of giving Future.
func (f *FutureImpl) Ancestor() Addr {
	return f.parent.Ancestor()
}

// Children returns an empty slice as futures can not have children actors.
func (f *FutureImpl) Children() []Addr {
	return nil
}

// Service returns the "Future" as the service name of FutureImpl.
func (f *FutureImpl) Service() string {
	return "Future"
}

// ID returns the unique id of giving Future.
func (f *FutureImpl) ID() string {
	return f.id.String()
}

// Addr returns s consistent address format representing a future which will always be
// in the format:
//
//	ParentAddr/:future/FutureID
//
func (f *FutureImpl) Addr() string {
	return f.parent.Addr() + "/:future/" + f.id.String()
}

// Escalate escalates giving value into the parent of giving future, which also fails future
// and resolves it as a failure.
func (f *FutureImpl) Escalate(m interface{}) {
	f.cond.L.Lock()
	f.err = ErrFutureEscalatedFailure
	f.cond.L.Unlock()

	f.cond.Broadcast()
	f.parent.Escalate(m)
}

// AddressOf requests giving service from future's parent AddressOf method.
func (f *FutureImpl) AddressOf(service string, ancestry bool) (Addr, error) {
	return f.parent.AddressOf(service, ancestry)
}

// Spawn requests giving service and Receiver from future's parent Spawn method.
func (f *FutureImpl) Spawn(service string, rr Behaviour, initial interface{}) (Addr, error) {
	return f.parent.Spawn(service, rr, initial)
}

// Wait blocks till the giving future is resolved and returns error if
// occurred.
func (f *FutureImpl) Wait() error {
	var err error
	f.cond.L.Lock()
	f.cond.Wait()
	err = f.err
	f.cond.L.Unlock()
	return err
}

// Pipe adds giving set of address into giving Future.
func (f *FutureImpl) Pipe(addrs ...Addr) {
	if f.resolved() {
		for _, addr := range addrs {
			addr.Forward(*f.result)
		}
		return
	}

	f.ac.Lock()
	f.pipes = append(f.pipes, addrs...)
	f.ac.Unlock()
}

// Err returns the error for the failure of
// giving error.
func (f *FutureImpl) Err() error {
	f.cond.L.Lock()
	defer f.cond.L.Unlock()
	return f.err
}

// Result returns the envelope which is used to resolve the future.
func (f *FutureImpl) Result() Envelope {
	f.cond.L.Lock()
	defer f.cond.L.Unlock()
	return *f.result
}

func (f *FutureImpl) resolved() bool {
	f.cond.L.Lock()
	defer f.cond.L.Unlock()
	return f.result != nil || f.err != nil
}

func (f *FutureImpl) broadcast() {
	f.ac.Lock()
	defer f.ac.Unlock()

	for _, addr := range f.pipes {
		addr.Forward(*f.result)
	}
}

func (f *FutureImpl) sendError(err error) {
	f.cond.L.Lock()
	f.err = err
	f.cond.L.Unlock()
	f.cond.Broadcast()
}

func (f *FutureImpl) timedResolved() {
	<-f.timer.C
	if f.resolved() {
		return
	}
	f.sendError(ErrFutureTimeout)
}
