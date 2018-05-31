package actors

import (
	"errors"
	"sync"
	"sync/atomic"

	"github.com/gokit/actorkit"
	"github.com/gokit/actorkit/mailbox"
	"github.com/rs/xid"
)

var (
	ErrAlreadySupervising = errors.New("already supervising")
)

var (
	queuedPool = sync.Pool{New: func() interface{} {
		return new(actorkit.QueuedEnvelope)
	}}
)

//**************************************************
//  Invoker
//**************************************************

// types of Invoker variables.
var (
	AsyncFunctionInvoker AsyncInvoker
	SyncFunctionInvoker  SyncInvoker
)

// Invoker defines a type which invokes a function.
type Invoker interface {
	Invoke(func())
}

type AsyncInvoker struct{}

func (AsyncInvoker) Invoke(f func()) {
	go f()
}

type SyncInvoker struct{}

func (SyncInvoker) Invoke(f func()) {
	f()
}

//**************************************************
//  actorSyncSupervisor implements Process and actorkit.Actor
//**************************************************

var _ actorkit.Process = &actorSyncSupervisor{}

// ActorSyncOption defines a function type to be called with a
// actorSyncSupervisor. It provides the means to set internal
// values of type.
type ActorSyncOption func(*actorSyncSupervisor)

// WithMailbox sets the mailbox to be used by an actorSyncSupervisor.
func WithMailbox(mail actorkit.Mailbox) ActorSyncOption {
	return func(s *actorSyncSupervisor) {
		s.mail = mail
	}
}

// WithInvoker sets the function invoker to provider.
func WithInvoker(in Invoker) ActorSyncOption {
	return func(s *actorSyncSupervisor) {
		s.fnInvoker = in
	}
}

// WithAddrs sets the address of the provided actorSyncSupervisor.
func WithAddrs(addr string) ActorSyncOption {
	return func(s *actorSyncSupervisor) {
		s.addr = addr
	}
}

// WithID sets the actor processor ID.
func WithID(id xid.ID) ActorSyncOption {
	return func(s *actorSyncSupervisor) {
		s.id = id
	}
}

// WithMailInvoker sets the actor processor to use provided invoker.
func WithMailInvoker(in actorkit.MailInvoker) ActorSyncOption {
	return func(s *actorSyncSupervisor) {
		s.mailInvoker = in
	}
}

// WithMessageInvoker sets the actor processor to use provided invoker.
func WithMessageInvoker(in actorkit.MessageInvoker) ActorSyncOption {
	return func(s *actorSyncSupervisor) {
		s.invoker = in
	}
}

// WithLocalEscalator sets a local escalator be called by the
// actorSyncSupervisor.
func WithLocalEscalator(es actorkit.Escalator) ActorSyncOption {
	return func(s *actorSyncSupervisor) {
		s.escalator = &escalateDistributor{
			src:         s,
			local:       es,
			Distributor: actorkit.GetDistributor(),
		}
	}
}

// FromFunc returns a new actorkit.Process using the provided actorkit.ActorFunc.
func FromFunc(fn actorkit.ActorFunc, ops ...ActorSyncOption) actorkit.Process {
	return FromActor(actorkit.FromFunc(fn), ops...)
}

// FromActor returns a new actorkit.Process using the provided actorkit.Actor.
func FromActor(action actorkit.Actor, ops ...ActorSyncOption) actorkit.Process {
	ac := new(actorSyncSupervisor)
	ac.id = xid.New()
	ac.behaviour = action
	ac.actions = make(chan func(), 1)
	ac.closer = make(chan struct{}, 1)
	ac.watchers = actorkit.NewWatchers()

	for _, op := range ops {
		op(ac)
	}

	if ac.fnInvoker == nil {
		ac.fnInvoker = SyncFunctionInvoker
	}

	if ac.mail == nil {
		ac.mail = mailbox.UnboundedBoxQueue()
	}

	if ac.addr == "" {
		ac.addr = actorkit.LocalNetworkAddr
	}

	ac.wg.Add(1)
	go ac.run()
	return ac
}

// actorSyncSupervisor implements the Process interface and
// provides a basic processor management for an actorkit.Actor.
type actorSyncSupervisor struct {
	id   xid.ID
	addr string

	do        sync.Once
	wg        sync.WaitGroup
	stopped   int64
	received  int64
	processed int64

	actions chan func()
	closer  chan struct{}

	fnInvoker   Invoker
	mail        actorkit.Mailbox
	behaviour   actorkit.Actor
	escalator   *escalateDistributor
	invoker     actorkit.MessageInvoker
	mailInvoker actorkit.MailInvoker
	watchers    *actorkit.Watchers
}

func (m *actorSyncSupervisor) RemoveWatcher(mw actorkit.Mask) {
	m.watchers.RemoveWatcher(mw)
}

func (m *actorSyncSupervisor) AddWatcher(mw actorkit.Mask, fn func(interface{})) {
	m.watchers.AddWatcher(mw, fn)
}

// Address returns the associated network address of the actor.
func (m *actorSyncSupervisor) Address() string {
	return m.addr
}

// ID returns the associated ID of the implementation.
func (m *actorSyncSupervisor) ID() string {
	return m.id.String()
}

// Wait will cause a block till the actor is fully closed.
func (m *actorSyncSupervisor) Wait() {
	m.wg.Wait()
}

// GracefulStop attempts to gracefully stop the actor.
func (m *actorSyncSupervisor) GracefulStop() actorkit.Waiter {
	m.Stop()
	return m
}

// Stopped returns true/false if processor has being stopped.
func (m *actorSyncSupervisor) Stopped() bool {
	return atomic.LoadInt64(&m.received) > 0
}

// Stop sends a signal to stop processor operation.
func (m *actorSyncSupervisor) Stop() {
	m.do.Do(func() {
		m.closer <- struct{}{}
		atomic.StoreInt64(&m.stopped, 1)
	})
}

// Receive delivers an incoming message to process actor for processing.
func (m *actorSyncSupervisor) Receive(myMask actorkit.Mask, env actorkit.Envelope) {
	myenv := queuedPool.Get().(*actorkit.QueuedEnvelope)
	myenv.MyMask = myMask
	myenv.Envelope = env

	atomic.AddInt64(&m.received, 1)
	m.mail.Push(myenv)
	if m.mailInvoker != nil {
		m.mailInvoker.InvokeReceived(env)
	}

	select {
	case m.actions <- m.doNext:
	default:
	}
}

func (m *actorSyncSupervisor) doNext() {
	var next *actorkit.QueuedEnvelope

	defer func() {
		if reason := recover(); reason != nil {
			if m.invoker != nil {
				m.invoker.InvokePanicked(next.Envelope, reason)
			}
		}

		next.MyMask = nil
		next.Envelope = nil
		queuedPool.Put(next)
	}()

	var ok bool
	if next, ok = m.mail.Pop().(*actorkit.QueuedEnvelope); next != nil && ok {
		if m.invoker != nil {
			m.invoker.InvokeRequest(next.MyMask, next.Envelope)
		}

		atomic.AddInt64(&m.processed, 1)

		if m.invoker != nil {
			m.invoker.InvokeMessageProcessing(next.Envelope)
		}

		if m.escalator != nil {
			m.behaviour.Respond(next.MyMask, next.Envelope, m.escalator)
		} else {
			m.behaviour.Respond(next.MyMask, next.Envelope, actorkit.GetDistributor())
		}

		if m.invoker != nil {
			m.invoker.InvokeMessageProcessed(next.Envelope)
		}

		// if we still have message, then signal to
		// process next.
		if !m.mail.Empty() {
			m.actions <- m.doNext
		}
	}
}

func (m *actorSyncSupervisor) run() {
	defer m.wg.Done()

	mymask := actorkit.ForceMaskWithProcess("process", m)
	defer func() {
		perr := recover()
		msg := actorkit.ProcessFinishedShutdown{
			ID:    m.id.String(),
			Panic: perr,
			Mail:  m.mail,
		}

		// inform all watchers
		m.watchers.Inform(msg)

		// inform all listeners
		actorkit.Publish(msg)

		if m.escalator != nil {
			m.behaviour.Respond(
				mymask,
				actorkit.LocalEnvelope(
					m.id.String(),
					actorkit.Header{},
					actorkit.GetDeadletter(),
					&msg,
				),
				m.escalator,
			)
		} else {
			m.behaviour.Respond(
				mymask,
				actorkit.LocalEnvelope(
					m.id.String(),
					actorkit.Header{},
					actorkit.GetDeadletter(),
					&msg,
				),
				actorkit.GetDistributor(),
			)
		}
	}()

	initialMsg := actorkit.ProcessStarted{ID: m.id.String()}

	// inform all watchers
	m.watchers.Inform(initialMsg)

	// inform all listeners
	actorkit.Publish(initialMsg)

	if m.escalator != nil {
		m.behaviour.Respond(
			mymask,
			actorkit.LocalEnvelope(m.id.String(), actorkit.Header{}, actorkit.GetDeadletter(), &initialMsg),
			m.escalator,
		)
	} else {
		m.behaviour.Respond(
			mymask,
			actorkit.LocalEnvelope(m.id.String(), actorkit.Header{}, actorkit.GetDeadletter(), &initialMsg),
			actorkit.GetDistributor(),
		)
	}

	for {
		select {
		case <-m.closer:
			lastMsg := actorkit.ProcessShuttingDown{ID: m.id.String()}
			// inform all watchers
			m.watchers.Inform(lastMsg)

			// inform all listeners
			actorkit.Publish(lastMsg)

			if m.escalator != nil {
				m.behaviour.Respond(
					mymask,
					actorkit.LocalEnvelope(
						m.id.String(),
						actorkit.Header{},
						actorkit.GetDeadletter(),
						&lastMsg,
					),
					m.escalator,
				)
			} else {
				m.behaviour.Respond(
					mymask,
					actorkit.LocalEnvelope(
						m.id.String(),
						actorkit.Header{},
						actorkit.GetDeadletter(),
						&lastMsg,
					),
					actorkit.GetDistributor(),
				)
			}

			return
		case action := <-m.actions:
			m.fnInvoker.Invoke(action)
		default:
		}
	}
}

type escalateDistributor struct {
	actorkit.Distributor
	local actorkit.Escalator
	src   *actorSyncSupervisor
}

func (es *escalateDistributor) EscalateFailure(by actorkit.Mask, env actorkit.Envelope, reason interface{}) {
	if es.src.invoker != nil {
		es.src.invoker.InvokeEscalateFailure(by, env, reason)
	}

	es.local.EscalateFailure(by, env, reason)
	es.Distributor.EscalateFailure(by, env, reason)
}
