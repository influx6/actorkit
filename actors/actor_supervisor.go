package actors

import (
	"errors"
	"sync/atomic"
	"sync"
	"github.com/rs/xid"
	"github.com/gokit/actorkit"
	"github.com/gokit/actorkit/mailbox"
)


var (
	ErrAlreadySupervising = errors.New("already supervising")
)


//**************************************************
//  actorkit.Actor System Messages
//**************************************************

// ActorStarted is sent when an actor has begun it's operation.
type ActorStarted struct{}

// ActorShuttingDown is send when an actor is requested to shutdown.
type ActorShuttingDown struct{
	Mask actorkit.Mask
	Mail actorkit.Mailbox
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
func WithMailbox(mail actorkit.Mailbox) ActorSyncOption{
	return func(s *actorSyncSupervisor) {
		s.mail = mail
	}
}

// WithID sets the actor processor ID.
func WithID(id xid.ID) ActorSyncOption{
	return func(s *actorSyncSupervisor) {
		s.id = id
	}
}

// WithMailInvoker sets the actor processor to use provided invoker.
func WithMailInvoker(in actorkit.MailInvoker) ActorSyncOption{
	return func(s *actorSyncSupervisor) {
		s.mailInvoker = in
	}
}

// WithMessageInvoker sets the actor processor to use provided invoker.
func WithMessageInvoker(in actorkit.MessageInvoker) ActorSyncOption{
	return func(s *actorSyncSupervisor) {
		s.invoker = in
	}
}

// WithLocalEscalator sets a local escalator be called by the
// actorSyncSupervisor.
func WithLocalEscalator(es actorkit.Escalator) ActorSyncOption{
	return func(s *actorSyncSupervisor) {
		s.escalator = &escalateDistributor{
			src: s,
			local: es,
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
	ac.closer = make(chan struct{}, 1)
	ac.actions = make(chan func(), 1)

	for _, op := range ops {
		op(ac)
	}

	if ac.mail == nil {
		ac.mail = mailbox.UnboundedBoxQueue()
	}

	ac.wg.Add(1)
	go ac.run()

	return ac
}

// actorSyncSupervisor implements the Process interface and
// provides a basic processor management for an actorkit.Actor.
type actorSyncSupervisor struct{
	id xid.ID

	do sync.Once
	wg sync.WaitGroup
	processed int64
	received int64

	actions chan func()
	closer chan struct{}

	mail actorkit.Mailbox
	behaviour actorkit.Actor
	escalator *escalateDistributor
	invoker actorkit.MessageInvoker
	mailInvoker actorkit.MailInvoker
}

// ID returns the associated ID of the implementation.
func (m *actorSyncSupervisor) ID()  string {
	return m.id.String()
}

// Wait will cause a block till the actor is fully closed.
func (m *actorSyncSupervisor) Wait()  {
	m.wg.Wait()
}

// GracefulStop attempts to gracefully stop the actor.
func (m *actorSyncSupervisor) GracefulStop()  actorkit.Waiter {
	m.Stop()
	return m
}

// Stop sends a signal to stop processor operation.
func (m *actorSyncSupervisor) Stop()  {
	m.do.Do(func(){
		m.closer <- struct{}{}
	})
}

// Receive delivers an incoming message to process actor for processing.
func (m *actorSyncSupervisor) Receive(env actorkit.Envelope) {
	atomic.AddInt64(&m.received, 1)
	m.mail.Push(env)

	select{
	case m.actions <- m.doNext:
		if m.invoker != nil {
			m.invoker.InvokeRequest(env)
		}
	default:
	}
}

func (m *actorSyncSupervisor) doNext()  {
	var next actorkit.Envelope

	defer func() {
		if reason := recover(); reason != nil {
			if m.invoker == nil {return}
			m.invoker.InvokePanicked(next, reason)
		}
	}()

	if next = m.mail.Pop(); next != nil {
		atomic.AddInt64(&m.processed, 1)

		if m.invoker != nil {
			m.invoker.InvokeMessageProcessing(next)
		}

		if m.escalator != nil {
			m.behaviour.Respond(next, m.escalator)
		}else{
			m.behaviour.Respond(next, actorkit.GetDistributor())
		}

		if m.invoker != nil {
			m.invoker.InvokeMessageProcessed(next)
		}

		// if we still have message, then signal to
		// process next.
		if !m.mail.Empty(){
			m.actions <- m.doNext
		}
	}
}

func (m *actorSyncSupervisor) run()  {
	defer m.wg.Done()

	if m.escalator != nil {
		m.behaviour.Respond(
			actorkit.NEnvelope(m.id.String(), actorkit.Header{}, actorkit.GetDeadletter(), ActorStarted{}),
			m.escalator,
		)
	}else{
		m.behaviour.Respond(
			actorkit.NEnvelope(m.id.String(), actorkit.Header{}, actorkit.GetDeadletter(), ActorStarted{}),
			actorkit.GetDistributor(),
		)
	}

	for {
		select {
		case <-m.closer:
			mymask := actorkit.ForceMaskWithProcess(actorkit.AnyNetworkAddr, "process", m)

			// ensure we deal with system messages.
			// if we have pending system messages, ensure all
			// system messages are dealt with first.
			if m.escalator != nil {
				m.behaviour.Respond(
					actorkit.NEnvelope(m.id.String(), actorkit.Header{}, actorkit.GetDeadletter(), ActorShuttingDown{
						Mail: m.mail,
						Mask: mymask,
					}),
					m.escalator,
				)
			}else{
				m.behaviour.Respond(
					actorkit.NEnvelope(m.id.String(), actorkit.Header{}, actorkit.GetDeadletter(), ActorShuttingDown{
						Mail: m.mail,
						Mask: mymask,
					}),
					actorkit.GetDistributor(),
				)
			}

			return
		case action := <-m.actions:
			action()
		default:
		}
	}
}

type escalateDistributor struct{
	actorkit.Distributor
	local actorkit.Escalator
	src *actorSyncSupervisor
}

func (es *escalateDistributor) EscalateFailure(by actorkit.Mask, env actorkit.Envelope, reason interface{}){
	if es.src.invoker != nil {
		es.src.invoker.InvokeEscalateFailure(by, env, reason)
	}

	es.local.EscalateFailure(by, env, reason)
	es.Distributor.EscalateFailure(by, env, reason)
}
