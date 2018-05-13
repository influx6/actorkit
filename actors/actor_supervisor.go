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
	Addr actorkit.Mask
	Mail actorkit.Mailbox
}

//**************************************************
//  ActorSyncSupervisor implements Process and actorkit.Actor
//**************************************************

var _ actorkit.Process = &ActorSyncSupervisor{}

// ActorSyncOption defines a function type to be called with a
// ActorSyncSupervisor. It provides the means to set internal
// values of type.
type ActorSyncOption func(*ActorSyncSupervisor)

// WithMailbox sets the mailbox to be used by an ActorSyncSupervisor.
func WithMailbox(mail actorkit.Mailbox) ActorSyncOption{
	return func(s *ActorSyncSupervisor) {
		s.mail = mail
	}
}

// WithID sets the actor processor ID.
func WithID(id xid.ID) ActorSyncOption{
	return func(s *ActorSyncSupervisor) {
		s.id = id
	}
}

// WithMailInvoker sets the actor processor to use provided invoker.
func WithMailInvoker(in actorkit.MailInvoker) ActorSyncOption{
	return func(s *ActorSyncSupervisor) {
		s.mailInvoker = in
	}
}

// WithMessageInvoker sets the actor processor to use provided invoker.
func WithMessageInvoker(in actorkit.MessageInvoker) ActorSyncOption{
	return func(s *ActorSyncSupervisor) {
		s.invoker = in
	}
}

// WithLocalEscalator sets a local escalator be called by the
// ActorSyncSupervisor.
func WithLocalEscalator(es actorkit.Escalator) ActorSyncOption{
	return func(s *ActorSyncSupervisor) {
		s.escalator = &escalatableDistributor{
			src: s,
			local: es,
			Distributor: actorkit.GetDistributor(),
		}
	}
}

// ActorSyncSupervisor implements the Process interface and
// provides a basic processor management for an actorkit.Actor.
type ActorSyncSupervisor struct{
	id xid.ID
	mail actorkit.Mailbox
	system actorkit.Mailbox
	invoker actorkit.MessageInvoker
	mailInvoker actorkit.MailInvoker
	actions chan func()
	closer chan struct{}

	wg sync.WaitGroup

	escalator *escalatableDistributor
	processed int64
	received int64

	bl sync.Mutex
	behaviour actorkit.Actor
}

// NewActorSuncSupervisor returns a new instance of a ActorSyncSupervisor.
func NewActorSyncSupervisor(ops ...ActorSyncOption) *ActorSyncSupervisor {
	ac := new(ActorSyncSupervisor)
	for _, op := range ops {
		op(ac)
	}

	ac.system = mailbox.UnboundedBoxQueue()
	if len(ac.id) == 0 {
		ac.id = xid.New()
	}

	if ac.mail == nil {
		ac.mail.Push(actorkit.NEnvelope(ac.id.String(), actorkit.Header{}, actorkit.GetDeadletter(), ActorStarted{}))
	}

	return ac
}

// ID returns the associated ID of the implementation.
func (m *ActorSyncSupervisor) ID()  string {
	return m.id.String()
}

// Wait will cause a block till the actor is fully closed.
func (m *ActorSyncSupervisor) Wait()  {
	m.wg.Wait()
}

// GracefulStop attempts to gracefully stop the actor.
func (m *ActorSyncSupervisor) GracefulStop()  actorkit.Waiter {
	m.closer <- struct{}{}
	return m
}

// Stop sends a signal to stop processor operation.
func (m *ActorSyncSupervisor) Stop()  {
	m.closer <- struct{}{}
}

// Receive delivers an incoming message to process actor for processing.
func (m *ActorSyncSupervisor) Receive(env actorkit.Envelope) {
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

// Supervise uses the provided actor for processing message.
// It is the entry point to startup a ActorSyncSupervisor.
func (m *ActorSyncSupervisor) Supervise(b actorkit.Actor) error {
	m.bl.Lock()
	if m.behaviour != nil {
		m.bl.Unlock()
		return ErrAlreadySupervising
	}

	m.behaviour = b
	m.bl.Unlock()

	m.wg.Add(1)
	m.closer = make(chan struct{}, 1)
	m.actions = make(chan func(), 1)
	go m.run()
	return nil
}

func (m *ActorSyncSupervisor) doNext()  {
	if sysNext := m.system.Pop(); sysNext != nil {
		atomic.AddInt64(&m.processed, 1)

		if m.invoker != nil {
			m.invoker.InvokeSystemMessageProcessing(sysNext)
		}

		if m.escalator != nil {
			m.behaviour.Respond(sysNext, m.escalator)
		}else{
			m.behaviour.Respond(sysNext, actorkit.GetDistributor())
		}

		if m.invoker != nil {
			m.invoker.InvokeSystemMessageProcessed(sysNext)
		}

		// if we still have message, then signal to
		// process next.
		if !m.mail.Empty(){
			m.actions <- m.doNext
		}
	}

	if next := m.mail.Pop(); next != nil {
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

func (m *ActorSyncSupervisor) run()  {
	defer m.wg.Done()
	for {
		select {
		case <-m.closer:
			return
		case action := <-m.actions:
			action()
		default:
		}
	}
}


type escalatableDistributor struct{
	actorkit.Distributor
	local actorkit.Escalator
	src *ActorSyncSupervisor
}

func (es *escalatableDistributor) EscalateFailure(by actorkit.Mask, env actorkit.Envelope, reason interface{}){
	if es.src.invoker != nil {
		es.src.invoker.InvokeEscalateFailure(by, env, reason)
	}

	es.local.EscalateFailure(by, env, reason)
	es.Distributor.EscalateFailure(by, env, reason)
}
