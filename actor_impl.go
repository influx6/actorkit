package actorkit

import (
	"context"
	"sync"

	"github.com/gokit/futurechain"

	"github.com/gokit/errors"
	"github.com/gokit/es"
	"github.com/gokit/xid"
)

// errors ...
var (
	ErrActorState        = errors.New("Actor is within an error state")
	ErrAlreadyStopped    = errors.Wrap(ErrActorState, "Actor already stopped")
	ErrAlreadyStarted    = errors.Wrap(ErrActorState, "Actor already started")
	ErrAlreadyStopping   = errors.Wrap(ErrActorState, "Actor already stopping")
	ErrAlreadyStarting   = errors.Wrap(ErrActorState, "Actor already starting")
	ErrAlreadyRestarting = errors.Wrap(ErrActorState, "Actor already restarting")
)

//********************************************************
// ActorImpl
//********************************************************

var _ Actor = &ActorImpl{}

// ActorImplOption defines a function type which is used to set giving
// field values for a ActorImpl instance
type ActorImplOption func(*ActorImpl)

// UseMailbox sets the mailbox to be used by the actor.
func UseMailbox(m Mailbox) ActorImplOption {
	return func(ac *ActorImpl) {
		ac.mails = m
	}
}

// UseParent sets the parent to be used by the actor.
func UseParent(a Actor) ActorImplOption {
	return func(ac *ActorImpl) {
		ac.parent = a
	}
}

// UseSupervisor sets the supervisor to be used by the actor.
func UseSupervisor(s Supervisor) ActorImplOption {
	return func(ac *ActorImpl) {
		ac.supervisor = s
	}
}

// UseEventStream sets the event stream to be used by the actor.
func UseEventStream(es *es.EventStream) ActorImplOption {
	return func(ac *ActorImpl) {
		ac.events = es
	}
}

// UseMailInvoker sets the mail invoker to be used by the actor.
func UseMailInvoker(st MailInvoker) ActorImplOption {
	return func(ac *ActorImpl) {
		ac.mailInvoker = st
	}
}

// UseBehaviour sets the behaviour to be used by a given actor.
func UseBehaviour(bh Behaviour) ActorImplOption {
	return func(ac *ActorImpl) {
		ac.receiver = bh

		if ss, ok := bh.(StartState); ok {
			ac.startState = ss
		}

		if ss, ok := bh.(StopState); ok {
			ac.stopState = ss
		}
	}
}

// UseStateInvoker sets the state invoker to be used by the actor.
func UseStateInvoker(st StateInvoker) ActorImplOption {
	return func(ac *ActorImpl) {
		ac.stateInvoker = st
	}
}

// UseMessageInvoker sets the message invoker to be used by the actor.
func UseMessageInvoker(st MessageInvoker) ActorImplOption {
	return func(ac *ActorImpl) {
		ac.messageInvoker = st
	}
}

// ActorImpl implements the Actor interface.
type ActorImpl struct {
	namespace      string
	protocol       string
	id             xid.ID
	parent         Actor
	mails          Mailbox
	tree           *ActorTree
	events         *es.EventStream
	stateInvoker   StateInvoker
	messageInvoker MessageInvoker
	mailInvoker    MailInvoker

	running    SwitchImpl
	starting   SwitchImpl
	stopping   SwitchImpl
	restarting SwitchImpl

	supervisor Supervisor
	signal     chan struct{}
	waiter     sync.WaitGroup

	receiver   Behaviour
	stopState  StopState
	startState StartState

	chl   sync.RWMutex
	chain []DiscoveryService
}

// NewActorImpl returns a new instance of an ActorImpl assigned giving protocol and service name.
func NewActorImpl(protocol string, namespace string, ops ...ActorImplOption) *ActorImpl {
	var ac ActorImpl

	for _, op := range ops {
		op(&ac)
	}

	if ac.events == nil {
		ac.events = es.New()
	}

	if ac.mails == nil {
		ac.mails = UnboundedBoxQueue(ac.mailInvoker)
	}

	// if we have no set provider then use a one-for-one strategy.
	if ac.supervisor == nil {
		ac.supervisor = &OneForOneSupervisor{}
	}

	ac.id = xid.New()
	ac.protocol = protocol
	ac.namespace = namespace
	ac.signal = make(chan struct{}, 1)
	ac.tree = NewActorTree(10)
	return &ac
}

// Wait implements the Waiter interface.
func (ati *ActorImpl) Wait() {
	ati.waiter.Wait()
}

// ID returns associated string version of id.
func (ati *ActorImpl) ID() string {
	return ati.id.String()
}

// Mailbox returns actors underline mailbox.
func (ati *ActorImpl) Mailbox() Mailbox {
	return ati.mails
}

// Spawn spawns a new actor under this parents tree returning address of
// created actor.
func (ati *ActorImpl) Spawn(service string, rec Behaviour, conf interface{}) (Addr, error) {
	am := NewActorImpl(ati.protocol, ati.namespace, UseParent(ati))
	return ati.manageActor(service, am, conf)
}

// Discover returns actor's Addr from this actor's
// discovery chain, else passing up the ladder till it
// reaches the actors root where no possible discovery can be done.
func (ati *ActorImpl) Discover(service string, ancestral bool) (Addr, error) {
	ati.chl.RLock()
	defer ati.chl.RUnlock()
	for _, disco := range ati.chain {
		if actor, err := disco.Discover(service); err == nil {
			return ati.manageActor(service, actor, nil)
		}
	}
	if ati.parent == nil {
		return nil, errors.New("service %q not found")
	}
	return ati.parent.Discover(service, ancestral)
}

// AddDiscovery adds new Discovery service into actor's discovery chain.
func (ati *ActorImpl) AddDiscovery(b DiscoveryService) error {
	ati.chl.Lock()
	ati.chain = append(ati.chain, b)
	ati.chl.Unlock()
	return nil
}

// Watch adds provided function as a subscriber to be called
// on events published by actor, it returns a subscription which
// can be used to end giving subscription.
func (ati *ActorImpl) Watch(fn func(interface{})) Subscription {
	return ati.events.Subscribe(fn)
}

// Receive adds giving Envelope into actor's mailbox.
func (ati *ActorImpl) Receive(a Addr, e Envelope) error {
	if ati.messageInvoker != nil {
		ati.messageInvoker.InvokedRequest(a, e)
	}

	return ati.mails.Push(a, e)
}

// manageActor handles addition of new actor into actor tree.
func (ati *ActorImpl) manageActor(service string, an Actor, conf interface{}) (Addr, error) {
	ati.tree.AddActor(an)

	an.Watch(func(event interface{}) {
		switch event.(type) {
		case ActorDestroyed:
			ati.tree.RemoveActor(an)
		}
	})

	go ati.setupActor(an, conf)

	return AddressOf(an, service), nil
}

// SetupActor will initialize and start provided actor, if an error
// occurred which is not an ErrActorState then it will be handled to
// this parent's supervisor, which will mitigation actions.
func (ati *ActorImpl) setupActor(ac Actor, initial interface{}) {
	if err := ac.Start(initial).Wait(); err != nil {
		if !errors.IsAny(err, ErrActorState) {
			ati.supervisor.Handle(err, AccessOf(ac), ac, ati)
		}
	}
}

// Addr returns a url-like representation of giving service by following two giving
// patterns:
//
// 1. If Actor is the highest ancestor then it will return address in form:
//
//		Protocol://Namespace/ID
//
// 2. If Actor is the a child of another, then it will return address in form:
//
//		AncestorAddress/:PROTOCOL/NAMESPACE/ID
//
//    where AncestorAddress is "Protocol://Namespace/ID"
//
// In either case, the protocol of both ancestor and parent is maintained.
// Namespace provides a field area which can define specific information that
// is specific to giving protocol e.g ip v4/v6 address
//
func (ati *ActorImpl) Addr() string {
	if ati.parent == nil {
		return ati.protocol + "://" + ati.namespace + "/" + ati.id.String()
	}
	return ati.parent.Addr() + "/:" + ati.protocol + "/" + ati.namespace + "/" + ati.id.String()
}

// Escalate sends giving error that occur to actor's supervisor
// which can make necessary decision on actions to be done, either
// to escalate to parent's supervisor or restart/stop or handle
// giving actor as dictated by it's algorithm.
func (ati *ActorImpl) Escalate(err interface{}, addr Addr) {
	ati.supervisor.Handle(err, addr, ati, ati.parent)
}

// Ancestor returns the root parent of giving Actor ancestral tree.
func (ati *ActorImpl) Ancestor() Actor {
	if ati.parent == nil {
		return ati
	}
	return ati.parent.Ancestor()
}

// Parent returns the parent of giving Actor.
func (ati *ActorImpl) Parent() Actor {
	if ati.parent == nil {
		return ati
	}
	return ati.parent
}

// Children returns a slice of all addresses of all child actors.
// All address have attached service name "access" for returned address,
// to indicate we are accessing this actors.
func (ati *ActorImpl) Children() []Addr {
	addrs := make([]Addr, 0, ati.tree.Length())
	ati.tree.Each(func(actor Actor) bool {
		addrs = append(addrs, AddressOf(actor, "access"))
		return true
	})
	return addrs
}

// Stopped returns true/false if given actor is stopped.
func (ati *ActorImpl) Stopped() bool {
	return !ati.running.IsOn()
}

// Kill immediately stops the actor and all message processing operations.
// It blocks till actor is absolutely stopped.
func (ati *ActorImpl) Kill(data interface{}) error {
	return ati.Stop(data).Wait()
}

// Start returns a giving waiter which will return a waiter
// which can be used to block until assurance of start completion.
func (ati *ActorImpl) Start(data interface{}) ErrWaiter {
	chain := futurechain.NewFutureChain(context.Background(), func() error {
		if ati.stopping.IsOn() {
			return errors.Wrap(ErrAlreadyStopping, "Actor %q already stopping", ati.id.String())
		}

		if ati.restarting.IsOn() {
			return errors.Wrap(ErrAlreadyRestarting, "Actor %q is restarting", ati.id.String())
		}

		if ati.running.IsOn() {
			return errors.Wrap(ErrAlreadyStarted, "Actor %q is running", ati.id.String())
		}

		if ati.starting.IsOn() {
			return errors.Wrap(ErrAlreadyStarting, "Actor %q is starting", ati.id.String())
		}

		return nil
	}).When(func() error {
		ati.running.Off()
		ati.stopping.Off()
		ati.restarting.Off()

		ati.starting.On()

		ati.events.Publish(ActorStartRequested{
			ID:   ati.id.String(),
			Addr: ati.Addr(),
		})

		if ati.startState != nil {
			if err := ati.startState.PreStart(data); err != nil {
				ati.starting.Off()
				return err
			}
		}

		return nil
	}).When(func() error {
		ati.waiter.Add(1)

		// We need a means of validating that the read message
		// goroutine has started, so we will use a signal channel
		// which will block till it's running.
		up := make(chan struct{}, 1)
		go func() {
			up <- struct{}{}
			ati.readMessages()
		}()

		// Release go-routine once we are sure, readMessage as
		// started.
		<-up
		return nil
	}).When(func() error {

		if ati.startState != nil {
			if err := ati.startState.PostStart(data); err != nil {
				ati.starting.Off()
				ati.signal <- struct{}{}
				ati.mails.Signal()
				return err
			}
		}

		ati.events.Publish(ActorStarted{
			ID:   ati.id.String(),
			Addr: ati.Addr(),
		})

		ati.starting.Off()
		ati.running.On()

		return nil
	})

	// signal children of actor to start as well.
	ati.tree.Each(func(actor Actor) bool {
		chain.Go(func() error {
			return actor.Start(data).Wait()
		})
		return true
	})

	return chain
}

// Stop stops the operations of the actor.
func (ati *ActorImpl) Stop(data interface{}) ErrWaiter {
	chain := futurechain.NewFutureChain(context.Background(), func() error {
		if !ati.running.IsOn() {
			return errors.Wrap(ErrAlreadyStopped, "Actor %q already stopped", ati.id.String())
		}

		if ati.stopping.IsOn() {
			return errors.Wrap(ErrAlreadyStopping, "Actor %q already stopping", ati.id.String())
		}

		return nil
	}).When(func() error {
		ati.events.Publish(ActorStopRequested{
			ID:   ati.id.String(),
			Addr: ati.Addr(),
		})

		// signal we are attempting to stop.
		ati.stopping.On()

		if ati.stopState != nil {
			return ati.stopState.PreStop(data)
		}

		return nil
	}).Then(func() error {

		// schedule closure of actor operation.
		ati.signal <- struct{}{}

		// signal waiter to recheck for new message or close signal.
		ati.mails.Signal()

		ati.waiter.Wait()

		ati.running.Off()

		if ati.stopState != nil {
			return ati.stopState.PostStop(data)
		}

		return nil
	}).Then(func() error {
		ati.stopping.Off()

		ati.events.Publish(ActorStopped{
			ID:   ati.id.String(),
			Addr: ati.Addr(),
		})

		return nil
	})

	ati.tree.Each(func(actor Actor) bool {
		chain.Go(func() error {
			return actor.Stop(data).Wait()
		})
		return true
	})

	return chain
}

// Restart will at attempt to restart actor and it's children.
func (ati *ActorImpl) Restart(data interface{}) ErrWaiter {
	if !ati.running.IsOn() {
		return ati.Start(data)
	}

	chain := futurechain.NewFutureChain(context.Background(), func() error {
		if ati.restarting.IsOn() {
			return errors.Wrap(ErrAlreadyRestarting, "Actor %q already restarting", ati.id.String())
		}
		return nil
	}).When(func() error {
		ati.events.Publish(ActorRestartRequested{
			ID:   ati.id.String(),
			Addr: ati.Addr(),
		})

		ati.restarting.On()

		return ati.Stop(data).Wait()
	}).Then(func() error {
		ati.restarting.Off()

		return ati.Start(data).Wait()
	}).Then(func() error {
		ati.events.Publish(ActorRestarted{
			Data: data,
			Addr: ati.Addr(),
			ID:   ati.id.String(),
		})

		return nil
	})

	return chain
}

// Destroy stops giving actor and emits a destruction event which
// will remove giving actor from it's ancestry trees.
func (ati *ActorImpl) Destroy(data interface{}) ErrWaiter {
	wc := ati.Stop(data)

	ati.events.Publish(ActorDestroyed{
		Addr: ati.Addr(),
		ID:   ati.id.String(),
	})

	return wc
}

func (ati *ActorImpl) readMessages() {
	defer ati.waiter.Done()

	for {
		// block until we have a message, internally
		// we will be put to sleep till there is a message.
		ati.mails.Wait()

		// Make a check after signal of new message, if we
		// are required to shutdown.
		select {
		case <-ati.signal:
			return
		default:
		}

		addr, msg, err := ati.mails.Pop()
		if err != nil {
			continue
		}

		if ati.messageInvoker != nil {
			ati.messageInvoker.InvokedProcessing(addr, msg)
		}

		// Execute receiver behaviour with panic guard.
		func(a Addr, x Envelope) {
			defer func() {
				if ati.messageInvoker != nil {
					ati.messageInvoker.InvokedProcessed(a, x)
				}

				if err := recover(); err != nil {
					ati.Escalate(ActorPanic{
						CausedAddr:    a,
						CausedMessage: x,
						Panic:         err,
						Addr:          ati.Addr(),
						ID:            ati.id.String(),
					}, a)
				}
			}()

			ati.receiver.Action(a, x)
		}(addr, msg)
	}
}

//********************************************************
// Actor Tree
//********************************************************

// ActorTree implements a hash/dictionary registry for giving actors.
// It combines internally a map and list to take advantage of quick lookup
// and order maintenance.
type ActorTree struct {
	cw       sync.RWMutex
	children []Actor
	registry map[string]int
}

// NewActorTree returns a new instance of an actor tree using the initial length
// as capacity to the underline slice for storing actors.
func NewActorTree(initialLength int) *ActorTree {
	return &ActorTree{
		registry: map[string]int{},
		children: make([]Actor, 0, initialLength),
	}
}

// Length returns the length of actors within tree.
func (at *ActorTree) Length() int {
	at.cw.RLock()
	defer at.cw.RUnlock()
	return len(at.children)
}

// Each will call giving function on all registered actors,
// it concurrency safe and uses locks underneath. The handler is
// expected to return true/false, this indicates if we want to
// continue iterating in the case of true or to stop iterating in
// the case of false.
func (at *ActorTree) Each(fn func(Actor) bool) {
	at.cw.RLock()
	defer at.cw.RUnlock()

	// If handler returns true, then continue else stop.
	for _, child := range at.children {
		if !fn(child) {
			return
		}
	}
}

// GetActor returns giving actor from tree using requested id.
func (at *ActorTree) GetActor(id string) (Actor, error) {
	at.cw.RLock()
	defer at.cw.RUnlock()

	if index, ok := at.registry[id]; ok {
		return at.children[index], nil
	}

	return nil, errors.New("AActor %q not found", id)
}

// RemoveActor removes attached actor from tree if found.
func (at *ActorTree) RemoveActor(c Actor) {
	at.cw.Lock()
	defer at.cw.Unlock()

	index := at.registry[c.ID()]
	total := len(at.children)

	item := at.children[total-1]
	if total == 1 {
		delete(at.registry, c.ID())
		at.children = nil
		return
	}

	delete(at.registry, c.ID())
	at.children[index] = item
	at.registry[item.ID()] = index
	at.children = at.children[:total-1]
}

// AddActor adds giving actor into tree.
func (at *ActorTree) AddActor(c Actor) {
	at.cw.Lock()
	defer at.cw.Unlock()

	if _, ok := at.registry[c.ID()]; ok {
		return
	}

	currentIndex := len(at.children)
	at.children = append(at.children, c)
	at.registry[c.ID()] = currentIndex
}
