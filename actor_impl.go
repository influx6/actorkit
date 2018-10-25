package actorkit

import (
	"context"
	"sync"

	"github.com/gokit/errors"
	"github.com/gokit/es"
	"github.com/gokit/xid"
	"golang.org/x/sync/errgroup"
)

// errors ...
var (
	ErrAlreadyStopped    = errors.New("Actor already stopped")
	ErrAlreadyStarted    = errors.New("Actor already started")
	ErrAlreadyStopping   = errors.New("Actor already stopping")
	ErrAlreadyStarting   = errors.New("Actor already starting")
	ErrAlreadyRestarting = errors.New("Actor already restarting")
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

		if rs, ok := bh.(RestartState); ok {
			ac.restartState = rs
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

	receiver     Behaviour
	stopState    StopState
	startState   StartState
	restartState RestartState

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
func (ati *ActorImpl) Spawn(service string, rec Behaviour) (Addr, error) {
	am := NewActorImpl(ati.protocol, ati.namespace, UseParent(ati))
	return ati.manageActor(service, am)
}

// Discover returns actor's Addr from this actor's
// discovery chain, else passing up the ladder till it
// reaches the actors root where no possible discovery can be done.
func (ati *ActorImpl) Discover(service string, ancestral bool) (Addr, error) {
	ati.chl.RLock()
	defer ati.chl.RUnlock()
	for _, disco := range ati.chain {
		if actor, err := disco.Discover(service); err == nil {
			return ati.manageActor(service, actor)
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
func (ati *ActorImpl) manageActor(service string, an Actor) (Addr, error) {
	ati.tree.AddActor(an)

	an.Watch(func(event interface{}) {
		switch event.(type) {
		case ActorDestroyed:
			ati.tree.RemoveActor(an)
		}
	})

	return AddressOf(an, service), nil
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
	errwc, _ := errgroup.WithContext(context.Background())

	if ati.stopping.IsOn() {
		errwc.Go(func() error {
			return errors.WrapOnly(ErrAlreadyStopping)
		})
		return errwc
	}

	if ati.restarting.IsOn() {
		errwc.Go(func() error {
			return errors.WrapOnly(ErrAlreadyRestarting)
		})
		return errwc
	}

	if ati.running.IsOn() {
		errwc.Go(func() error {
			return errors.WrapOnly(ErrAlreadyStarted)
		})
		return errwc
	}

	if ati.starting.IsOn() {
		errwc.Go(func() error {
			return errors.WrapOnly(ErrAlreadyStarting)
		})
		return errwc
	}

	if ati.startState != nil {
		ati.startState.PreStart(data)
	}

	ati.events.Publish(ActorStartRequested{
		ID:   ati.id.String(),
		Addr: ati.Addr(),
	})

	ati.running.Off()
	ati.restarting.Off()

	ati.starting.On()
	ati.starting.On()

	ati.waiter.Add(1)
	go ati.readMessages(ati.receiver)

	ati.events.Publish(ActorStarted{
		ID:   ati.id.String(),
		Addr: ati.Addr(),
	})

	if ati.startState != nil {
		ati.startState.PostStart(data)
	}

	return errwc
}

// Stop stops the operations of the actor.
func (ati *ActorImpl) Stop(data interface{}) ErrWaiter {
	errwc, _ := errgroup.WithContext(context.Background())
	if !ati.running.IsOn() {
		errwc.Go(func() error {
			return errors.WrapOnly(ErrAlreadyStopped)
		})
		return errwc
	}

	if ati.stopping.IsOn() {
		errwc.Go(func() error {
			return errors.WrapOnly(ErrAlreadyStopping)
		})
		return errwc
	}

	if ati.stopState != nil {
		ati.stopState.PreStop(data)
	}

	// signal we are attempting to stop.
	ati.stopping.On()

	// schedule closure of actor operation.
	ati.signal <- struct{}{}

	// signal waiter to recheck for new message or close signal.
	ati.mails.Signal()

	errwc.Go(func() error {
		ati.waiter.Wait()
		return nil
	})

	ati.events.Publish(ActorStopRequested{
		ID:   ati.id.String(),
		Addr: ati.Addr(),
	})

	ati.tree.Each(func(actor Actor) bool {
		errwc.Go(func() error {
			return actor.Stop(data).Wait()
		})
		return true
	})

	// block till we truly have stopped, then immediately
	// publish stop event.
	go func() {
		errwc.Wait()

		if ati.stopState != nil {
			ati.stopState.PostStop(data)
		}

		ati.running.Off()
		ati.stopping.Off()

		ati.events.Publish(ActorStopped{
			ID:   ati.id.String(),
			Addr: ati.Addr(),
		})
	}()

	return errwc
}

// Restart will at attempt to restart actor and it's children.
func (ati *ActorImpl) Restart(data interface{}) ErrWaiter {
	errwc, _ := errgroup.WithContext(context.Background())

	if !ati.running.IsOn() {
		return ati.Start(data)
	}

	if ati.restarting.IsOn() {
		return errwc
	}

	if ati.restartState != nil {
		ati.restartState.PreRestart(data)
	}

	ati.restarting.On()

	ati.events.Publish(ActorRestartRequested{
		ID:   ati.id.String(),
		Addr: ati.Addr(),
	})

	errwc.Go(func() error {
		return ati.Stop(data).Wait()
	})

	errwc.Go(func() error {
		return ati.Start(data).Wait()
	})

	go func() {
		_ = errwc.Wait()

		if ati.restartState != nil {
			ati.restartState.PostRestart(data)
		}

		ati.restarting.Off()

		ati.events.Publish(ActorRestarted{
			Data: data,
			Addr: ati.Addr(),
			ID:   ati.id.String(),
		})
	}()

	return errwc
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

func (ati *ActorImpl) readMessages(receiver Behaviour) {
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

			receiver.Action(a, x)
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
