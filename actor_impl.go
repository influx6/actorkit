package actorkit

import (
	"context"
	"sync"
	"time"

	"github.com/gokit/futurechain"

	"github.com/gokit/errors"
	"github.com/gokit/es"
	"github.com/gokit/xid"
)

const (
	threeSecond = time.Second * 3
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
// Directives
//********************************************************

// Directive defines a int type which represents a giving action to be taken
// for an actor.
type Directive int

// directive sets...
const (
	IgnoreDirective Directive = iota
	DestroyDirective
	KillDirective
	StopDirective
	RestartDirective
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

// UseDeadLockTicker sets the duration which must not be less than
// 3 second to ensure intermittent checks on available messages by
// an actor mailbox and also as mitigation of possible deadlocks
// due to goroutine sleep.
func UseDeadLockTicker(dur time.Duration) ActorImplOption {
	return func(ac *ActorImpl) {
		if dur < threeSecond {
			dur = threeSecond
		}

		ac.tickerDur = dur
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

		switch tm := bh.(type) {
		case PreStop:
			ac.preStop = tm
		case PostStop:
			ac.postStop = tm
		case PreStart:
			ac.preStart = tm
		case PostStart:
			ac.postStart = tm
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

	tickerDur      time.Duration
	deadlockTicker *time.Ticker

	running    SwitchImpl
	starting   SwitchImpl
	stopping   SwitchImpl
	restarting SwitchImpl

	supervisor    Supervisor
	signal        chan struct{}
	routines      sync.WaitGroup
	childRoutines sync.WaitGroup

	receiver  Behaviour
	preStop   PreStop
	postStop  PostStop
	preStart  PreStart
	postStart PostStart

	chl   sync.RWMutex
	chain []DiscoveryService
}

// FromProtocol returns a partial function which taking a provided initial data which is optional
// will return a new root actor with giving behaviour, protocol and namespace.
func FromProtocol(ac Behaviour, protocol string, namespace string) func() (Actor, error) {
	return func() (Actor, error) {
		actor := NewActorImpl(protocol, namespace, UseBehaviour(ac))
		err := actor.Start().Wait()
		return actor, err
	}
}

// FromBehaviour returns a partial function which taking a provided initial data which is optional
// will return a new root actor with giving behaviour as factory behaviour for it's message processing.
func FromBehaviour(ac Behaviour) func(string, string) (Actor, error) {
	return func(protocol string, namespace string) (Actor, error) {
		actor := NewActorImpl(protocol, namespace, UseBehaviour(ac))
		err := actor.Start().Wait()
		return actor, err
	}
}

// System is generally used to create an Ancestor actor with a default behaviour, it returns
// the actor itself, it's access address and an error if failed.
//
// Usually you always have one root or system actor per namespace (i.e host:port, ipv6, ..etc),
// then build off your actor system off of it, so do ensure to minimize the
// use of multiple system or ancestor actor roots.
//
// Remember all child actors spawned from an ancestor always takes its protocol and
// namespace.
func System(protocol string, namespace string) (Addr, Actor, error) {
	actor := NewActorImpl(protocol, namespace, UseBehaviour(&DeadLetterBehaviour{}))
	err := actor.Start().Wait()
	return AccessOf(actor), actor, err
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
		ac.supervisor = &OneForOneSupervisor{
			Max: 10,
			Invoker: &EventSupervisingInvoker{
				Event: ac.events,
			},
			Direction: func(tm interface{}) Directive {
				switch tm.(type) {
				case ActorPanic:
					return DestroyDirective
				default:
					return IgnoreDirective
				}
			},
		}
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
	ati.routines.Wait()
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
	am := NewActorImpl(ati.protocol, ati.namespace, UseParent(ati), UseBehaviour(rec))
	return ati.manageActor(service, am)
}

// Discover returns actor's Addr from this actor's
// discovery chain, else passing up the ladder till it
// reaches the actors root where no possible discovery can be done.
func (ati *ActorImpl) Discover(service string, ancestral bool) (Addr, error) {
	ati.chl.RLock()
	for _, disco := range ati.chain {
		if actor, err := disco.Discover(service); err == nil {
			ati.chl.RUnlock()
			return ati.manageActor(service, actor)
		}
	}
	ati.chl.RUnlock()

	if ati.parent != nil && ancestral {
		return ati.parent.Discover(service, ancestral)
	}

	return nil, errors.New("service %q not found")
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

// AdoptChildren processes provided actor by adopting all children of providing actor.
// It moves all children of actor into it's own children tree and clears all
// connection of said actor
func (ati *ActorImpl) AdoptChildren(a Actor) error {
	am, ok := a.(*ActorImpl)
	if ok {
		return errors.New("%T is not an instance of *ActorImpl")
	}

	am.tree.Each(func(actor Actor) bool {
		ati.tree.AddActor(actor)
		return true
	})

	am.tree.Reset()
	return nil
}

// manageActor handles addition of new actor into actor tree.
func (ati *ActorImpl) manageActor(service string, an Actor) (Addr, error) {
	ati.tree.AddActor(an)

	finalize := make(chan struct{}, 1)
	sub := an.Watch(func(event interface{}) {
		switch event.(type) {
		case ActorAdopted:
			// ensure if finalize is full, we don't end up blocking, but simply skip.
			select {
			case finalize <- struct{}{}:
			default:
			}
		case ActorDestroyed:
			// ensure if finalize is full, we don't end up blocking, but simply skip.
			select {
			case finalize <- struct{}{}:
			default:
			}
		}
	})

	ati.childRoutines.Add(2)
	go ati.setupActor(an)
	go ati.finalizeActor(finalize, an, sub)

	return AddressOf(an, service), nil
}

// SetupActor will initialize and start provided actor, if an error
// occurred which is not an ErrActorState then it will be handled to
// this parent's supervisor, which will mitigation actions.
func (ati *ActorImpl) setupActor(ac Actor) {
	defer ati.childRoutines.Done()
	if err := ac.Start().Wait(); err != nil {
		if !errors.IsAny(err, ErrActorState) {
			ati.supervisor.Handle(err, AccessOf(ac), ac, ati)
		}
	}
}

// finalizeActor listens to provided channel for signal once received, will remove actor
// and end subscription.
func (ati *ActorImpl) finalizeActor(signal <-chan struct{}, an Actor, sub Subscription) {
	defer ati.childRoutines.Done()
	<-signal
	ati.tree.RemoveActor(an)
	sub.Stop()
}

// notifyAdoption exports a notification that an actor has being adaopted.
func (ati *ActorImpl) notifyAdoption(addr string, id string) {
	ati.events.Publish(ActorAdopted{
		ByID:   id,
		ByAddr: addr,
		ID:     ati.ID(),
		Addr:   ati.Addr(),
	})
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

// Kill immediately stops the actor and clears all pending messages.
func (ati *ActorImpl) Kill() ErrWaiter {
	return ati.kill()
}

func (ati *ActorImpl) kill() *futurechain.FutureChain {
	return ati.stop(KillDirective, true).Then(func(_ context.Context) error {
		for !ati.mails.IsEmpty() {
			if nextAddr, next, err := ati.mails.Pop(); err == nil {
				deadLetters.Publish(DeadMail{To: nextAddr, Message: next})
			}
		}
		return nil
	})
}

// Start starts off or resumes giving actor operations for processing
// received messages.
func (ati *ActorImpl) Start() ErrWaiter {
	return ati.start(true)
}

func (ati *ActorImpl) start(children bool) *futurechain.FutureChain {
	var parentAddr Addr
	if ati.parent == nil {
		parentAddr = DeadLetters()
	} else {
		parentAddr = AccessOf(ati.parent)
	}

	chain := futurechain.NewFutureChain(context.Background(), func(_ context.Context) error {
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
	}).When(func(_ context.Context) error {
		ati.running.Off()
		ati.stopping.Off()
		ati.restarting.Off()

		ati.starting.On()

		ati.events.Publish(ActorStartRequested{
			ID:   ati.id.String(),
			Addr: ati.Addr(),
		})

		if ati.preStart != nil {
			if err := ati.preStart.PreStart(parentAddr); err != nil {
				ati.starting.Off()
				return err
			}
		}

		return nil
	}).When(func(_ context.Context) error {
		if ati.tickerDur > time.Second {
			ati.deadlockTicker = time.NewTicker(ati.tickerDur)
		}

		ati.manageRoutines()

		return nil
	}).When(func(_ context.Context) error {
		if ati.postStart != nil {
			if err := ati.postStart.PostStart(AccessOf(ati), parentAddr); err != nil {
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
	if children {
		ati.tree.Each(func(actor Actor) bool {
			chain.Go(func(_ context.Context) error {
				return actor.Start().Wait()
			})
			return true
		})
	}

	return chain
}

// Stop stops the operations of the actor on processing received messages.
// All pending messages will be kept, so the actor can continue once started.
// To both stop and clear all messages, use ActorImpl.Kill().
func (ati *ActorImpl) Stop() ErrWaiter {
	return ati.stop(StopDirective, true)
}

func (ati *ActorImpl) stop(dir Directive, children bool) *futurechain.FutureChain {
	var parentAddr Addr
	if ati.parent == nil {
		parentAddr = DeadLetters()
	} else {
		parentAddr = AccessOf(ati.parent)
	}

	chain := futurechain.NewFutureChain(context.Background(), func(_ context.Context) error {
		if !ati.running.IsOn() {
			return errors.Wrap(ErrAlreadyStopped, "Actor %q already stopped", ati.id.String())
		}

		if ati.stopping.IsOn() {
			return errors.Wrap(ErrAlreadyStopping, "Actor %q already stopping", ati.id.String())
		}

		return nil
	}).When(func(_ context.Context) error {
		ati.events.Publish(ActorStopRequested{
			ID:   ati.id.String(),
			Addr: ati.Addr(),
		})

		// signal we are attempting to stop.
		ati.stopping.On()

		if ati.preStop != nil {
			return ati.preStop.PreStop(AccessOf(ati), parentAddr)
		}

		return nil
	}).Then(func(_ context.Context) error {

		// schedule closure of actor operation.
		ati.signal <- struct{}{}

		// stop intermittent deadlock fix ticker if set.
		if ati.deadlockTicker != nil {
			ati.deadlockTicker.Stop()
		}

		// signal waiter to recheck for new message or close signal.
		ati.mails.Signal()

		ati.routines.Wait()

		ati.running.Off()

		if ati.postStop != nil {
			return ati.postStop.PostStop(parentAddr)
		}

		return nil
	}).Then(func(_ context.Context) error {
		ati.stopping.Off()

		ati.events.Publish(ActorStopped{
			ID:   ati.id.String(),
			Addr: ati.Addr(),
		})

		return nil
	})

	if children {
		// Add children routines wait ending routines.
		chain.Go(func(_ context.Context) error {
			ati.childRoutines.Wait()
			return nil
		})

		ati.tree.Each(func(actor Actor) bool {
			chain.Go(func(_ context.Context) error {
				switch dir {
				case DestroyDirective:
					return actor.Destroy().Wait()
				case KillDirective:
					return actor.Kill().Wait()
				default:
					return actor.Stop().Wait()
				}
			})
			return true
		})
	}

	return chain
}

// RestartSelf restarts the actors message processing operations. It
// will immediately resume operations from pending messages within
// mailbox. It will not restart children of actor but only actor's
// internal, this is useful for restarts that affect parent but not children.
func (ati *ActorImpl) RestartSelf() ErrWaiter {
	return ati.restart(false)
}

// Restart restarts the actors message processing operations. It
// will immediately resume operations from pending messages within
// mailbox. This will also restarts actors children.
func (ati *ActorImpl) Restart() ErrWaiter {
	return ati.restart(true)
}

func (ati *ActorImpl) restart(children bool) *futurechain.FutureChain {
	if !ati.running.IsOn() {
		return ati.start(children)
	}

	chain := futurechain.NewFutureChain(context.Background(), func(_ context.Context) error {
		if ati.restarting.IsOn() {
			return errors.Wrap(ErrAlreadyRestarting, "Actor %q already restarting", ati.id.String())
		}
		return nil
	}).When(func(_ context.Context) error {
		ati.events.Publish(ActorRestartRequested{
			ID:   ati.id.String(),
			Addr: ati.Addr(),
		})

		ati.restarting.On()

		return ati.stop(StopDirective, children).Wait()
	}).Then(func(_ context.Context) error {
		ati.restarting.Off()
		return ati.start(children).Wait()
	}).Then(func(_ context.Context) error {
		ati.events.Publish(ActorRestarted{
			Addr: ati.Addr(),
			ID:   ati.id.String(),
		})

		return nil
	})

	return chain
}

// Destroy stops giving actor and emits a destruction event which
// will remove giving actor from it's ancestry trees.
func (ati *ActorImpl) Destroy() ErrWaiter {
	return ati.stop(DestroyDirective, true).Then(func(_ context.Context) error {
		for !ati.mails.IsEmpty() {
			if nextAddr, next, err := ati.mails.Pop(); err == nil {
				deadLetters.Publish(DeadMail{To: nextAddr, Message: next})
			}
		}
		return nil
	}).Then(func(_ context.Context) error {
		ati.tree.Reset()
		return nil
	}).Then(func(_ context.Context) error {
		ati.events.Publish(ActorDestroyed{
			Addr: ati.Addr(),
			ID:   ati.id.String(),
		})
		return nil
	})
}

// manageRoutines will setup go-routines workers for giving actor,
// adding signal to block till giving routines have initialized and
// are started.
// We need a means of validating that the read message
// goroutine has started, so we will use a signal channel
// which will block till it's running.
func (ati *ActorImpl) manageRoutines() {
	ati.routines.Add(2)

	signal := make(chan struct{}, 2)

	go func() {
		signal <- struct{}{}
		ati.readMessages()
	}()

	go func() {
		signal <- struct{}{}
		ati.manageDeadlock()
	}()

	<-signal
	<-signal
}

// manageDeadlock runs indefinite loop per a giving ticking duration, where
// it signals for a recheck on available messages on the mail. It exists due
// to the nature of possible deadlock when all goroutines fall asleep.
func (ati *ActorImpl) manageDeadlock() {
	defer ati.routines.Done()
	if ati.deadlockTicker == nil {
		return
	}

	for range ati.deadlockTicker.C {
		ati.mails.Signal()
	}
}

// readMessages runs the process for executing messages.
func (ati *ActorImpl) readMessages() {
	defer ati.routines.Done()

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
					event := ActorPanic{
						CausedAddr:    a,
						CausedMessage: x,
						Panic:         err,
						Addr:          ati.Addr(),
						ID:            ati.id.String(),
					}

					ati.events.Publish(event)
					ati.Escalate(event, a)
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

// Reset resets content of actor tree, removing all children and registry.
func (at *ActorTree) Reset() {
	at.cw.Lock()
	defer at.cw.Unlock()
	at.children = nil
	at.registry = map[string]int{}
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
