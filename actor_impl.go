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
	defaultFlood          = 10
	defaultDeadLockTicker = time.Second * 5
)

// errors ...
var (
	ErrActorState        = errors.New("Actor is within an error state")
	ErrMustBeRunning     = errors.New("Actor must be running")
	ErrAlreadyStopped    = errors.Wrap(ErrActorState, "Actor already stopped")
	ErrAlreadyStarted    = errors.Wrap(ErrActorState, "Actor already started")
	ErrAlreadyStopping   = errors.Wrap(ErrActorState, "Actor already stopping")
	ErrAlreadyStarting   = errors.Wrap(ErrActorState, "Actor already starting")
	ErrAlreadyRestarting = errors.Wrap(ErrActorState, "Actor already restarting")
)

//********************************************************
// Actor Constructors
//********************************************************

// FromPartial returns a ActorSpawner which will be used for spawning new Actor using
// provided options both from the call to FromPartial and those passed to the returned function.
func FromPartial(ops ...ActorOption) ActorSpawner {
	return func(more ...ActorOption) Actor {
		ops = append(ops, more...)
		return NewActorImpl(ops...)
	}
}

// FromOptions returns a new Function which spawns giving Actor based on provided ActorOptions.
func FromOptions(ops ...ActorOption) func() Actor {
	return func() Actor {
		return NewActorImpl(ops...)
	}
}

// From returns a new spawned and not yet started Actor based on provided ActorOptions.
func From(ops ...ActorOption) Actor {
	return NewActorImpl(ops...)
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
func System(protocol string, namespace string, ops ...ActorOption) (Addr, Actor, error) {
	ops = append(ops, UseBehaviour(&DeadLetterBehaviour{}), Protocol(protocol), Namespace(namespace))
	actor := NewActorImpl(ops...)
	err := actor.Start().Wait()
	return AccessOf(actor), actor, err
}

//********************************************************
// ActorOptions
//********************************************************

// ActorOption defines a function type which is used to set giving
// field values for a ActorImpl instance
type ActorOption func(*ActorImpl)

// ActorSpawner defines a function interface which takes a giving set of
// options returns a new instantiated Actor.
type ActorSpawner func(...ActorOption) Actor

// UseMailbox sets the mailbox to be used by the actor.
func UseMailbox(m Mailbox) ActorOption {
	return func(ac *ActorImpl) {
		ac.mails = m
	}
}

// Protocol sets giving protocol value for a actor.
func Protocol(proc string) ActorOption {
	return func(impl *ActorImpl) {
		impl.protocol = proc
	}
}

// ForceXID forces giving id value for a actor.
func ForceXID(id xid.ID) ActorOption {
	return func(impl *ActorImpl) {
		impl.id = id
	}
}

// ForceID forces giving id value for a actor if the id is a valid xid (github.com/gokit/xid) id string.
func ForceID(forceID string) ActorOption {
	return func(impl *ActorImpl) {
		if id, err := xid.FromString(forceID); err != nil {
			impl.id = id
		}
	}
}

// Namespace sets giving namespace value for a actor.
func Namespace(ns string) ActorOption {
	return func(impl *ActorImpl) {
		impl.namespace = ns
	}
}

// UseDeadLockTicker sets the duration for giving anti-deadlock
// ticker which is runned for every actor, to avoid all goroutines
// sleeping error. Set within durable time, by default 5s is used.
func UseDeadLockTicker(dur time.Duration) ActorOption {
	return func(ac *ActorImpl) {
		ac.deadLockDur = dur
	}
}

// UseParent sets the parent to be used by the actor.
func UseParent(a Actor) ActorOption {
	return func(ac *ActorImpl) {
		ac.parent = a
	}
}

// UseSupervisor sets the supervisor to be used by the actor.
func UseSupervisor(s Supervisor) ActorOption {
	return func(ac *ActorImpl) {
		ac.supervisor = s
	}
}

// UseEventStream sets the event stream to be used by the actor.
func UseEventStream(es *es.EventStream) ActorOption {
	return func(ac *ActorImpl) {
		ac.events = es
	}
}

// UseMailInvoker sets the mail invoker to be used by the actor.
func UseMailInvoker(st MailInvoker) ActorOption {
	return func(ac *ActorImpl) {
		ac.mailInvoker = st
	}
}

// UseBehaviour sets the behaviour to be used by a given actor.
func UseBehaviour(bh Behaviour) ActorOption {
	return func(ac *ActorImpl) {
		ac.receiver = bh
		if tm, ok := bh.(PreStart); ok {
			ac.preStart = tm
		}
		if tm, ok := bh.(PostStart); ok {
			ac.postStart = tm
		}
		if tm, ok := bh.(PreDestroy); ok {
			ac.preDestroy = tm
		}
		if tm, ok := bh.(PostDestroy); ok {
			ac.postDestroy = tm
		}
		if tm, ok := bh.(PreRestart); ok {
			ac.preRestart = tm
		}
		if tm, ok := bh.(PostRestart); ok {
			ac.postRestart = tm
		}
		if tm, ok := bh.(PreStop); ok {
			ac.preStop = tm
		}
		if tm, ok := bh.(PostStop); ok {
			ac.postStop = tm
		}
	}
}

// UseStateInvoker sets the state invoker to be used by the actor.
func UseStateInvoker(st StateInvoker) ActorOption {
	return func(ac *ActorImpl) {
		ac.stateInvoker = st
	}
}

// UseMessageInvoker sets the message invoker to be used by the actor.
func UseMessageInvoker(st MessageInvoker) ActorOption {
	return func(ac *ActorImpl) {
		ac.messageInvoker = st
	}
}

//********************************************************
// ActorImpl
//********************************************************

var _ Actor = &ActorImpl{}

type actorSub struct {
	Actor Actor
	Sub   Subscription
}

// ActorImpl implements the Actor interface.
type ActorImpl struct {
	namespace   string
	protocol    string
	accessAddr  Addr
	id          xid.ID
	parent      Actor
	mails       Mailbox
	tree        *ActorTree
	deadLockDur time.Duration
	events      *es.EventStream

	stateInvoker   StateInvoker
	mailInvoker    MailInvoker
	messageInvoker MessageInvoker

	destroyChan chan Actor
	startChan   chan actorSub
	actions     chan func()
	closer      chan struct{}
	signal      chan struct{}
	subs        map[Actor]Subscription

	processable *SwitchImpl
	running     *SwitchImpl
	starting    *SwitchImpl
	stopping    *SwitchImpl
	restarting  *SwitchImpl

	supervisor Supervisor
	stats      *ActorStatImpl
	messages   sync.WaitGroup
	routines   sync.WaitGroup

	receiver    Behaviour
	preStop     PreStop
	postStop    PostStop
	preRestart  PreRestart
	postRestart PostRestart
	preStart    PreStart
	postStart   PostStart
	preDestroy  PreDestroy
	postDestroy PostDestroy

	chl   sync.RWMutex
	chain []DiscoveryService
}

// NewActorImpl returns a new instance of an ActorImpl assigned giving protocol and service name.
func NewActorImpl(ops ...ActorOption) *ActorImpl {
	ac := &ActorImpl{}

	for _, op := range ops {
		op(ac)
	}

	if ac.namespace == "" {
		ac.namespace = "localhost"
	}

	if ac.protocol == "" {
		ac.protocol = "kit"
	}

	if ac.deadLockDur <= 0 {
		ac.deadLockDur = defaultDeadLockTicker
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
			Max: 30,
			Invoker: &EventSupervisingInvoker{
				Event: ac.events,
			},
			Direction: func(tm interface{}) Directive {
				switch tm.(type) {
				case ActorPanic, ActorRoutinePanic:
					return PanicDirective
				default:
					return RestartDirective
				}
			},
		}
	}

	ac.subs = map[Actor]Subscription{}
	ac.actions = make(chan func(), 0)
	ac.closer = make(chan struct{}, 1)
	ac.signal = make(chan struct{}, 1)
	ac.destroyChan = make(chan Actor, 0)
	ac.startChan = make(chan actorSub, 0)

	ac.id = xid.New()
	ac.starting = NewSwitch()
	ac.processable = NewSwitch()
	ac.running = NewSwitch()
	ac.stopping = NewSwitch()
	ac.restarting = NewSwitch()
	ac.stats = NewActorStatImpl()
	ac.tree = NewActorTree(10)
	ac.accessAddr = AccessOf(ac)

	ac.processable.On()
	return ac
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
	// if we are not able to process and not restarting then
	// return error.
	if !ati.processable.IsOn() && !ati.restarting.IsOn() {
		return errors.New("actor is unable to handle message")
	}

	// if we cant process, then return error.
	if !ati.processable.IsOn() {
		return errors.New("actor is stopped and hence can't handle message")
	}

	if ati.messageInvoker != nil {
		ati.messageInvoker.InvokedRequest(a, e)
	}

	ati.messages.Add(1)

	return ati.mails.Push(a, e)
}

// Discover returns actor's Addr from this actor's
// discovery chain, else passing up the ladder till it
// reaches the actors root where no possible discovery can be done.
//
// The method will return an error if Actor is not already running.
func (ati *ActorImpl) Discover(service string, ancestral bool) (Addr, error) {
	if !ati.running.IsOn() && !ati.starting.IsOn() {
		return nil, errors.WrapOnly(ErrMustBeRunning)
	}

	ati.chl.RLock()
	for _, disco := range ati.chain {
		if actor, err := disco.Discover(service); err == nil {
			ati.chl.RUnlock()
			if err := ati.manageChild(actor); err != nil {
				return nil, err
			}
			return AddressOf(actor, service), nil
		}
	}
	ati.chl.RUnlock()

	if ati.parent != nil && ancestral {
		return ati.parent.Discover(service, ancestral)
	}

	return nil, errors.New("service %q not found")
}

// Spawn spawns a new actor under this parents tree returning address of
// created actor.
//
// The method will return an error if Actor is not already running.
func (ati *ActorImpl) Spawn(service string, rec Behaviour) (Addr, error) {
	if !ati.running.IsOn() && !ati.starting.IsOn() {
		return nil, errors.WrapOnly(ErrMustBeRunning)
	}

	am := NewActorImpl(Namespace(ati.namespace), Protocol(ati.protocol), UseParent(ati), UseBehaviour(rec))
	if err := ati.manageChild(am); err != nil {
		return nil, err
	}
	return AddressOf(am, service), nil
}

// manageChild handles addition of new actor into actor tree.
func (ati *ActorImpl) manageChild(an Actor) error {
	sub := an.Watch(func(event interface{}) {
		switch event.(type) {
		case ActorDestroyed:
			ati.destroyChan <- an
		}
	})

	// send actor for registration
	ati.startChan <- actorSub{Actor: an, Sub: sub}

	// setup initiation management for actor
	return ati.setupActor(an)
}

// SetupActor will initialize and start provided actor, if an error
// occurred which is not an ErrActorState then it will be handled to
// this parent's supervisor, which will mitigation actions.
func (ati *ActorImpl) setupActor(ac Actor) error {
	// if actor is already running, just return.
	if ac.Running() {
		return nil
	}

	// if actor is already starting, just return.
	if ac.Starting() {
		return nil
	}

	// if we are currently stopping, then
	// we don't need to start up.
	if ati.stopping.IsOn() {
		return nil
	}

	// if we are restating, then skip
	if ati.restarting.IsOn() {
		return nil
	}

	// if we are not starting or running then skip.
	if !ati.starting.IsOn() && !ati.running.IsOn() {
		return nil
	}

	if err := ac.Start().Wait(); err != nil {
		if !errors.IsAny(err, ErrActorState) {
			ati.supervisor.Handle(err, AccessOf(ac), ac, ati)
		}
		return err
	}

	return nil
}

// Stats returns giving actor stat associated with
// actor.
func (ati *ActorImpl) Stats() Stat {
	return ati.stats
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

// Starting returns true/false if giving actor is in the starting process.
func (ati *ActorImpl) Starting() bool {
	return ati.starting.IsOn()
}

// Running returns true/false if giving actor is running.
func (ati *ActorImpl) Running() bool {
	return ati.running.IsOn()
}

// Stopped returns true/false if given actor is stopped.
func (ati *ActorImpl) Stopped() bool {
	return !ati.running.IsOn()
}

// Start starts off or resumes giving actor operations for processing
// received messages.
func (ati *ActorImpl) Start() ErrWaiter {
	return ati.start(true, false)
}

func (ati *ActorImpl) start(children bool, restart bool) *futurechain.FutureChain {
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
			return errors.Wrap(ErrAlreadyStarting, "Actor %q is already starting", ati.id.String())
		}

		return nil
	}).When(func(_ context.Context) error {
		ati.running.Off()
		ati.stopping.Off()
		ati.restarting.Off()

		ati.starting.On()
		ati.processable.On()

		if len(ati.closer) != 0 {
			<-ati.closer
		}

		if len(ati.signal) != 0 {
			<-ati.signal
		}

		// start off processing go routines.
		ati.startupLifeCycle()

		if restart {
			if ati.preRestart != nil {
				if err := ati.preRestart.PreRestart(ati.accessAddr); err != nil {
					ati.starting.Off()
					return err
				}
			}
		} else {
			ati.events.Publish(ActorStartRequested{
				ID:   ati.id.String(),
				Addr: ati.Addr(),
			})

			if ati.preStart != nil {
				if err := ati.preStart.PreStart(ati.accessAddr); err != nil {
					ati.starting.Off()
					return err
				}
			}
		}

		return nil
	}).When(func(_ context.Context) error {
		ati.startupMessageReader()
		return nil
	})

	// signal children of actor to start as well.
	if children {
		chain.WhenFuture(ati.startChildren(restart))
	}

	chain = chain.When(func(_ context.Context) error {
		if restart {
			if ati.postRestart != nil {
				if err := ati.postRestart.PostRestart(ati.accessAddr); err != nil {
					ati.starting.Off()
					ati.signal <- struct{}{}
					ati.mails.Signal()
					return err
				}
			}
		} else {
			if ati.postStart != nil {
				if err := ati.postStart.PostStart(ati.accessAddr); err != nil {
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
		}

		ati.starting.Off()
		ati.running.On()

		return nil
	})

	return chain
}

// startChildren attempts to start/restart children of actor only.
func (ati *ActorImpl) startChildren(restart bool) *futurechain.FutureChain {
	chain := futurechain.NoWorkChain(context.Background())

	waiter := make(chan struct{}, 0)

	ati.actions <- func() {
		ati.tree.Each(func(actor Actor) bool {
			chain.Go(func(_ context.Context) error {
				var waiter ErrWaiter
				if restart {
					waiter = actor.Restart()
				} else {
					waiter = actor.Start()
				}

				if err := waiter.Wait(); err != nil {
					if !errors.IsAny(err, ErrActorState) {
						ati.supervisor.Handle(err, AccessOf(actor), actor, ati)
					}
					return err
				}

				return nil
			})
			return true
		})
		waiter <- struct{}{}
	}

	<-waiter

	return chain
}

// RestartSelf restarts the actors message processing operations. It
// will immediately resume operations from pending messages within
// mailbox. It will not restart children of actor but only actor's
// internal, this is useful for restarts that affect parent but not children.
func (ati *ActorImpl) RestartSelf() ErrWaiter {
	return ati.restart(false)
}

// RestartChildren restarts all children of giving actor without applying same
// operation to parent.
func (ati *ActorImpl) RestartChildren() ErrWaiter {
	return ati.startChildren(true)
}

// Restart restarts the actors message processing operations. It
// will immediately resume operations from pending messages within
// mailbox. This will also restarts actors children.
func (ati *ActorImpl) Restart() ErrWaiter {
	return ati.restart(true)
}

func (ati *ActorImpl) restart(children bool) *futurechain.FutureChain {
	if !ati.running.IsOn() {
		return ati.start(children, true)
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

		return ati.stop(StopDirective, children, false).Wait()
	}).Then(func(_ context.Context) error {
		ati.processable.On()
		ati.restarting.Off()
		return ati.start(children, true).Wait()
	}).Then(func(_ context.Context) error {
		ati.events.Publish(ActorRestarted{
			Addr: ati.Addr(),
			ID:   ati.id.String(),
		})

		return nil
	})

	return chain
}

// Kill immediately stops the actor and clears all pending messages.
func (ati *ActorImpl) Kill() ErrWaiter {
	return ati.kill()
}

// KillChildren immediately kills giving children of actor.
func (ati *ActorImpl) KillChildren() ErrWaiter {
	return ati.stopChildren(KillDirective)
}

func (ati *ActorImpl) kill() *futurechain.FutureChain {
	return ati.stop(KillDirective, true, false).Then(func(_ context.Context) error {
		for !ati.mails.IsEmpty() {
			if nextAddr, next, err := ati.mails.Pop(); err == nil {
				ati.messages.Done()
				deadLetters.Publish(DeadMail{To: nextAddr, Message: next})
			}
		}
		return nil
	})
}

// Destroy stops giving actor and emits a destruction event which
// will remove giving actor from it's ancestry trees.
func (ati *ActorImpl) Destroy() ErrWaiter {
	return ati.stop(DestroyDirective, true, false).Then(func(_ context.Context) error {
		var err error
		if ati.preDestroy != nil {
			err = ati.preDestroy.PreDestroy(ati.accessAddr)
		}

		for !ati.mails.IsEmpty() {
			if nextAddr, next, err := ati.mails.Pop(); err == nil {
				ati.messages.Done()
				deadLetters.Publish(DeadMail{To: nextAddr, Message: next})
			}
		}

		return err
	}).Then(func(_ context.Context) error {
		ati.tree.Reset()
		return nil
	}).Then(func(_ context.Context) error {
		ati.events.Publish(ActorDestroyed{
			Addr: ati.Addr(),
			ID:   ati.id.String(),
		})

		if ati.postDestroy != nil {
			return ati.postDestroy.PostDestroy(ati.accessAddr)
		}

		return nil
	})
}

// DestroyChildren immediately destroys giving children of actor.
func (ati *ActorImpl) DestroyChildren() ErrWaiter {
	return ati.stopChildren(DestroyDirective)
}

// Stop stops the operations of the actor on processing received messages.
// All pending messages will be kept, so the actor can continue once started.
// To both stop and clear all messages, use ActorImpl.Kill().
func (ati *ActorImpl) Stop() ErrWaiter {
	return ati.stop(StopDirective, true, true)
}

// StopChildren immediately stops all children of actor.
func (ati *ActorImpl) StopChildren() ErrWaiter {
	return ati.stopChildren(StopDirective)
}

// stopChildren attempts to stop/kill/destroy depending on Directive children of
// actor without applying same directive to parent.
func (ati *ActorImpl) stopChildren(dir Directive) *futurechain.FutureChain {
	chain := futurechain.NoWorkChain(context.Background())

	waiter := make(chan struct{}, 0)

	ati.actions <- func() {
		ati.tree.Each(func(actor Actor) bool {
			chain.Go(func(_ context.Context) error {
				switch dir {
				case DestroyDirective:

					// signal to parent to remove actor from tree
					// and stop subscription.
					ati.destroyChan <- actor

					return actor.Destroy().Wait()
				case KillDirective:
					return actor.Kill().Wait()
				default:
					return actor.Stop().Wait()
				}
			})
			return true
		})

		waiter <- struct{}{}
	}

	<-waiter
	return chain
}

func (ati *ActorImpl) stop(dir Directive, children bool, graceful bool) *futurechain.FutureChain {
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
			return ati.preStop.PreStop(ati.accessAddr)
		}

		return nil
	}).Then(func(_ context.Context) error {

		// If we are required to be graceful, then
		// wait till all messages have being
		// processed then stop.
		if graceful {
			ati.messages.Wait()
		}

		return nil
	}).Then(func(_ context.Context) error {
		// schedule closure of actor operation.
		ati.signal <- struct{}{}

		// signal waiter to recheck for new message or close signal.
		ati.mails.Signal()

		return nil
	})

	if children {
		chain.ChainFuture(ati.stopChildren(dir))
	}

	chain = chain.Then(func(_ context.Context) error {
		ati.closer <- struct{}{}

		ati.routines.Wait()

		ati.running.Off()
		ati.processable.Off()

		if ati.postStop != nil {
			return ati.postStop.PostStop(ati.accessAddr)
		}

		return nil
	})

	chain.Then(func(_ context.Context) error {
		ati.stopping.Off()

		ati.events.Publish(ActorStopped{
			ID:   ati.id.String(),
			Addr: ati.Addr(),
		})

		return nil
	})

	return chain
}

func (ati *ActorImpl) startupLifeCycle() {
	ati.routines.Add(1)

	var w sync.WaitGroup
	w.Add(1)
	go func() {
		w.Done()
		ati.manageLifeCycle()
	}()
	w.Wait()
}

func (ati *ActorImpl) startupMessageReader() {
	ati.routines.Add(1)

	var w sync.WaitGroup
	w.Add(1)
	go func() {
		w.Done()
		ati.readMessages()
	}()
	w.Wait()
}

func (ati *ActorImpl) manageLifeCycle() {
	defer func() {
		ati.routines.Done()

		//if err := recover(); err != nil {
		//	trace := make([]byte, stackSize)
		//	coll := runtime.Stack(trace, false)
		//	trace = trace[:coll]
		//
		//	event := ActorRoutinePanic{
		//		Stack: trace,
		//		Panic: err,
		//		Addr:  ati.Addr(),
		//		ID:    ati.id.String(),
		//	}
		//
		//	ati.events.Publish(event)
		//
		//	if !ati.stopping.IsOn() {
		//		go ati.Escalate(event, ati.accessAddr)
		//	}
		//}
	}()

	deadlockTicker := time.NewTicker(ati.deadLockDur)

	for {
		select {
		case <-deadlockTicker.C:
			// do nothing but also allow us avoid
			// possible all goroutine sleep bug.
		case <-ati.closer:
			// end this giving life cycle routine.
			return
		case action := <-ati.actions:
			action()
		case sub := <-ati.startChan:
			// add new actor into tree.
			ati.tree.AddActor(sub.Actor)

			// add actor subscription into map.
			ati.subs[sub.Actor] = sub.Sub
		case actor := <-ati.destroyChan:

			// Remove actor from tree and subscription
			ati.tree.RemoveActor(actor)

			if sub, ok := ati.subs[actor]; ok {
				sub.Stop()
				delete(ati.subs, actor)
			}
		}
	}
}

// readMessages runs the process for executing messages.
func (ati *ActorImpl) readMessages() {
	defer func() {
		ati.routines.Done()

		//if err := recover(); err != nil {
		//	ati.routines.Done()
		//
		//	trace := make([]byte, stackSize)
		//	coll := runtime.Stack(trace, false)
		//	trace = trace[:coll]
		//
		//	event := ActorRoutinePanic{
		//		Stack: trace,
		//		Panic: err,
		//		Addr:  ati.Addr(),
		//		ID:    ati.id.String(),
		//	}
		//
		//	ati.events.Publish(event)
		//
		//	if !ati.stopping.IsOn() {
		//		go ati.Escalate(event, ati.accessAddr)
		//	}
		//
		//	return
		//}
	}()

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

		// Execute receiver behaviour with panic guard.
		ati.process(addr, msg)
	}
}

func (ati *ActorImpl) process(a Addr, x Envelope) {
	// decrease message wait counter.
	ati.messages.Done()

	if ati.messageInvoker != nil {
		ati.messageInvoker.InvokedProcessing(a, x)
	}

	ati.receiver.Action(a, x)

	if ati.messageInvoker != nil {
		ati.messageInvoker.InvokedProcessed(a, x)
	}
}

//********************************************************
// Actor Tree
//********************************************************

// ActorTree implements a hash/dictionary registry for giving actors.
// It combines internally a map and list to take advantage of quick lookup
// and order maintenance.
//
// ActorTree is not safe for concurrent access.
type ActorTree struct {
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
	at.children = nil
	at.registry = map[string]int{}
}

// Length returns the length of actors within tree.
func (at *ActorTree) Length() int {
	return len(at.children)
}

// Each will call giving function on all registered actors,
// it concurrency safe and uses locks underneath. The handler is
// expected to return true/false, this indicates if we want to
// continue iterating in the case of true or to stop iterating in
// the case of false.
func (at *ActorTree) Each(fn func(Actor) bool) {
	// If handler returns true, then continue else stop.
	for _, child := range at.children {
		if !fn(child) {
			return
		}
	}
}

// GetActor returns giving actor from tree using requested id.
func (at *ActorTree) GetActor(id string) (Actor, error) {
	if index, ok := at.registry[id]; ok {
		return at.children[index], nil
	}

	return nil, errors.New("AActor %q not found", id)
}

// RemoveActor removes attached actor from tree if found.
func (at *ActorTree) RemoveActor(c Actor) {
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
	if _, ok := at.registry[c.ID()]; ok {
		return
	}

	currentIndex := len(at.children)
	at.children = append(at.children, c)
	at.registry[c.ID()] = currentIndex
}
