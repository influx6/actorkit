package actorkit

import (
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gokit/errors"
	"github.com/gokit/es"
	"github.com/gokit/xid"
)

const (
	defaultWaitDuration   = time.Second * 4
	defaultDeadLockTicker = time.Second * 5
)

var (
	// ErrActorBusyState is returned when an actor is processing a state request when another is made.
	ErrActorBusyState = errors.New("Actor is busy with state (STOP|STOPCHILDREN|DESTROY|DESTROY)")

	// ErrActorMustBeRunning is returned when an operation is to be done and the giving actor is not started.
	ErrActorMustBeRunning = errors.New("Actor must be running")

	// ErrActorHasNoBehaviour is returned when an is to start with no attached behaviour.
	ErrActorHasNoBehaviour = errors.New("Actor must be running")
)

//********************************************************
// Behaviour Function
//********************************************************

// BehaviourFunc defines a function type which is wrapped by a
// type implementing the Behaviour interface to be used in a
// actor.
type BehaviourFunc func(Addr, Envelope)

// FromBehaviourFunc returns a new Behaviour from the function.
func FromBehaviourFunc(b BehaviourFunc) Behaviour {
	return &behaviourFunctioner{fn: b}
}

type behaviourFunctioner struct {
	fn BehaviourFunc
}

func (bh *behaviourFunctioner) Action(addr Addr, env Envelope) {
	bh.fn(addr, env)
}

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

// FromPartialFunc defines a giving function which can be supplied a function which will
// be called with provided ActorOption to be used for generating new actors for
// giving options.
func FromPartialFunc(partial func(...ActorOption) Actor, pre ...ActorOption) func(...ActorOption) Actor {
	return func(ops ...ActorOption) Actor {
		pre = append(pre, ops...)
		return partial(pre...)
	}
}

// From returns a new spawned and not yet started Actor based on provided ActorOptions.
func From(ops ...ActorOption) Actor {
	return NewActorImpl(ops...)
}

// FromFunc returns a new actor based on provided function.
func FromFunc(fn BehaviourFunc, ops ...ActorOption) Actor {
	actor := NewActorImpl(ops...)
	UseBehaviour(FromBehaviourFunc(fn))(actor)
	return actor
}

// Func returns a Actor generating function which uses provided BehaviourFunc.
func Func(fn BehaviourFunc) func(...ActorOption) Actor {
	return FromPartialFunc(func(options ...ActorOption) Actor {
		actor := NewActorImpl(options...)
		UseBehaviour(FromBehaviourFunc(fn))(actor)
		return actor
	})
}

// Ancestor create an actor with a default DeadLetter behaviour, where this actor
// is the root node in a tree of actors. It is the entity by which all children
// spawned or discovered from it will be connected to, and allows a group control
// over them.
//
// Usually you always have one root or system actor per namespace (i.e host:port, ipv6, ..etc),
// then build off your child actors from of it, so do ensure to minimize the
// use of multiple system or ancestor actor roots.
//
// Remember all child actors spawned from an ancestor always takes its protocol and
// namespace.
func Ancestor(protocol string, namespace string, ops ...ActorOption) (Addr, error) {
	ops = append(ops, UseBehaviour(&DeadLetterBehaviour{}), UseProtocol(protocol), UseNamespace(namespace))
	actor := NewActorImpl(ops...)
	return AccessOf(actor), actor.Start()
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

// UseProtocol sets giving protocol value for a actor.
func UseProtocol(proc string) ActorOption {
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

// UseSentinel sets giving Sentinel provider for a actor.
func UseSentinel(sn Sentinel) ActorOption {
	return func(impl *ActorImpl) {
		impl.sentinel = sn
	}
}

// UseNamespace sets giving namespace value for a actor.
func UseNamespace(ns string) ActorOption {
	return func(impl *ActorImpl) {
		impl.namespace = ns
	}
}

// DeadLockTicker sets the duration for giving anti-deadlock
// ticker which is runned for every actor, to avoid all goroutines
// sleeping error. Set within durable time, by default 5s is used.
func DeadLockTicker(dur time.Duration) ActorOption {
	return func(ac *ActorImpl) {
		ac.deadLockDur = dur
	}
}

// WaitBusyDuration sets  giving duration to wait for a critical
// call to Stop, Kill, Destroy a actor or it's children before
// the actor returns a ErrActorBusyState error, due to a previously
// busy state operation.
func WaitBusyDuration(dur time.Duration) ActorOption {
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
		} else {
			ac.preStart = nil
		}

		if tm, ok := bh.(PostStart); ok {
			ac.postStart = tm
		} else {
			ac.postStart = nil
		}

		if tm, ok := bh.(PreDestroy); ok {
			ac.preDestroy = tm
		} else {
			ac.preDestroy = nil
		}

		if tm, ok := bh.(PostDestroy); ok {
			ac.postDestroy = tm
		} else {
			ac.postDestroy = nil
		}

		if tm, ok := bh.(PreRestart); ok {
			ac.preRestart = tm
		} else {
			ac.preRestart = nil
		}

		if tm, ok := bh.(PostRestart); ok {
			ac.postRestart = tm
		} else {
			ac.postRestart = nil
		}

		if tm, ok := bh.(PreStop); ok {
			ac.preStop = tm
		} else {
			ac.preStop = nil
		}

		if tm, ok := bh.(PostStop); ok {
			ac.postStop = tm
		} else {
			ac.postStop = nil
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
	busyDur     time.Duration
	events      *es.EventStream

	sentinel       Sentinel
	stateInvoker   StateInvoker
	mailInvoker    MailInvoker
	messageInvoker MessageInvoker

	death               time.Time
	created             time.Time
	failedDeliveryCount int64
	failedRestartCount  int64
	restartedCount      int64
	deliveryCount       int64
	processedCount      int64
	stoppedCount        int64
	killedCount         int64

	addActor            chan Actor
	rmActor             chan Actor
	signal              chan struct{}
	sentinelChan        chan Addr
	killChan            chan chan error
	stopChan            chan chan error
	destroyChan         chan chan error
	killChildrenChan    chan chan error
	restartChildrenChan chan chan error
	stopChildrenChan    chan chan error
	destroyChildrenChan chan chan error

	started     *SwitchImpl
	starting    *SwitchImpl
	destruction *SwitchImpl
	processable *SwitchImpl

	supervisor Supervisor

	proc     sync.WaitGroup
	messages sync.WaitGroup
	routines sync.WaitGroup

	receiver    Behaviour
	preStop     PreStop
	postStop    PostStop
	preRestart  PreRestart
	postRestart PostRestart
	preStart    PreStart
	postStart   PostStart
	preDestroy  PreDestroy
	postDestroy PostDestroy

	sentinelSubs map[Addr]Subscription
	subs         map[Actor]Subscription
	chl          sync.RWMutex
	chain        []DiscoveryService
}

// NewActorImpl returns a new instance of an ActorImpl assigned giving protocol and service name.
func NewActorImpl(ops ...ActorOption) *ActorImpl {
	ac := &ActorImpl{}

	// apply the options.
	for _, op := range ops {
		op(ac)
	}

	// set the namespace to localhost if not set.
	if ac.namespace == "" {
		ac.namespace = "localhost"
	}

	// set the protocol to kit if not set.
	if ac.protocol == "" {
		ac.protocol = "kit"
	}

	// use default deadlock timer.
	if ac.deadLockDur <= 0 {
		ac.deadLockDur = defaultDeadLockTicker
	}

	if ac.busyDur <= 0 {
		ac.busyDur = defaultWaitDuration
	}

	// add event provider.
	if ac.events == nil {
		ac.events = es.New()
	}

	// add unbouned mailbox.
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

	ac.rmActor = make(chan Actor, 0)
	ac.addActor = make(chan Actor, 0)

	ac.signal = make(chan struct{}, 1)
	ac.sentinelChan = make(chan Addr, 1)
	ac.stopChan = make(chan chan error, 1)
	ac.killChan = make(chan chan error, 1)
	ac.destroyChan = make(chan chan error, 1)
	ac.stopChildrenChan = make(chan chan error, 1)
	ac.killChildrenChan = make(chan chan error, 1)
	ac.restartChildrenChan = make(chan chan error, 1)
	ac.destroyChildrenChan = make(chan chan error, 1)

	ac.id = xid.New()
	ac.started = NewSwitch()
	ac.starting = NewSwitch()
	ac.destruction = NewSwitch()
	ac.accessAddr = AccessOf(ac)
	ac.processable = NewSwitch()
	ac.tree = NewActorTree(10)
	ac.subs = map[Actor]Subscription{}

	ac.processable.On()

	return ac
}

// Wait implements the Waiter interface.
func (ati *ActorImpl) Wait() {
	ati.proc.Wait()
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

// WatchOther asks this actor sentinel to advice on behaviour or operation to
// be performed for the provided actor's states (i.e Stopped, Restarted, Killed, Destroyed).
// being watched for.
//
// If actor has no Sentinel then an error is returned.
// Sentinels are required to advice on action for watched actors by watching actor.
func (ati *ActorImpl) WatchOther(addr Addr) error {
	if ati.sentinel == nil {
		return errors.New("Actor does not have a sentinel")
	}

	select {
	case ati.sentinelChan <- addr:
		return nil
	case <-time.After(ati.busyDur):
		return errors.WrapOnly(ErrActorBusyState)
	}
}

// Publish publishes an event into the actor event notification system.
func (ati *ActorImpl) Publish(message interface{}) {
	ati.events.Publish(message)
}

// Receive adds giving Envelope into actor's mailbox.
func (ati *ActorImpl) Receive(a Addr, e Envelope) error {
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
	if !ati.started.IsOn() && !ati.starting.IsOn() {
		return nil, errors.WrapOnly(ErrActorMustBeRunning)
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
	if !ati.started.IsOn() && !ati.starting.IsOn() {
		return nil, errors.WrapOnly(ErrActorMustBeRunning)
	}

	am := NewActorImpl(UseNamespace(ati.namespace), UseProtocol(ati.protocol), UseParent(ati), UseBehaviour(rec))
	if err := ati.manageChild(am); err != nil {
		return nil, err
	}
	return AddressOf(am, service), nil
}

// Stats returns giving actor stat associated with
// actor.
func (ati *ActorImpl) Stats() Stat {
	return Stat{
		Death:          ati.death,
		Creation:       ati.created,
		Killed:         atomic.LoadInt64(&ati.killedCount),
		Stopped:        atomic.LoadInt64(&ati.stoppedCount),
		Delivered:      atomic.LoadInt64(&ati.deliveryCount),
		Processed:      atomic.LoadInt64(&ati.processedCount),
		Restarted:      atomic.LoadInt64(&ati.restartedCount),
		FailedRestarts: atomic.LoadInt64(&ati.failedRestartCount),
		FailedDelivery: atomic.LoadInt64(&ati.failedDeliveryCount),
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

// Namespace returns actor's namespace.
func (ati *ActorImpl) Namespace() string {
	return ati.namespace
}

// ProtocolAddr returns the Actors.Protocol and Actors.Namespace
// values in the format: Protocol@Namespace.
func (ati *ActorImpl) ProtocolAddr() string {
	return ati.protocol + "@" + ati.namespace
}

// Escalate sends giving error that occur to actor's supervisor
// which can make necessary decision on actions to be done, either
// to escalate to parent's supervisor or restart/stop or handle
// giving actor as dictated by it's algorithm.
func (ati *ActorImpl) Escalate(err interface{}, addr Addr) {
	go ati.supervisor.Handle(err, addr, ati, ati.parent)
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
		addrs = append(addrs, AccessOf(actor))
		return true
	})
	return addrs
}

// Running returns true/false if giving actor is running.
func (ati *ActorImpl) Running() bool {
	return ati.started.IsOn()
}

// Start starts off or resumes giving actor operations for processing
// received messages.
func (ati *ActorImpl) Start() error {
	if ati.started.IsOn() {
		return nil
	}

	return ati.runSystem(false)
}

// RestartChildren restarts all children of giving actor without applying same
// operation to parent.
func (ati *ActorImpl) RestartChildren() error {
	res := make(chan error, 1)
	select {
	case ati.restartChildrenChan <- res:
		return <-res
	case <-time.After(ati.busyDur):
		res <- errors.WrapOnly(ErrActorBusyState)
	}
	return <-res
}

// Restart restarts the actors message processing operations. It
// will immediately resume operations from pending messages within
// mailbox. This will also restarts actors children.
func (ati *ActorImpl) Restart() error {
	if !ati.started.IsOn() {
		return ati.Start()
	}

	if err := ati.Stop(); err != nil {
		return err
	}

	if err := ati.runSystem(true); err != nil {
		return err
	}
	return nil
}

// Stop stops the operations of the actor on processing received messages.
// All pending messages will be kept, so the actor can continue once started.
// To both stop and clear all messages, use ActorImpl.Kill().
func (ati *ActorImpl) Stop() error {
	if !ati.started.IsOn() {
		return nil
	}

	res := make(chan error, 1)
	select {
	case ati.stopChan <- res:
		return <-res
	case <-time.After(ati.busyDur):
		res <- errors.WrapOnly(ErrActorBusyState)
	}

	err := <-res

	ati.proc.Wait()

	return err
}

// StopChildren immediately stops all children of actor.
func (ati *ActorImpl) StopChildren() error {
	if !ati.started.IsOn() {
		return nil
	}

	res := make(chan error, 1)
	select {
	case ati.stopChildrenChan <- res:
		return <-res
	case <-time.After(ati.busyDur):
		res <- errors.WrapOnly(ErrActorBusyState)
	}
	return <-res
}

// Kill immediately stops the actor and clears all pending messages.
func (ati *ActorImpl) Kill() error {
	if !ati.started.IsOn() {
		return nil
	}

	res := make(chan error, 1)
	select {
	case ati.killChan <- res:
		return <-res
	case <-time.After(ati.busyDur):
		res <- errors.WrapOnly(ErrActorBusyState)
	}
	return <-res
}

// KillChildren immediately kills giving children of actor.
func (ati *ActorImpl) KillChildren() error {
	if !ati.started.IsOn() {
		return nil
	}

	res := make(chan error, 1)
	select {
	case ati.killChildrenChan <- res:
		return <-res
	case <-time.After(ati.busyDur):
		res <- errors.WrapOnly(ErrActorBusyState)
	}
	return <-res
}

// Destroy stops giving actor and emits a destruction event which
// will remove giving actor from it's ancestry trees.
func (ati *ActorImpl) Destroy() error {
	if !ati.started.IsOn() {
		return nil
	}

	res := make(chan error, 1)
	select {
	case ati.destroyChan <- res:
		return <-res
	case <-time.After(ati.busyDur):
		res <- errors.WrapOnly(ErrActorBusyState)
	}
	return <-res
}

// DestroyChildren immediately destroys giving children of actor.
func (ati *ActorImpl) DestroyChildren() error {
	if !ati.started.IsOn() {
		return nil
	}

	res := make(chan error, 1)
	select {
	case ati.destroyChildrenChan <- res:
		return <-res
	case <-time.After(ati.busyDur):
		res <- errors.WrapOnly(ErrActorBusyState)
	}
	return <-res
}

//****************************************************************
// internal functions
//****************************************************************

func (ati *ActorImpl) runSystem(restart bool) error {
	if ati.receiver == nil {
		return errors.WrapOnly(ErrActorHasNoBehaviour)
	}

	ati.starting.On()
	defer ati.starting.Off()

	ati.proc.Add(1)
	ati.routines.Add(1)

	ati.initRoutines()
	ati.processable.On()

	if restart {
		atomic.AddInt64(&ati.restartedCount, 1)

		ati.events.Publish(ActorRestartRequested{
			ID:   ati.id.String(),
			Addr: ati.accessAddr,
		})

		if ati.preRestart != nil {
			if err := ati.preRestart.PreRestart(ati.accessAddr); err != nil {
				atomic.AddInt64(&ati.failedRestartCount, 1)
				return err
			}
		}
	} else {
		ati.events.Publish(ActorStartRequested{
			ID:   ati.id.String(),
			Addr: ati.accessAddr,
		})

		if ati.preStart != nil {
			if err := ati.preStart.PreStart(ati.accessAddr); err != nil {
				return err
			}
		}
	}

	if restart {
		if ati.postRestart != nil {
			if err := ati.postRestart.PostRestart(ati.accessAddr); err != nil {
				ati.Stop()
				return err
			}
		}

		ati.events.Publish(ActorRestarted{
			Addr: ati.accessAddr,
			ID:   ati.id.String(),
		})
	} else {
		if ati.postStart != nil {
			if err := ati.postStart.PostStart(ati.accessAddr); err != nil {
				ati.Stop()
				return err
			}
		}

		ati.events.Publish(ActorStarted{
			ID:   ati.id.String(),
			Addr: ati.accessAddr,
		})
	}

	ati.started.On()

	return nil
}

func (ati *ActorImpl) initRoutines() {
	waitTillRunned(ati.manageLifeCycle)
	waitTillRunned(ati.readMessages)
}

func (ati *ActorImpl) exhaustMessages() {
	for !ati.mails.IsEmpty() {
		if nextAddr, next, err := ati.mails.Pop(); err == nil {
			ati.messages.Done()
			deadLetters.Publish(DeadMail{To: nextAddr, Message: next})
		}
	}
}

func (ati *ActorImpl) preDestroySystem() {
	ati.destruction.On()
	ati.events.Publish(ActorDestroyRequested{
		Addr: ati.accessAddr,
		ID:   ati.id.String(),
	})

	if ati.preDestroy != nil {
		ati.preDestroy.PreDestroy(ati.accessAddr)
	}
}

func (ati *ActorImpl) preMidDestroySystem() {
	// clean out all subscription first.
	for _, sub := range ati.subs {
		sub.Stop()
	}

	ati.subs = map[Actor]Subscription{}
}

func (ati *ActorImpl) postDestroySystem() {
	if ati.postDestroy != nil {
		ati.postDestroy.PostDestroy(ati.accessAddr)
	}

	ati.events.Publish(ActorDestroyed{
		Addr: ati.accessAddr,
		ID:   ati.id.String(),
	})
	ati.destruction.Off()
}

func (ati *ActorImpl) preKillSystem() {
	ati.events.Publish(ActorKillRequested{
		ID:   ati.id.String(),
		Addr: ati.accessAddr,
	})
}

func (ati *ActorImpl) postKillSystem() {
	ati.events.Publish(ActorKilled{
		ID:   ati.id.String(),
		Addr: ati.accessAddr,
	})
}

func (ati *ActorImpl) preStopSystem() {
	ati.events.Publish(ActorStopRequested{
		ID:   ati.id.String(),
		Addr: ati.accessAddr,
	})

	if ati.preStop != nil {
		ati.preStop.PreStop(ati.accessAddr)
	}
}

func (ati *ActorImpl) postStopSystem() {
	if ati.postStop != nil {
		ati.postStop.PostStop(ati.accessAddr)
	}

	ati.events.Publish(ActorStopped{
		ID:   ati.id.String(),
		Addr: ati.accessAddr,
	})

	ati.started.Off()
}

func (ati *ActorImpl) addSentinelWatch(addr Addr) {
	sub := addr.Watch(func(ev interface{}) {
		switch tm := ev.(type) {
		case ActorDestroyed:
			ati.sentinel.Advice(addr, tm)
		case ActorRestarted:
			ati.sentinel.Advice(addr, tm)
		case ActorStopped:
			ati.sentinel.Advice(addr, tm)
		case ActorKilled:
			ati.sentinel.Advice(addr, tm)
		default:
			return
		}
	})

	ati.sentinelSubs[addr] = sub
}

func (ati *ActorImpl) stopSentinelSubscriptions() {
	for _, sub := range ati.sentinelSubs {
		sub.Stop()
	}
}

func (ati *ActorImpl) awaitMessageExhaustion() {
	ati.processable.Off()
	ati.messages.Wait()
}

func (ati *ActorImpl) stopMessageReception() {
	ati.processable.Off()
	ati.signal <- struct{}{}
	ati.mails.Signal()

	// TODO(influx6): Noticed a rare bug where the initial call to
	// ati.mails.Signal() fails to signal end of giving mail check routine.
	// hence am adding this into this as a temporary fix.
	// Please remove once we figure how to fix this scheduling issue.
	tm := time.AfterFunc(1*time.Second, func() {
		ati.mails.Signal()
	})

	ati.routines.Wait()
	tm.Stop()
}

func (ati *ActorImpl) startChildrenSystems() {
	ati.tree.Each(func(actor Actor) bool {
		if err := actor.Start(); err != nil {
			ati.Escalate(err, AccessOf(actor))
			return true
		}

		ati.registerChild(actor)
		return true
	})
}

func (ati *ActorImpl) stopChildrenSystems() {
	ati.tree.Each(func(actor Actor) bool {
		// TODO: handle the error returned here or log it out.
		linearDoUntil(actor.Stop, 100, time.Second)
		return true
	})
}

func (ati *ActorImpl) killChildrenSystems() {
	ati.tree.Each(func(actor Actor) bool {
		// TODO: handle the error returned here or log it out.
		linearDoUntil(actor.Kill, 100, time.Second)
		return true
	})
}

func (ati *ActorImpl) destroyChildrenSystems() {
	ati.tree.Each(func(actor Actor) bool {
		waitTillRunned(func() {
			ati.unregisterChild(actor)
		})

		// TODO: handle the error returned here or log it out.
		linearDoUntil(actor.Destroy, 100, time.Second)
		return true
	})
}

func (ati *ActorImpl) exhaustSignals() {
	ati.exhaustSignalChan(ati.stopChan)
	ati.exhaustSignalChan(ati.killChan)
	ati.exhaustSentinel(ati.sentinelChan)
	ati.exhaustSignalChan(ati.destroyChan)
	ati.exhaustSignalChan(ati.stopChildrenChan)
	ati.exhaustSignalChan(ati.killChildrenChan)
	ati.exhaustSignalChan(ati.destroyChildrenChan)
}

func (ati *ActorImpl) exhaustSentinel(signal chan Addr) {
	if len(signal) == 0 {
		return
	}
	<-signal
}

func (ati *ActorImpl) exhaustSignalChan(signal chan chan error) {
	if len(signal) == 0 {
		return
	}
	res := <-signal
	res <- nil
}

func (ati *ActorImpl) registerChild(ac Actor) {
	sub := ac.Watch(func(event interface{}) {
		switch event.(type) {
		case ActorDestroyed:
			if ati.destruction.IsOn() {
				return
			}

			ati.rmActor <- ac
		}
	})

	// add actor subscription into map.
	ati.subs[ac] = sub
}

func (ati *ActorImpl) unregisterChild(ac Actor) {
	if sub, ok := ati.subs[ac]; ok {
		delete(ati.subs, ac)
		sub.Stop()
	}

	// Remove actor from tree and subscription
	ati.tree.RemoveActor(ac)
}

// manageChild handles addition of new actor into actor tree.
func (ati *ActorImpl) manageChild(an Actor) error {
	if err := an.Start(); err != nil {
		return err
	}

	// add new actor into tree.
	ati.tree.AddActor(an)

	// send actor for registration
	ati.addActor <- an
	return nil
}

func (ati *ActorImpl) manageLifeCycle() {
	defer func() {
		ati.proc.Done()
		ati.exhaustSignals()
	}()

	deadlockTicker := time.NewTicker(ati.deadLockDur)

	for {
		select {
		case <-deadlockTicker.C:
			// do nothing but also allow us avoid
			// possible all goroutine sleep bug.
		case addr := <-ati.sentinelChan:
			ati.addSentinelWatch(addr)
		case actor := <-ati.addActor:
			ati.registerChild(actor)
		case actor := <-ati.rmActor:
			ati.unregisterChild(actor)
		case res := <-ati.stopChan:
			atomic.AddInt64(&ati.stoppedCount, 1)
			ati.preStopSystem()
			ati.awaitMessageExhaustion()
			ati.stopMessageReception()
			ati.stopChildrenSystems()
			ati.postStopSystem()
			res <- nil
			return
		case res := <-ati.stopChildrenChan:
			ati.stopChildrenSystems()
			res <- nil
		case res := <-ati.killChan:
			atomic.AddInt64(&ati.killedCount, 1)
			ati.preKillSystem()
			ati.preStopSystem()
			ati.stopSentinelSubscriptions()
			ati.stopMessageReception()
			ati.exhaustMessages()
			ati.killChildrenSystems()
			ati.postStopSystem()
			ati.postKillSystem()
			res <- nil
			return
		case res := <-ati.killChildrenChan:
			ati.killChildrenSystems()
			res <- nil
		case res := <-ati.destroyChan:
			ati.death = time.Now()
			ati.preDestroySystem()
			ati.preMidDestroySystem()
			ati.stopSentinelSubscriptions()
			ati.preStopSystem()
			ati.stopMessageReception()
			ati.exhaustMessages()
			ati.destroyChildrenSystems()
			ati.postStopSystem()
			ati.postDestroySystem()
			ati.events.Reset()
			res <- nil
			return
		case res := <-ati.destroyChildrenChan:
			ati.preMidDestroySystem()
			ati.destroyChildrenSystems()
			res <- nil
		}
	}
}

// readMessages runs the process for executing messages.
func (ati *ActorImpl) readMessages() {
	defer func() {
		ati.routines.Done()

		if err := recover(); err != nil {

			trace := make([]byte, stackSize)
			coll := runtime.Stack(trace, false)
			trace = trace[:coll]

			event := ActorRoutinePanic{
				Stack: trace,
				Panic: err,
				Addr:  ati.accessAddr,
				ID:    ati.id.String(),
			}

			ati.events.Publish(event)

			ati.Escalate(event, ati.accessAddr)
		}
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
// ActorTree is safe for concurrent access.
type ActorTree struct {
	ml        sync.RWMutex
	children  []Actor
	registry  map[string]int
	addresses map[string]int
}

// NewActorTree returns a new instance of an actor tree using the initial length
// as capacity to the underline slice for storing actors.
func NewActorTree(initialLength int) *ActorTree {
	return &ActorTree{
		registry:  map[string]int{},
		addresses: map[string]int{},
		children:  make([]Actor, 0, initialLength),
	}
}

// Reset resets content of actor tree, removing all children and registry.
func (at *ActorTree) Reset() {
	at.ml.Lock()
	at.children = nil
	at.registry = map[string]int{}
	at.addresses = map[string]int{}
	at.ml.Unlock()
}

// Length returns the length of actors within tree.
func (at *ActorTree) Length() int {
	at.ml.RLock()
	m := len(at.children)
	at.ml.RUnlock()
	return m
}

// Each will call giving function on all registered actors,
// it concurrency safe and uses locks underneath. The handler is
// expected to return true/false, this indicates if we want to
// continue iterating in the case of true or to stop iterating in
// the case of false.			res <- nil
func (at *ActorTree) Each(fn func(Actor) bool) {
	at.ml.RLock()
	defer at.ml.RUnlock()

	// If handler returns true, then continue else stop.
	for _, child := range at.children {
		if !fn(child) {
			return
		}
	}
}

// HasActor returns true/false if giving actor exists.
func (at *ActorTree) HasActor(id string) bool {
	at.ml.RLock()
	defer at.ml.RUnlock()

	if _, ok := at.registry[id]; ok {
		return true
	}
	return false
}

// GetActorByAddr returns giving actor from tree using requested id.
func (at *ActorTree) GetActorByAddr(addr string) (Actor, error) {
	at.ml.RLock()
	defer at.ml.RUnlock()

	if index, ok := at.addresses[addr]; ok {
		return at.children[index], nil
	}

	return nil, errors.New("Actor %q not found", addr)
}

// GetActor returns giving actor from tree using requested id.
func (at *ActorTree) GetActor(id string) (Actor, error) {
	at.ml.RLock()
	defer at.ml.RUnlock()

	if index, ok := at.registry[id]; ok {
		return at.children[index], nil
	}

	return nil, errors.New("Actor %q not found", id)
}

// RemoveActor removes attached actor from tree if found.
func (at *ActorTree) RemoveActor(c Actor) {
	at.ml.Lock()
	defer at.ml.Unlock()

	index := at.registry[c.ID()]
	total := len(at.children)

	item := at.children[total-1]
	if total == 1 {
		delete(at.registry, c.ID())
		at.children = nil
		return
	}

	delete(at.registry, c.ID())
	delete(at.addresses, c.Addr())
	at.children[index] = item
	at.registry[item.ID()] = index
	at.addresses[item.Addr()] = index
	at.children = at.children[:total-1]
}

// AddActor adds giving actor into tree.
func (at *ActorTree) AddActor(c Actor) {
	at.ml.Lock()
	defer at.ml.Unlock()

	if _, ok := at.registry[c.ID()]; ok {
		return
	}

	currentIndex := len(at.children)
	at.children = append(at.children, c)
	at.registry[c.ID()] = currentIndex
	at.addresses[c.Addr()] = currentIndex
}
