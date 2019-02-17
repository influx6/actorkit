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

	// ErrActorHasNoOp is returned when an is to start with no attached behaviour.
	ErrActorHasNoOp = errors.New("Actor must be running")

	// ErrActorHasNoDiscoveryService is returned when actor has no discovery server.
	ErrActorHasNoDiscoveryService = errors.New("Actor does not support discovery")
)

//********************************************************
// signal dummy
//********************************************************

var _ Signals = &signalDummy{}

type signalDummy struct{}

func (signalDummy) SignalState(_ Addr, _ Signal) {}

//********************************************************
// Actors
//********************************************************

// ActorFunc defines a function which taking a provided Prop object
// returns a new Actor.
type ActorFunc func(Prop) Actor

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
func Ancestor(protocol string, namespace string, prop Prop) (Addr, error) {
	if prop.Op == nil {
		prop.Op = &DeadLetterOp{}
	}

	actor := NewActorImpl(namespace, protocol, prop)
	return AccessOf(actor), actor.Start()
}

//********************************************************
// ActorImpl
//********************************************************

var _ Actor = &ActorImpl{}

type actorSub struct {
	Actor Actor
	Sub   Subscription
}

// BusyDuration defines the acceptable wait time for an actor
// to allow for calls to giving state functions like Restart, Stop
// Kill, Destroy before timeout, as an actor could be busy handling a
// different state transition request.
//BusyDuration time.Duration

// DeadLockDuration defines the custom duration to be used for the deadlock
// go-routine asleep issue, which may occur if all actor goroutines become idle.
// The default used is 5s, but if it brings performance costs, then this can be
// increased or decreased as desired for optimum performance.
//DeadLockDuration time.Duration

// ActorImpl implements the Actor interface.
type ActorImpl struct {
	accessAddr  Addr
	props       Prop
	parent      Actor
	id          xid.ID
	namespace   string
	protocol    string
	state       uint32
	wstate      uint32
	logger      Logs
	tree        *ActorTree
	deadLockDur time.Duration
	busyDur     time.Duration

	death               time.Time
	created             time.Time
	failedDeliveryCount int64
	failedRestartCount  int64
	restartedCount      int64
	deliveryCount       int64
	processedCount      int64
	stoppedCount        int64
	killedCount         int64
	registered          int64

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

	proc     sync.WaitGroup
	messages sync.WaitGroup
	routines sync.WaitGroup

	preStop     PreStop
	postStop    PostStop
	preRestart  PreRestart
	postRestart PostRestart
	preStart    PreStart
	postStart   PostStart
	preDestroy  PreDestroy
	postDestroy PostDestroy

	gsub         *es.Subscription
	sentinelSubs map[Addr]Subscription
	subs         map[Actor]Subscription
}

// NewActorImpl returns a new instance of an ActorImpl assigned giving protocol and service name.
func NewActorImpl(namespace string, protocol string, props Prop) *ActorImpl {
	if namespace == "" {
		namespace = "localhost"
	}

	if protocol == "" {
		protocol = "kit"
	}

	if props.Op == nil {
		panic("Can have nil Op")
	}

	ac := &ActorImpl{}
	ac.protocol = protocol
	ac.logger = &DrainLog{}
	ac.namespace = namespace
	ac.state = uint32(INACTIVE)
	ac.deadLockDur = defaultDeadLockTicker
	ac.busyDur = defaultWaitDuration

	// add event provider.
	if props.Event == nil {
		props.Event = NewEventer()
	}

	if props.DeadLetters == nil {
		props.DeadLetters = eventDeathMails
	}

	if props.Signals == nil {
		props.Signals = &signalDummy{}
	}

	if tm, ok := props.Op.(PreStart); ok {
		ac.preStart = tm
	} else {
		ac.preStart = nil
	}

	if tm, ok := props.Op.(PostStart); ok {
		ac.postStart = tm
	} else {
		ac.postStart = nil
	}

	if tm, ok := props.Op.(PreDestroy); ok {
		ac.preDestroy = tm
	} else {
		ac.preDestroy = nil
	}

	if tm, ok := props.Op.(PostDestroy); ok {
		ac.postDestroy = tm
	} else {
		ac.postDestroy = nil
	}

	if tm, ok := props.Op.(PreRestart); ok {
		ac.preRestart = tm
	} else {
		ac.preRestart = nil
	}

	if tm, ok := props.Op.(PostRestart); ok {
		ac.postRestart = tm
	} else {
		ac.postRestart = nil
	}

	if tm, ok := props.Op.(PreStop); ok {
		ac.preStop = tm
	} else {
		ac.preStop = nil
	}

	if tm, ok := props.Op.(PostStop); ok {
		ac.postStop = tm
	} else {
		ac.postStop = nil
	}

	// add unbouned mailbox.
	if props.Mailbox == nil {
		props.Mailbox = UnboundedBoxQueue(props.MailInvoker)
	}

	// if we have no set provider then use a one-for-one strategy.
	if props.Supervisor == nil {
		props.Supervisor = &OneForOneSupervisor{
			Max: 30,
			Invoker: &EventSupervisingInvoker{
				Event: ac.props.Event,
			},
			Decider: func(tm interface{}) Directive {
				switch tm.(type) {
				case PanicEvent:
					return PanicDirective
				default:
					return RestartDirective
				}
			},
		}
	}

	ac.props = props
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
	ac.subs = map[Actor]Subscription{}
	ac.tree = NewActorTree(10)

	ac.processable.On()

	// if we have ContextLogs, get a logger for yourself.
	if props.ContextLogs != nil {
		ac.logger = props.ContextLogs.Get(ac)
	}

	return ac
}

// Wait implements the Waiter interface.
func (ati *ActorImpl) Wait() {
	ati.proc.Wait()
}

// WorkState returns the current work state of giving actor
// in a safe-concurrent manner.
func (ati *ActorImpl) WorkState() WorkSignal {
	return WorkSignal(atomic.LoadUint32(&ati.wstate))
}

func (ati *ActorImpl) setWorkState(ns WorkSignal) {
	atomic.StoreUint32(&ati.wstate, uint32(ns))
}

// State returns the current state of giving actor in a safe-concurrent
// manner.
func (ati *ActorImpl) State() Signal {
	return Signal(atomic.LoadUint32(&ati.state))
}

// setState sets the new state for the actor.
func (ati *ActorImpl) setState(ns Signal) {
	atomic.StoreUint32(&ati.state, uint32(ns))
}

func (ati *ActorImpl) resetState() {
	atomic.StoreUint32(&ati.state, 0)
}

// ID returns associated string version of id.
func (ati *ActorImpl) ID() string {
	return ati.id.String()
}

// Mailbox returns actors underline mailbox.
func (ati *ActorImpl) Mailbox() Mailbox {
	return ati.props.Mailbox
}

// GetAddr returns the child of this actor which has this address string version.
//
// This method is more specific and will not respect or handle a address which
// the root ID is not this actor's identification ID. It heavily relies on walking
// the address tree till it finds the target actor or there is found no matching actor
//
func (ati *ActorImpl) GetAddr(addr string) (Addr, error) {

	return nil, nil
}

// GetChild returns the child of this actor which has this matching id.
//
// If the sub is provided, then the function will drill down the provided
// target actor getting the child actor of that actor which matches the
// next string ID till it finds the last target string ID or fails to
// find it.
func (ati *ActorImpl) GetChild(id string, subID ...string) (Addr, error) {
	parent, err := ati.tree.GetActor(id)
	if err != nil {
		return nil, err
	}

	if len(subID) == 0 {
		return AccessOf(parent), nil
	}

	return parent.GetChild(subID[0], subID[1:]...)
}

// Watch adds provided function as a subscriber to be called
// on events published by actor, it returns a subscription which
// can be used to end giving subscription.
func (ati *ActorImpl) Watch(fn func(interface{})) Subscription {
	return ati.props.Event.Subscribe(fn, nil)
}

// DeathWatch asks this actor sentinel to advice on behaviour or operation to
// be performed for the provided actor's states (i.e Stopped, Restarted, Killed, Destroyed).
// being watched for.
//
// If actor has no Sentinel then an error is returned.
// Sentinels are required to advice on action for watched actors by watching actor.
func (ati *ActorImpl) DeathWatch(addr Addr) error {
	if ati.props.Sentinel == nil {
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
	ati.props.Event.Publish(message)
}

// Receive adds giving Envelope into actor's mailbox.
func (ati *ActorImpl) Receive(a Addr, e Envelope) error {
	// if we cant process, then return error.
	if !ati.processable.IsOn() {
		return errors.New("actor is stopped and hence can't handle message")
	}

	if ati.props.MessageInvoker != nil {
		ati.props.MessageInvoker.InvokedRequest(a, e)
	}

	ati.messages.Add(1)

	return ati.props.Mailbox.Push(a, e)
}

// Discover returns actor's Addr from this actor's
// discovery chain, else passing up the service till it
// reaches the actors root where no possible discovery can be done.
//
// The returned address is not added to this actor's death watch, so user
// if desiring this must add it themselves.
//
// The method will return an error if Actor is not already running.
func (ati *ActorImpl) Discover(service string, ancestral bool) (Addr, error) {
	if !ati.started.IsOn() && !ati.starting.IsOn() {
		return nil, errors.WrapOnly(ErrActorMustBeRunning)
	}

	if ati.parent != nil && ati.props.Discovery == nil && ancestral {
		return ati.parent.Discover(service, ancestral)
	}

	addr, err := ati.props.Discovery.Discover(service)
	if err != nil {
		if ati.parent != nil && ancestral {
			return ati.parent.Discover(service, ancestral)
		}
		return nil, errors.WrapOnly(err)
	}
	return addr, nil
}

// Spawn spawns a new actor under this parents tree returning address of
// created actor.
//
// The method will return an error if Actor is not already running.
func (ati *ActorImpl) Spawn(service string, prop Prop) (Addr, error) {
	if !ati.started.IsOn() && !ati.starting.IsOn() {
		return nil, errors.WrapOnly(ErrActorMustBeRunning)
	}

	if prop.ContextLogs == nil {
		prop.ContextLogs = ati.props.ContextLogs
	}

	if ati.props.Signals != nil {
		prop.Signals = ati.props.Signals
	}

	if prop.MessageInvoker == nil {
		prop.MessageInvoker = ati.props.MessageInvoker
	}

	if prop.Sentinel == nil {
		prop.Sentinel = ati.props.Sentinel
	}

	if prop.StateInvoker == nil {
		prop.StateInvoker = ati.props.StateInvoker
	}

	if prop.MailInvoker == nil {
		prop.MailInvoker = ati.props.MailInvoker
	}

	if prop.DeadLetters == nil {
		prop.DeadLetters = ati.props.DeadLetters
	}

	am := NewActorImpl(ati.namespace, ati.protocol, prop)
	am.parent = ati

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
//		Protocol@Namespace/ID
//
// 2. If Actor is the a child of another, then it will return address in form:
//
//		AncestorAddress/ID
//
//    where AncestorAddress is "Protocol@Namespace/ID"
//
// In either case, the protocol of both ancestor and parent is maintained.
// Namespace provides a field area which can define specific information that
// is specific to giving protocol e.g ip v4/v6 address.
//
func (ati *ActorImpl) Addr() string {
	if ati.parent == nil {
		return FormatAddr(ati.protocol, ati.namespace, ati.id.String())
	}
	return FormatAddrChild(ati.parent.Addr(), ati.id.String())
}

// ProtocolAddr returns the Actors.Protocol and Actors.Namespace
// values in the format:
//
//  Protocol@Namespace.
//
func (ati *ActorImpl) ProtocolAddr() string {
	return FormatNamespace(ati.protocol, ati.namespace)
}

// Protocol returns actor's protocol.
func (ati *ActorImpl) Protocol() string {
	return ati.protocol
}

// Namespace returns actor's namespace.
func (ati *ActorImpl) Namespace() string {
	return ati.namespace
}

// Escalate sends giving error that occur to actor's supervisor
// which can make necessary decision on actions to be done, either
// to escalate to parent's supervisor or restart/stop or handle
// giving actor as dictated by it's algorithm.
func (ati *ActorImpl) Escalate(err interface{}, addr Addr) {
	go ati.props.Supervisor.Handle(err, addr, ati, ati.parent)
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

	ati.logger.Emit(DEBUG, OpMessage{Detail: "Sending destruction signal"})

	res := make(chan error, 1)
	select {
	case ati.destroyChildrenChan <- res:
		ati.logger.Emit(DEBUG, OpMessage{Detail: "Awaiting destruction response"})
		return <-res
	case <-time.After(ati.busyDur):
		ati.logger.Emit(ERROR, OpMessage{Detail: "Failed to deliver destruction signal", Data: ErrActorBusyState})
		res <- errors.WrapOnly(ErrActorBusyState)
	}
	return <-res
}

//****************************************************************
// internal functions
//****************************************************************

func (ati *ActorImpl) runSystem(restart bool) error {
	ati.setState(STARTING)
	defer ati.setState(RUNNING)

	ati.starting.On()
	defer ati.starting.Off()

	ati.proc.Add(1)
	ati.routines.Add(1)

	ati.initRoutines()
	ati.processable.On()

	if restart {
		atomic.AddInt64(&ati.restartedCount, 1)

		ati.props.Event.Publish(ActorSignal{
			Signal: RESTARTING,
			Addr:   ati.accessAddr,
		})

		ati.props.Signals.SignalState(ati.accessAddr, RESTARTING)

		if ati.preRestart != nil {
			if err := ati.preRestart.PreRestart(ati.accessAddr); err != nil {
				atomic.AddInt64(&ati.failedRestartCount, 1)
				return err
			}
		}
	} else {
		ati.props.Event.Publish(ActorSignal{
			Signal: STARTING,
			Addr:   ati.accessAddr,
		})

		ati.props.Signals.SignalState(ati.accessAddr, STARTING)

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

		ati.props.Signals.SignalState(ati.accessAddr, RESTARTED)

		ati.props.Event.Publish(ActorSignal{
			Addr:   ati.accessAddr,
			Signal: RESTARTED,
		})
	} else {
		if ati.postStart != nil {
			if err := ati.postStart.PostStart(ati.accessAddr); err != nil {
				ati.Stop()
				return err
			}
		}

		ati.props.Signals.SignalState(ati.accessAddr, RUNNING)

		ati.props.Event.Publish(ActorSignal{
			Addr:   ati.accessAddr,
			Signal: RUNNING,
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
	for !ati.props.Mailbox.IsEmpty() {
		if nextAddr, next, err := ati.props.Mailbox.Pop(); err == nil {
			ati.messages.Done()

			dm := DeadMail{To: nextAddr, Message: next}
			ati.props.DeadLetters.RecoverMail(dm)
		}
	}
}

func (ati *ActorImpl) preDestroySystem() {
	ati.destruction.On()
	ati.setState(DESTRUCTING)
	ati.props.Signals.SignalState(ati.accessAddr, DESTRUCTING)
	ati.props.Event.Publish(ActorSignal{
		Addr:   ati.accessAddr,
		Signal: DESTRUCTING,
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

	if ati.gsub != nil {
		ati.gsub.Stop()
	}

	ati.subs = map[Actor]Subscription{}
}

func (ati *ActorImpl) postDestroySystem() {

	if ati.postDestroy != nil {
		ati.postDestroy.PostDestroy(ati.accessAddr)
	}

	ati.props.Event.Publish(ActorSignal{
		Addr:   ati.accessAddr,
		Signal: DESTROYED,
	})

	ati.props.Signals.SignalState(ati.accessAddr, DESTROYED)
	ati.destruction.Off()
	ati.setState(DESTROYED)
}

func (ati *ActorImpl) preKillSystem() {
	ati.setState(KILLING)
	ati.props.Signals.SignalState(ati.accessAddr, KILLING)
	ati.props.Event.Publish(ActorSignal{
		Signal: KILLING,
		Addr:   ati.accessAddr,
	})
}

func (ati *ActorImpl) postKillSystem() {
	ati.props.Signals.SignalState(ati.accessAddr, KILLED)
	ati.props.Event.Publish(ActorSignal{
		Signal: KILLED,
		Addr:   ati.accessAddr,
	})
	ati.setState(KILLED)
}

func (ati *ActorImpl) preStopSystem() {
	ati.setState(STOPPING)
	ati.props.Signals.SignalState(ati.accessAddr, STOPPING)
	ati.props.Event.Publish(ActorSignal{
		Signal: STOPPING,
		Addr:   ati.accessAddr,
	})

	if ati.preStop != nil {
		ati.preStop.PreStop(ati.accessAddr)
	}
}

func (ati *ActorImpl) postStopSystem() {
	ati.props.Signals.SignalState(ati.accessAddr, STOPPED)
	if ati.postStop != nil {
		ati.postStop.PostStop(ati.accessAddr)
	}

	ati.props.Event.Publish(ActorSignal{
		Signal: STOPPED,
		Addr:   ati.accessAddr,
	})

	ati.started.Off()
	ati.setState(STOPPED)
}

func (ati *ActorImpl) addSentinelWatch(addr Addr) {
	sub := addr.Watch(func(ev interface{}) {
		switch tm := ev.(type) {
		case ActorSignal:
			ati.props.Sentinel.Advice(addr, tm)
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
	ati.props.Mailbox.Signal()

	// TODO(influx6): Noticed a rare bug where the initial call to
	// ati.mails.Signal() fails to signal end of giving mail check routine.
	// hence am adding this into this as a temporary fix.
	// Please remove once we figure how to fix this scheduling issue.
	tm := time.AfterFunc(1*time.Second, func() {
		ati.props.Mailbox.Signal()
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
		switch tm := event.(type) {
		case ActorSignal:
			if tm.Signal != DESTROYED {
				return
			}

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
			ati.logger.Emit(DEBUG, Message("Incurring deadlock safety skip"))
		case addr := <-ati.sentinelChan:
			ati.logger.Emit(DEBUG, Message("Initiating sentinel watch for addr"))
			ati.addSentinelWatch(addr)
			ati.logger.Emit(DEBUG, Message("Done adding sentinel watch"))
		case actor := <-ati.addActor:
			ati.logger.Emit(DEBUG, Message("Initiating register for actor child"))
			ati.registerChild(actor)
			ati.logger.Emit(DEBUG, Message("Done registering"))
		case actor := <-ati.rmActor:
			ati.logger.Emit(DEBUG, Message("Initiating unregister for actor child"))
			ati.unregisterChild(actor)
			ati.logger.Emit(DEBUG, Message("Done unregistering"))
		case res := <-ati.stopChan:
			ati.logger.Emit(DEBUG, Message("Initiating stop procedure for actor"))
			atomic.AddInt64(&ati.stoppedCount, 1)
			ati.logger.Emit(DEBUG, Message("Running pre-stop procedures"))
			ati.preStopSystem()
			ati.logger.Emit(DEBUG, Message("Awaiting pending messages total processing by actor"))
			ati.awaitMessageExhaustion()
			ati.logger.Emit(DEBUG, Message("Stopping message reception"))
			ati.stopMessageReception()
			ati.logger.Emit(DEBUG, Message("Requesting stop of actor's children"))
			ati.stopChildrenSystems()
			ati.logger.Emit(DEBUG, Message("Running post-stop procedures"))
			ati.postStopSystem()
			ati.logger.Emit(DEBUG, Message("Sending finished signal"))
			res <- nil
			ati.logger.Emit(DEBUG, Message("Done stopping"))
			return
		case res := <-ati.stopChildrenChan:
			ati.logger.Emit(DEBUG, Message("Initiating stop procedure for actor's children"))
			ati.stopChildrenSystems()
			ati.logger.Emit(DEBUG, Message("Sending finished signal"))
			res <- nil
			ati.logger.Emit(DEBUG, Message("Done stopping children"))
		case res := <-ati.killChan:
			ati.logger.Emit(DEBUG, Message("Initiating kill procedure of actor"))
			atomic.AddInt64(&ati.killedCount, 1)
			ati.logger.Emit(DEBUG, Message("Running pre-kill procedure"))
			ati.preKillSystem()
			ati.logger.Emit(DEBUG, Message("Running pre-stop procedure"))
			ati.preStopSystem()
			ati.logger.Emit(DEBUG, Message("Stopping sentinel subscriptions"))
			ati.stopSentinelSubscriptions()
			ati.logger.Emit(DEBUG, Message("Stopping message reception"))
			ati.stopMessageReception()
			ati.logger.Emit(DEBUG, Message("Exhausting pending messages to death mailbox"))
			ati.exhaustMessages()
			ati.logger.Emit(DEBUG, Message("Requesting kill of actor's children"))
			ati.killChildrenSystems()
			ati.logger.Emit(DEBUG, Message("Running post-stop procedure"))
			ati.postStopSystem()
			ati.logger.Emit(DEBUG, Message("Running post-kill procedure"))
			ati.postKillSystem()
			ati.logger.Emit(DEBUG, Message("Sending finished signal"))
			res <- nil
			ati.logger.Emit(DEBUG, Message("Done killing"))
			return
		case res := <-ati.killChildrenChan:
			ati.logger.Emit(DEBUG, Message("Requesting kill procedure for actor's children"))
			ati.killChildrenSystems()
			ati.logger.Emit(DEBUG, Message("Sending finished signal"))
			res <- nil
			ati.logger.Emit(DEBUG, Message("Done killing children"))
		case res := <-ati.destroyChan:
			ati.logger.Emit(DEBUG, Message("Initiating destruction of actor"))
			ati.death = time.Now()
			ati.logger.Emit(DEBUG, Message("Running pre-destruction procedure"))
			ati.preDestroySystem()
			ati.logger.Emit(DEBUG, Message("Running pre-mid-destruction procedure"))
			ati.preMidDestroySystem()
			ati.logger.Emit(DEBUG, Message("Stopping sentinel subscriptions"))
			ati.stopSentinelSubscriptions()
			ati.logger.Emit(DEBUG, Message("Running pre-stop procedure"))
			ati.preStopSystem()
			ati.logger.Emit(DEBUG, Message("Stopping message reception"))
			ati.stopMessageReception()
			ati.logger.Emit(DEBUG, Message("Exhausting pending messages to death mailbox"))
			ati.exhaustMessages()
			ati.logger.Emit(DEBUG, Message("Requesting destruction of actor's's children"))
			ati.destroyChildrenSystems()
			ati.logger.Emit(DEBUG, Message("Running post-stop procedure"))
			ati.postStopSystem()
			ati.logger.Emit(DEBUG, Message("Running post-destroy procedure"))
			ati.postDestroySystem()
			ati.logger.Emit(DEBUG, Message("Resetting event subscription queue"))
			ati.props.Event.Reset()
			ati.logger.Emit(DEBUG, Message("Sending finished signal"))
			res <- nil
			ati.logger.Emit(DEBUG, Message("Done destructing"))
			return
		case res := <-ati.destroyChildrenChan:
			ati.logger.Emit(DEBUG, Message("Running mid-destruction procedure"))
			ati.preMidDestroySystem()
			ati.logger.Emit(DEBUG, Message("Requesting destruction of actor children"))
			ati.destroyChildrenSystems()
			ati.logger.Emit(DEBUG, Message("Sending finished signal"))
			res <- nil
			ati.logger.Emit(DEBUG, Message("Done destructing"))
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

			event := PanicEvent{
				Stack: trace,
				Panic: err,
				Addr:  ati.accessAddr,
				ID:    ati.id.String(),
			}

			ati.props.Event.Publish(event)
			ati.Escalate(event, ati.accessAddr)
		}
	}()

	//ticker := time.NewTicker(1 * time.Second)

	for {
		// block until we have a message, internally
		// we will be put to sleep till there is a message.
		ati.props.Mailbox.Wait()

		// Make a check after signal of new message, if we
		// are required to shutdown.
		select {
		case <-ati.signal:
			ati.logger.Emit(DEBUG, Message("Received message stop signal"))
			return
		default:
			ati.logger.Emit(DEBUG, Message("Reading new message from mailbox"))
		}

		addr, msg, err := ati.props.Mailbox.Pop()
		if err != nil {
			continue
		}

		// Execute receiver behaviour with panic guard.
		ati.process(addr, msg)
	}
}

func (ati *ActorImpl) process(a Addr, x Envelope) {
	ati.setWorkState(BUSY)
	defer ati.setWorkState(FREE)

	// decrease message wait counter.
	ati.messages.Done()

	if ati.props.MessageInvoker != nil {
		ati.props.MessageInvoker.InvokedProcessing(a, x)
	}

	ati.props.Op.Action(a, x)

	if ati.props.MessageInvoker != nil {
		ati.props.MessageInvoker.InvokedProcessed(a, x)
	}
}

//********************************************************
// Actor Tree
//********************************************************

// ActorTree implements a hash/dictionary registry for giving actors.
// It combines internally a map and list to take advantage of quick lookup
// and order maintenance.
//
// ActorTree implements the ActorRegistry interface.
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

// GetAddr returns a actor from tree using requested id.
func (at *ActorTree) GetAddr(addr string) (Actor, error) {
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
func (at *ActorTree) RemoveActor(c Actor) error {
	at.ml.Lock()
	defer at.ml.Unlock()

	index := at.registry[c.ID()]
	total := len(at.children)

	item := at.children[total-1]
	if total == 1 {
		delete(at.registry, c.ID())
		at.children = nil
		return nil
	}

	delete(at.registry, c.ID())
	delete(at.addresses, c.Addr())
	at.children[index] = item
	at.registry[item.ID()] = index
	at.addresses[item.Addr()] = index
	at.children = at.children[:total-1]
	return nil
}

// AddActor adds giving actor into tree.
func (at *ActorTree) AddActor(c Actor) error {
	at.ml.Lock()
	defer at.ml.Unlock()

	if _, ok := at.registry[c.ID()]; ok {
		return nil
	}

	currentIndex := len(at.children)
	at.children = append(at.children, c)
	at.registry[c.ID()] = currentIndex
	at.addresses[c.Addr()] = currentIndex
	return nil
}
