package actorkit

import (
	"sync"

	"github.com/rs/xid"

	"github.com/gokit/es"

	"github.com/gokit/errors"
)

//********************************************************
// ActorImpl
//********************************************************

// ActorImpl implements the Actor interface.
type ActorImpl struct {
	id     xid.ID
	parent Actor
	mails  Mailbox
	tree   *ActorTree
	events *es.EventStream

	waiter      sync.WaitGroup
	closeSignal chan struct{}
}

// NewActorImpl returns a new instance of an ActorImpl.
func NewActorImpl(parent Actor, mail Mailbox) *ActorImpl {
	var ac ActorImpl
	ac.mails = mail
	ac.id = xid.New()
	ac.parent = parent
	ac.events = es.New()
	ac.tree = NewActorTree(10)
	ac.closeSignal = make(chan struct{}, 1)
	return &ac
}

func (ati *ActorImpl) Discover(service string) {

}

func (ati *ActorImpl) AddDiscovery(b Discovery) {

}

func (ati *ActorImpl) Watch(Addr) Subscription {
	panic("implement me")
}

func (ati *ActorImpl) Fn(func(interface{})) Subscription {
	panic("implement me")
}

// Receive adds giving Envelope into actor's mailbox.
func (ati *ActorImpl) Receive(Addr, Envelope) error {
	return nil
}

// ID returns associated string version of id.
func (ati *ActorImpl) ID() string {
	return ati.id.String()
}

func (ati *ActorImpl) Escalate(interface{}, Addr) Future {
	panic("implement me")
}

func (ati *ActorImpl) Stopped() bool {
	panic("implement me")
}

func (ati *ActorImpl) Kill() {
	panic("implement me")
}

func (ati *ActorImpl) Stop() Waiter {
	panic("implement me")
}

func (ati *ActorImpl) Addr() string {
	panic("implement me")
}

func (ati *ActorImpl) readMessages() {
	defer ati.waiter.Done()
	for {
		select {
		case <-ati.closeSignal:
			return
		default:
		}

		// check if we have pending message?
		if ati.mails.Empty() {
			continue
		}
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

//***********************************
//  Actor System Message
//***********************************

// TerminatedActor is sent when an Mask processor has already
// being shutdown/stopped.
type TerminatedActor struct {
	ID string
}

// ActorStarted is sent when an actor has begun it's operation.
type ActorStarted struct {
	ID string
}

// ActorStop is send when an actor is in the process of shutdown.
type ActorStop struct {
	ID string
}

// ActorStopped is send when an actor is in the process of shutdown.
type ActorStopped struct {
	ID   string
	Mail Mailbox
}

// ActorPanic is sent when an actor panics internally.
type ActorPanic struct {
	ID    string
	Mail  Mailbox
	Panic interface{}
}
