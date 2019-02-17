package actorkit

import (
	"log"
	"sync"
	"time"
)

//***********************************************************
// Route Messages
//***********************************************************

// AddRoute defines a giving message delivered
// for adding sending address into route list.
//
// Used by the RoundRobin, RandomRouter, HashedRouter and Broadcast Router.
type AddRoute struct{}

// RemoveRoute defines a giving message delivered for
// removing sending address from route list.
//
// Used by the RoundRobin, RandomRouter, HashedRouter and Broadcast Router.
type RemoveRoute struct{}

//***********************************************************
// SpawningRouter
//***********************************************************

// SpawnerFunc spawns a new actor with provided ModOps which will be
// used to build it's prop list.
type SpawnerFunc func(mods ...ModProp) Actor

// SpawnerConfig defines configuration which is used to power a underline
// spawner and how it build's it's underline children from provided spawned function.
type SpawnerConfig struct {
	// Max sets the maximum allowed children actors to be spawned for giving
	// actors.
	// ( Default: 20 ).
	Max int

	// Mods sets the default Mod functions for using to create a new Prop
	// for giving generated actor.
	Mods []ModProp

	// SpawnerFunc sets the function to be used for spawning new
	// actors.
	SpawnerFunc SpawnerFunc

	// MaxIdle sets the maximum duration which we must wait till the
	// actor can be considered idle and killable since no new work has
	// being received within giving duration.
	// ( Default: 10 seconds ).
	MaxIdle time.Duration
}

// SpawningRouter differs from other router which have their actors
// provided to them over a message, instead these are specific for a giving
// actor type and are provided a function which consistently spawns
// actors for handling messages based on specific configurable conditions.
type SpawningRouter struct {
	config  *SpawnerConfig
	spawner SpawnerFunc

	fallbackRobin *RoundRobinSet

	sml   sync.Mutex
	slots []*spawnTtlActor
}

// NewSpawningRouter returns a new SpawningRouter.
func NewSpawningRouter(config SpawnerConfig) *SpawningRouter {
	if config.SpawnerFunc == nil {
		panic("SpawnerConfig.SpawnerFunc must be supplied")
	}
	if config.MaxIdle <= 0 {
		config.MaxIdle = time.Second * 10
	}
	if config.Max <= 0 {
		config.Max = 20
	}

	var spawner SpawningRouter
	spawner.config = &config
	spawner.fallbackRobin = NewRoundRobinSet()
	spawner.slots = make([]*spawnTtlActor, 0, config.Max)
	return &spawner
}

// Action implements the Op interface.
// It handles the allocation of new pending work to available nodes
// within the maximum allowed actors. It will dish out incoming messages
// to non-busy workers when possible, else dish incoming work in a
// round robin fashion.
func (rr *SpawningRouter) Action(addr Addr, msg Envelope) {
	switch msg.Data.(type) {

	}
}

func (rr *SpawningRouter) remove(index int) {
	rr.sml.Lock()
	defer rr.sml.Unlock()
}

// spawnTtlActor implements a time limited actor which if idle for
// a giving duration will be killed.
type spawnTtlActor struct {
	index  int
	actor  Actor
	timer  *time.Timer
	owner  *SpawningRouter
	waiter sync.WaitGroup
}

// newSpawnTTLActor returns a new instance of a spawnTtlActor.
func newSpawnTTLActor(index int, actor Actor, owner *SpawningRouter) *spawnTtlActor {
	var ttl spawnTtlActor
	ttl.index = index
	ttl.actor = actor
	ttl.owner = owner
	ttl.timer = time.NewTimer(owner.config.MaxIdle)

	return &ttl
}

// Wait blocks till giving instance is closed.
func (ttl *spawnTtlActor) Wait() {
	ttl.waiter.Wait()
}

// Receive implements the Receiver interface.
func (ttl *spawnTtlActor) Receive(addr Addr, msg Envelope) {
	ttl.timer.Reset(ttl.owner.config.MaxIdle)
	ttl.actor.Receive(addr, msg)
}

func (ttl *spawnTtlActor) manageTTL() {
	ttl.waiter.Add(1)
	go func() {
		defer ttl.waiter.Done()

		for {
			select {
			case <-ttl.timer.C:
				// the maximum idle time out as being reach,
				ttl.actor.Destroy()

				// Remove this actor from spawner list.
				ttl.owner.remove(ttl.index)
			default:
				continue
			}
		}
	}()
}

//***********************************************************
// RoundRobinRouter
//***********************************************************

// RoundRobinRouter implements a router which delivers messages to giving address
// in a round robin manner. The router uses the Address.Addr() value to allow distinct
// addresses regardless if underline serving actor is the same to maintain address uniqueness
// and logic.
//
// It stores address by their Addr.Addr() which means even if two Addr are referencing
// same Actor, they will be respected, added and broadcasted to, as the Addr represents
// a unique capability.
type RoundRobinRouter struct {
	addrs *ServiceSet
	set   *RoundRobinSet
}

// NewRoundRobinRouter returns a new instance of a RoundRobinRouter using
// provided address list if any to setup.
func NewRoundRobinRouter(addrs ...Addr) *RoundRobinRouter {
	var service ServiceSet
	set := NewRoundRobinSet()

	for _, addr := range addrs {
		service.Add(addr)
		set.Add(addr.Addr())
	}

	return &RoundRobinRouter{
		set:   set,
		addrs: &service,
	}
}

// Action implements the Op interface.
func (rr *RoundRobinRouter) Action(addr Addr, msg Envelope) {
	switch msg.Data.(type) {
	case AddRoute:
		if msg.Sender != nil && msg.Sender.ID() != addr.ID() {
			if rr.addrs.RemoveAddr(msg.Sender) {
				rr.set.Remove(msg.Sender.Addr())
			}
		}
	case RemoveRoute:
		if msg.Sender != nil && msg.Sender.ID() != addr.ID() {
			if rr.addrs.Add(msg.Sender) {
				rr.set.Add(msg.Sender.Addr())
			}
		}
	default:
		targetAddr := rr.set.Get()
		if target, ok := rr.addrs.Get(targetAddr); ok {
			if err := target.Forward(msg); err != nil {
				log.Printf("[RoundRobin:Routing] Failed to route message %q to addr %q: %#v", msg.Ref.String(), addr.Addr(), err)
			}
			return
		}
		log.Printf("[RoundRobin:Routing] Failed get node for addr %q to route message %q", targetAddr, msg.Ref.String())
	}
}

//***********************************************************
// BroadcastRouter
//***********************************************************

// BroadcastRouter implements a router which delivers messages in a fan-out
// manner to all addresses.
//
// It stores address by their Addr.Addr() which means even if two Addr are referencing
// same Actor, they will be respected, added and broadcasted to, as the Addr represents
// a unique capability.
type BroadcastRouter struct {
	addrs *ServiceSet
}

// NewBroadcastRouter adds giving set of address, returning a new BroadcastRouter
// which will broadcast incoming messages to all addresses.
func NewBroadcastRouter(addrs ...Addr) *BroadcastRouter {
	var service ServiceSet
	for _, addr := range addrs {
		service.Add(addr)
	}

	return &BroadcastRouter{
		addrs: &service,
	}
}

// Action implements the Op interface.
func (rr *BroadcastRouter) Action(addr Addr, msg Envelope) {
	switch msg.Data.(type) {
	case AddRoute:
		if msg.Sender != nil && msg.Sender.ID() != addr.ID() {
			rr.addrs.Add(msg.Sender)
		}
	case RemoveRoute:
		if msg.Sender != nil && msg.Sender.ID() != addr.ID() {
			rr.addrs.RemoveAddr(msg.Sender)
		}
	default:
		rr.addrs.ForEach(func(addr Addr, i int) bool {
			if err := addr.Forward(msg); err != nil {
				log.Printf("[Broadcast:Routing] Failed to route message %q to addr %q: %#v", msg.Ref.String(), addr.Addr(), err)
			}
			return true
		})
	}
}

//***********************************************************
// RandomRouter
//***********************************************************

// RandomRouter implements a router which delivers messages to giving address
// based on one randomly chosen address from it's set of known addresses.
//
// It stores address by their Addr.Addr() which means even if two Addr are referencing
// same Actor, they will be respected, added and broadcasted to, as the Addr represents
// a unique capability.
type RandomRouter struct {
	rand  *RandomSet
	addrs *ServiceSet
}

// NewRandomRouter returns a new instance of a RandomRouter.
func NewRandomRouter(addrs ...Addr) *RandomRouter {
	var service ServiceSet
	set := NewRandomSet()

	for _, addr := range addrs {
		service.Add(addr)
		set.Add(addr.Addr())
	}

	return &RandomRouter{
		rand:  set,
		addrs: &service,
	}
}

// Action implements the Op interface.
func (rr *RandomRouter) Action(addr Addr, msg Envelope) {
	switch msg.Data.(type) {
	case AddRoute:
		if msg.Sender != nil && msg.Sender.ID() != addr.ID() {
			if rr.addrs.Add(msg.Sender) {
				rr.rand.Add(msg.Sender.Addr())
			}
		}
	case RemoveRoute:
		if msg.Sender != nil && msg.Sender.ID() != addr.ID() {
			if rr.addrs.RemoveAddr(msg.Sender) {
				rr.rand.Remove(msg.Sender.Addr())
			}
		}
	default:
		targetAddr := rr.rand.Get()
		if target, ok := rr.addrs.Get(targetAddr); ok {
			if err := target.Forward(msg); err != nil {
				log.Printf("[Random:Routing] Failed to route message %q to addr %q: %#v", msg.Ref.String(), addr.Addr(), err)
			}
			return
		}
		log.Printf("[Random:Routing] Failed get node for addr %q to route message %q", targetAddr, msg.Ref.String())
	}
}

//***********************************************************
// HashedRouter
//***********************************************************

// Hashed defines a interface where it's implementers must expose a method
// which returns a string hash used for routing purposes.
type Hashed interface {
	Hash() string
}

// HashingReference defines a function which is provided to the HashRouter
// which will return a string from a adderess. This allows custom values based
// of giving Addr to be returned as hashing input value.
type HashingReference func(Addr) string

// ProtocolAddrReference defines a function which matches the HashingReference function
// type. It simply returns the ProtocolAddr() value of a Addr object.
func ProtocolAddrReference(addr Addr) string {
	return addr.ProtocolAddr()
}

// AddressReference defines a function which matches the HashingReference function
// type and is the default use. It simply returns the Addr() value of a Addr object.
//
// This might not necessarily be desired as the address contains the actor's process id
// details which can become too specific in certain cases.
func AddressReference(addr Addr) string {
	return addr.Addr()
}

// HashedRouter implements a router which delivers messages to giving address
// based on hash value from message to possible address.
//
// It stores address by their Addr.Addr() which means even if two Addr are referencing
// same Actor, they will be respected, added and broadcasted to, as the Addr represents
// a unique capability.
type HashedRouter struct {
	ref    HashingReference
	hashes *HashedSet
	addrs  *ServiceSet
}

// NewHashedRouter returns a new instance of a HashedRouter.
func NewHashedRouter(ref HashingReference, addrs ...Addr) *HashedRouter {
	var service ServiceSet

	address := make([]string, 0, len(addrs))
	for _, addr := range addrs {
		if service.Add(addr) {
			address = append(address, ref(addr))
		}
	}

	return &HashedRouter{
		hashes: NewHashedSet(address),
		addrs:  &service,
		ref:    ref,
	}
}

// Action implements the Op interface.
func (rr *HashedRouter) Action(addr Addr, msg Envelope) {
	switch msg.Data.(type) {
	case AddRoute:
		if msg.Sender != nil && msg.Sender.ID() != addr.ID() {
			if rr.addrs.Add(msg.Sender) {
				rr.hashes.Add(rr.ref(msg.Sender))
			}
		}
	case RemoveRoute:
		if msg.Sender != nil && msg.Sender.ID() != addr.ID() {
			if rr.addrs.RemoveAddr(msg.Sender) {
				rr.hashes.Remove(rr.ref(msg.Sender))
			}
		}
	default:
		if hashed, ok := msg.Data.(Hashed); ok {
			if targetAddr, found := rr.hashes.Get(hashed.Hash()); found {
				if target, got := rr.addrs.Get(targetAddr); got {
					if err := target.Forward(msg); err != nil {
						log.Printf("[Hashed:Routing] Failed to route message %q to addr %q: %#v", msg.Ref.String(), addr.Addr(), err)
					}
					return
				}
				log.Printf("[Hashed:Routing] Failed get node for addr %q to route message %q", targetAddr, msg.Ref.String())
				return
			}

			log.Printf("[Hashed:Routing] Message %q data with hash %q has no routable address", msg.Ref.String(), hashed.Hash())
			return
		}

		log.Printf("[Hashed:Routing] Message %q data of type %T must implement Hashed interface: %#v", msg.Ref.String(), msg.Data, msg)
	}
}
