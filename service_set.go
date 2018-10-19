package actorkit

import (
	"sync"
	"sync/atomic"

	"github.com/gokit/errors"
)

var (
	// ErrIDAlreadyExists is returned when a process is already registered with id.
	ErrIDAlreadyExists = errors.New("Actor.ID already existing")

	// ErrProcNotFound is returned when a process does not exists in registry.
	ErrProcNotFound = errors.New("Actor not found in registry")

	// ErrFleetNotFound is returned when giving actor fleet for service is not found.
	ErrFleetNotFound = errors.New("Actor fleet not found in registry")
)

//***********************************************
// ServiceSet: A round robin service registry
//***********************************************

// ServiceSet implements the Resolver interface.
type ServiceSet struct {
	sm      sync.RWMutex
	seen    map[string]int
	service map[string]*roundRobinProcessSet
	subs    map[string]Subscription
	procs   []Actor
}

// NewServiceSet returns a new instance of LocalResolver.
// It implements the Resolver and FleetResolver interface.
func NewServiceSet() *ServiceSet {
	return &ServiceSet{
		seen:    map[string]int{},
		service: map[string]*roundRobinProcessSet{},
		subs:    map[string]Subscription{},
	}
}

// GetProcess attempts to retrieve process using provided id
// else returning a false if not found.
func (lr *ServiceSet) GetProcess(id string) (Actor, error) {
	lr.sm.Lock()
	defer lr.sm.Unlock()

	if index, ok := lr.seen[id]; ok {
		return lr.procs[index], nil
	}

	return nil, ErrProcNotFound
}

// Unregister a process from giving service.
func (lr *ServiceSet) Unregister(process Actor, service string) {
	lr.sm.Lock()
	defer lr.sm.Unlock()

	if set, ok := lr.service[service]; ok {
		set.RemoveInSet(process)
	}
}

// Register adds giving set into LocalResolver.
func (lr *ServiceSet) Register(process Actor, service string) {
	lr.sm.Lock()
	defer lr.sm.Unlock()

	if set, ok := lr.service[service]; ok {
		set.Add(process)

		// Add watcher to ensure process gets removed here.
		lr.subs[process.Addr()] = process.Fn(func(msg interface{}) {
			if _, ok := msg.(*ActorStopped); ok {
				lr.Remove(process)
			}
		})

		return
	}

	set := newroundRobinProcessSet(lr)
	set.Add(process)

	// Add watcher to ensure process gets removed here.
	lr.subs[process.Addr()] = process.Fn(func(msg interface{}) {
		if _, ok := msg.(*ActorStopped); ok {
			lr.Remove(process)
		}
	})

	lr.service[service] = set
}

// Resolve returns a random Actor offering giving service.
func (lr *ServiceSet) Resolve(service string) (Actor, bool) {
	lr.sm.RLock()
	defer lr.sm.RUnlock()
	if set, ok := lr.service[service]; ok {
		return set.GetRobin(), true
	}
	return nil, false
}

// Remove removes giving actor from all underline service sets.
func (lr *ServiceSet) Remove(proc Actor) {
	lr.sm.Lock()
	defer lr.sm.Unlock()

	if sub, ok := lr.subs[proc.Addr()]; ok {
		sub.Stop()
	}

	for _, set := range lr.service {
		set.RemoveInSet(proc)
	}

	index := lr.seen[proc.ID()]
	total := len(lr.procs)

	item := lr.procs[total-1]
	if total == 1 {
		delete(lr.seen, proc.ID())
		lr.procs = nil
		return
	}

	delete(lr.seen, proc.ID())
	lr.procs[index] = item
	lr.seen[item.ID()] = index
	lr.procs = lr.procs[:total-1]
}

//**************************************
// roundRobinProcessSet
//**************************************

// roundRobinProcessSet defines a process set/group which
// are processes offering the same service contract and
// will randomly based on index be provided when a process
// is needed for communication.
type roundRobinProcessSet struct {
	lastIndex int32
	src       *ServiceSet
	set       map[string]int
	ribbon    map[int]string
}

func newroundRobinProcessSet(rs *ServiceSet) *roundRobinProcessSet {
	return &roundRobinProcessSet{
		set:    map[string]int{},
		ribbon: map[int]string{},
		src:    rs,
	}
}

// GetRobin will return the next Actor in a round-robin
// random fashion, allowing some form of distributed calls
// for different process to handle messages.
func (p *roundRobinProcessSet) GetRobin() Actor {
	var lastIndex int32
	total := int32(len(p.ribbon))
	if atomic.LoadInt32(&p.lastIndex) >= total {
		atomic.StoreInt32(&p.lastIndex, -1)
	}

	lastIndex = atomic.AddInt32(&p.lastIndex, 1)
	target := int(lastIndex % total)

	return p.src.procs[p.set[p.ribbon[target]]]
}

// Total returns total of set.
func (p *roundRobinProcessSet) Total() int {
	return len(p.set)
}

// Copy returns a copy of all process within set.
func (p *roundRobinProcessSet) CopyOnly(target []Actor) []Actor {
	for _, ind := range p.set {
		target = append(target, p.src.procs[ind])
	}
	return target
}

// Copy retuirns a slice of all process within set.
func (p *roundRobinProcessSet) Copy(target []Actor, seen map[string]struct{}) []Actor {
	for key, ind := range p.set {
		if _, ok := seen[key]; ok {
			continue
		}
		target = append(target, p.src.procs[ind])
		seen[key] = struct{}{}
	}
	return target
}

// RemoveInSet removes process form set.
func (p *roundRobinProcessSet) RemoveInSet(proc Actor) {
	if !p.Has(proc.ID()) {
		return
	}

	delete(p.set, proc.ID())

	var c = 0

	// we bare the cost of removal here.
	for k, m := range p.ribbon {
		if m == proc.ID() {
			delete(p.ribbon, k)
			continue
		}

		p.ribbon[c] = m
		c++
	}
}

// Add adds giving Actor into list.
func (p *roundRobinProcessSet) Add(proc Actor) {
	if p.Has(proc.ID()) {
		return
	}

	p.ribbon[len(p.ribbon)] = proc.ID()

	if ind, ok := p.src.seen[proc.ID()]; ok {
		p.set[proc.ID()] = ind
		return
	}

	p.set[proc.ID()] = len(p.src.procs)
	p.src.seen[proc.ID()] = len(p.src.procs)
	p.src.procs = append(p.src.procs, proc)
}

// Has returns true/false if set has giving string.
func (p *roundRobinProcessSet) Has(s string) bool {
	_, ok := p.set[s]
	return ok
}

//**************************************
// locality
//**************************************

type localitySrc interface {
	Remove(*localityIndex)
	Get(*localityIndex) interface{}
}

type localityIndex struct {
	index int
	src   localitySrc
}

func (l *localityIndex) Remove() {
	l.src.Remove(l)
}

func (l *localityIndex) Get() interface{} {
	return l.src.Get(l)
}

//**************************************
// localityMap
//**************************************

type localityMap struct {
	rl    sync.RWMutex
	items map[int]interface{}
}

func (l *localityMap) Get(index *localityIndex) interface{} {
	if index.index == -1 {
		return nil
	}

	l.rl.RLock()
	defer l.rl.RUnlock()

	return l.items[index.index]
}

func (l *localityMap) Remove(index *localityIndex) {
	if index.index == -1 {
		return
	}

	l.rl.Lock()
	defer l.rl.Unlock()

	total := len(l.items)
	if total == 1 {
		l.items = nil
		index.index = 1
		return
	}

	l.items[index.index] = l.items[total-1]
	delete(l.items, total-1)
}

func (l *localityMap) Add(item interface{}) *localityIndex {
	index := new(localityIndex)
	index.src = l

	l.rl.Lock()
	defer l.rl.Unlock()

	index.index = len(l.items)
	l.items[index.index] = item
	return index
}

//**************************************
// localityList
//**************************************

type localityList struct {
	rl    sync.RWMutex
	items []interface{}
}

func (l *localityList) Get(index *localityIndex) interface{} {
	if index.index == -1 {
		return nil
	}

	l.rl.RLock()
	defer l.rl.RUnlock()

	return l.items[index.index]
}

func (l *localityList) Remove(index *localityIndex) {
	if index.index == -1 {
		return
	}

	l.rl.Lock()
	defer l.rl.Unlock()

	total := len(l.items)
	if total == 1 {
		l.items = nil
		index.index = 1
		return
	}

	l.items[index.index] = l.items[total-1]
	l.items = l.items[:total-1]
}

func (l *localityList) Add(item interface{}) *localityIndex {
	index := new(localityIndex)
	index.src = l

	l.rl.Lock()
	defer l.rl.Unlock()

	index.index = len(l.items)
	l.items = append(l.items, item)
	return index
}
