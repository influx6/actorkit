package actorkit

import (
	"sync"
	"errors"
	"sync/atomic"
)

var (
	defaultResolver = NewLocalResolver()
)

var (
	// ErrIDAlreadyExists is returned when a process is already registered with id.
	ErrIDAlreadyExists = errors.New("Process.ID already existing")
)

// GetProcessRegistry returns the default resolver for the package.
func GetProcessRegistry() ProcessRegistry {
	return defaultResolver
}

//**************************************
// LocalResolver implements Resolver
//**************************************

// roundRobinProcessSet defines a process set/group which
// are processes offering the same service contract and
// will randomly based on index be provided when a process
// is needed for communication.
type roundRobinProcessSet struct{
	lastIndex int32
	sl sync.RWMutex
	set map[string]int
	procs []Process
}

func newRoundRobinProcessSet() *roundRobinProcessSet{
	return &roundRobinProcessSet{
		set: map[string]int{},
	}
}

// GetRobin will return the next Process in a round-robin
// random fashion, allowing some form of distributed calls
// for different process to handle messages.
func (p *roundRobinProcessSet) GetRobin() Process  {
	p.sl.Lock()
	defer p.sl.Unlock()

	var lastIndex int32

	total := int32(len(p.procs))
	if atomic.LoadInt32(&p.lastIndex) >= total {
		atomic.StoreInt32(&p.lastIndex, -1)
	}

	lastIndex = atomic.AddInt32(&p.lastIndex, 1)

	target := int(lastIndex%total)
	return p.procs[target]
}

func (p *roundRobinProcessSet) Total() int  {
	p.sl.RLock()
	defer p.sl.RUnlock()
	return len(p.procs)
}

func (p *roundRobinProcessSet) Copy() []Process  {
	p.sl.RLock()
	defer p.sl.RUnlock()

	set := make([]Process, 0, len(p.procs))
	set = append(set, p.procs...)
	return set
}

func (p *roundRobinProcessSet) Remove(proc Process)  {
	if !p.Has(proc.ID()){
		return
	}

	p.sl.Lock()
	defer p.sl.Unlock()

	index := p.set[proc.ID()]
	total := len(p.procs)

	item := p.procs[total-1]
	if total == 1 {
		delete(p.set, proc.ID())
		p.procs = nil
		return
	}

	p.procs[index] = item
	p.set[item.ID()] = index
	p.procs = p.procs[:total-1]
}


func (p *roundRobinProcessSet) Add(proc Process)  {
	if p.Has(proc.ID()){
		return
	}

	p.sl.Lock()
	defer p.sl.Unlock()
	p.set[proc.ID()] = len(p.procs)
	p.procs = append(p.procs, proc)
}

func (p *roundRobinProcessSet) Has(s string) bool {
	p.sl.RLock()
	defer p.sl.RUnlock()
	_, ok := p.set[s]
	return ok
}

// LocalResolver implements the Resolver interface.
type LocalResolver struct{
	sm sync.RWMutex
	service map[string]*roundRobinProcessSet
}

// NewLocalResolver returns a new instance of LocalResolver.
// It implements the Resolver and FleetResolver interface.
func NewLocalResolver() *LocalResolver{
	return &LocalResolver{
		service: map[string]*roundRobinProcessSet{},
	}
}

// Register adds giving set into LocalResolver.
func (lr *LocalResolver) Register(process Process, service string) error {
	lr.sm.Lock()
	if set, ok := lr.service[service]; ok {
		lr.sm.Unlock()
		if !set.Has(process.ID()){
			set.Add(process)
		}
		return ErrIDAlreadyExists
	}
	lr.sm.Unlock()

	set := newRoundRobinProcessSet()
	set.Add(process)

	lr.sm.Lock()
	lr.service[service] = set
	lr.sm.Unlock()
	return nil
}

func (lr *LocalResolver) Fleets(service string) ([]Process, error) {
	lr.sm.RLock()
	if set, ok := lr.service[service]; ok {
		lr.sm.RUnlock()
		return set.Copy(), nil
	}

	return nil, ErrFleetsNotFound
}

func (lr *LocalResolver) Resolve(m Mask) (Process, bool) {
	lr.sm.RLock()
	if set, ok := lr.service[m.Service()]; ok {
		lr.sm.RUnlock()
		return set.GetRobin(), true
	}
	return nil, false
}

//**************************************
// sourceResolver implements Resolver
//**************************************

// ResolveAlways returns a Resolver that always returns
// given process regardless of Mask.
func ResolveAlways(p Process) Resolver {
	return &sourceResolver{target:p}
}

// sourceResolver implements the Resolver interface.
type sourceResolver struct{
	target Process
}

func (s sourceResolver) Fleets(m Mask) ([]Process, error) {
	return []Process{s.target}, nil
}

func (s sourceResolver) ResolveById(m string) (Process, bool){
	return s.target, true
}

// Resolve implements the Resolver interface.
func (s sourceResolver) Resolve(m Mask) (Process, bool){
	return s.target, true
}

//**************************************
// locality
//**************************************

type localitySrc interface{
	Remove(*localityIndex) 
	Get(*localityIndex) interface{}
}

type localityIndex struct{
	index int
	src localitySrc
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

type localityMap struct{
	rl sync.RWMutex
	items map[int]interface{}
}

func (l *localityMap) Get(index *localityIndex)  interface{} {
	if index.index == -1 {
		return nil
	}

	l.rl.RLock()
	defer l.rl.RUnlock()

	return l.items[index.index]
}

func (l *localityMap) Remove(index *localityIndex)  {
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
	delete(l.items, total -1)
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

type localityList struct{
	rl sync.RWMutex
	items []interface{}	
}

func (l *localityList) Get(index *localityIndex)  interface{} {
	if index.index == -1 {
		return nil
	}

	l.rl.RLock()
	defer l.rl.RUnlock()

	return l.items[index.index]
}

func (l *localityList) Remove(index *localityIndex)  {
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
