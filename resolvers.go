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

	// ErrProcNotFound is returned when a process does not exists in registry.
	ErrProcNotFound = errors.New("Process not found in registry")
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
	src *LocalResolver
	set map[string]int
	ribbon map[int]string
}

func newRoundRobinProcessSet(rs *LocalResolver) *roundRobinProcessSet{
	return &roundRobinProcessSet{
		set: map[string]int{},
		ribbon: map[int]string{},
		src: rs,
	}
}

// GetRobin will return the next Process in a round-robin
// random fashion, allowing some form of distributed calls
// for different process to handle messages.
func (p *roundRobinProcessSet) GetRobin() Process  {
	var lastIndex int32
	total := int32(len(p.ribbon))
	if atomic.LoadInt32(&p.lastIndex) >= total {
		atomic.StoreInt32(&p.lastIndex, -1)
	}

	lastIndex = atomic.AddInt32(&p.lastIndex, 1)
	target := int(lastIndex%total)

	return p.src.procs[p.set[p.ribbon[target]]]
}

func (p *roundRobinProcessSet) Total() int  {
	return len(p.set)
}

func (p *roundRobinProcessSet) CopyOnly(target []Process) []Process  {
	for _, ind := range p.set{
		target = append(target, p.src.procs[ind])
	}
	return target
}

func (p *roundRobinProcessSet) Copy(target []Process,seen map[string]struct{}) []Process  {
	for key, ind := range p.set{
		if _, ok := seen[key]; ok {
			continue
		}
		target = append(target, p.src.procs[ind])
		seen[key] = struct{}{}
	}
	return target
}

func (p *roundRobinProcessSet) RemoveInSet(proc Process)  {
	if !p.Has(proc.ID()){
		return
	}

	delete(p.set, proc.ID())

	var c = 0

	// we bare the cost of removal here.
	for k, m := range p.ribbon {
		if m == proc.ID(){
			delete(p.ribbon, k)
			continue
		}

		p.ribbon[c] = m
		c++
	}
}


func (p *roundRobinProcessSet) Add(proc Process)  {
	if p.Has(proc.ID()){
		return
	}

	p.ribbon[len(p.ribbon)] = proc.ID()

	if ind, ok := p.src.seen[proc.ID()]; ok{
		p.set[proc.ID()] = ind
		return
	}

	p.set[proc.ID()] = len(p.src.procs)
	p.src.seen[proc.ID()] = len(p.src.procs)
	p.src.procs = append(p.src.procs, proc)
}

func (p *roundRobinProcessSet) Has(s string) bool {
	_, ok := p.set[s]
	return ok
}

// LocalResolver implements the Resolver interface.
type LocalResolver struct{
	sm sync.RWMutex
	seen map[string]int
	service map[string]*roundRobinProcessSet
	procs []Process
}

// NewLocalResolver returns a new instance of LocalResolver.
// It implements the Resolver and FleetResolver interface.
func NewLocalResolver() *LocalResolver{
	return &LocalResolver{
		seen: map[string]int{},
		service: map[string]*roundRobinProcessSet{},
	}
}

// GetProcess attempts to retrieve process using provided id
// else returning a false if not found.
func (lr *LocalResolver) GetProcess(id string) (Process, error)  {
	lr.sm.Lock()
	defer lr.sm.Unlock()

	if index, ok := lr.seen[id]; ok {
		return lr.procs[index], nil
	}

	return nil, ErrProcNotFound
}

func (lr *LocalResolver) Unregister(process Process, service string)  {
	lr.sm.Lock()
	defer lr.sm.Unlock()

	if set, ok := lr.service[service]; ok {
		set.RemoveInSet(process)
	}
}

// Register adds giving set into LocalResolver.
func (lr *LocalResolver) Register(process Process, service string)  {
	lr.sm.Lock()
	defer lr.sm.Unlock()

	if set, ok := lr.service[service]; ok {
		set.Add(process)
		return
	}

	set := newRoundRobinProcessSet(lr)
	set.Add(process)

	// Add watcher to ensure process gets removed here.
	process.AddWatcher(deadMask, func(msg interface{}) {
		if _, ok := msg.(ProcessShuttingDown); ok {
			lr.Remove(process)
		}
	})

	lr.service[service] = set
}

func (lr *LocalResolver) Fleets(service string) ([]Process, error) {
	lr.sm.RLock()
	defer lr.sm.RUnlock()

	if set, ok := lr.service[service]; ok {
		return set.CopyOnly(make([]Process, 0, set.Total())), nil
	}

	return nil, ErrFleetsNotFound
}

func (lr *LocalResolver) Resolve(m Mask) (Process, bool) {
	lr.sm.RLock()
	defer lr.sm.RUnlock()
	if set, ok := lr.service[m.Service()]; ok {
		return set.GetRobin(), true
	}
	return nil, false
}

func (p *LocalResolver) Remove(proc Process)  {
	// go-routine this due to possible locks
	go proc.RemoveWatcher(deadMask)

	p.sm.Lock()
	defer p.sm.Unlock()

	for _, set := range p.service {
		set.RemoveInSet(proc)
	}

	index := p.seen[proc.ID()]
	total := len(p.procs)

	item := p.procs[total-1]
	if total == 1 {
		delete(p.seen, proc.ID())
		p.procs = nil
		return
	}

	delete(p.seen, proc.ID())
	p.procs[index] = item
	p.seen[item.ID()] = index
	p.procs = p.procs[:total-1]
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
