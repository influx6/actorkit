package actorkit

import (
	"math/rand"
	"sync"
	"sync/atomic"

	"github.com/serialx/hashring"
)

//**************************************
// RandomSet
//**************************************

// RandomSet implements a element set which returns
// a random item on every call to it's Get() method.
// It uses the internal random package, which is not
// truly random.
type RandomSet struct {
	procs []string
	set   map[string]int
}

// NewRandomSet returns a new instance of RandomSet.
func NewRandomSet() *RandomSet {
	return &RandomSet{
		set: map[string]int{},
	}
}

// Get will return the next Process in a round-robin
// random fashion, allowing some form of distributed calls
// for different process to handle messages.
func (p *RandomSet) Get() string {
	total := len(p.procs)
	target := rand.Intn(total)
	return p.procs[target]
}

// Total returns current total of items in round robin.
func (p *RandomSet) Total() int {
	return len(p.procs)
}

// Remove removes giving item from set.
func (p *RandomSet) Remove(proc string) {
	if !p.Has(proc) {
		return
	}

	index := p.set[proc]
	delete(p.set, proc)

	last := len(p.procs) - 1
	if last == 1 {
		p.procs = nil
		return
	}

	lastItem := p.procs[last]
	p.procs[index] = lastItem
	p.procs = p.procs[:last]
}

// Add adds giving item into set.
func (p *RandomSet) Add(proc string) {
	if p.Has(proc) {
		return
	}

	pIndex := len(p.procs)
	p.procs = append(p.procs, proc)
	p.set[proc] = pIndex
}

// Has returns true/false if giving item is in set.
func (p *RandomSet) Has(s string) bool {
	_, ok := p.set[s]
	return ok
}

//**************************************
// HashedSet
//**************************************

// HashedSet implements a giving set which is unique in that
// it has a hash ring underline which is encoded to return specific
// keys for specific hash strings. It allows consistently retrieving
// same key for same hash.
type HashedSet struct {
	set     map[string]struct{}
	hashing *hashring.HashRing
}

// NewHashedSet returns a new instance of HashedSet.
func NewHashedSet(set []string) *HashedSet {
	var hashed HashedSet
	hashed.set = map[string]struct{}{}
	hashed.hashing = hashring.New(set)

	for _, k := range set {
		hashed.set[k] = struct{}{}
	}

	return &hashed
}

// Get returns a giving item for provided hash value.
func (hs *HashedSet) Get(hashed string) (string, bool) {
	if content, ok := hs.hashing.GetNode(hashed); ok {
		return content, ok
	}
	return "", false
}

// Add adds giving item into set.
func (hs *HashedSet) Add(n string) {
	hs.hashing = hs.hashing.AddNode(n)
	hs.set[n] = struct{}{}
}

// Remove removes giving item from set.
func (hs *HashedSet) Remove(n string) {
	hs.hashing = hs.hashing.RemoveNode(n)
	delete(hs.set, n)
}

// Has returns true/false if giving item is in set.
func (hs *HashedSet) Has(n string) bool {
	_, ok := hs.set[n]
	return ok
}

//**************************************
// RoundRobinSet
//**************************************

// RoundRobinSet defines a process set/group which
// are processes offering the same service contract and
// will randomly based on index be provided when a process
// is needed for communication.
type RoundRobinSet struct {
	lastIndex int32
	procs     []string
	set       map[string]int
}

// NewRoundRobinSet returns a new instance of RoundRobinSet.
func NewRoundRobinSet() *RoundRobinSet {
	return &RoundRobinSet{
		set: map[string]int{},
	}
}

// Get will return the next Process in a round-robin
// random fashion, allowing some form of distributed calls
// for different process to handle messages.
func (p *RoundRobinSet) Get() string {
	var lastIndex int32
	total := int32(len(p.procs))
	if atomic.LoadInt32(&p.lastIndex) >= total {
		atomic.StoreInt32(&p.lastIndex, -1)
	}

	lastIndex = atomic.AddInt32(&p.lastIndex, 1)
	target := int(lastIndex % total)

	return p.procs[target]
}

// Total returns current total of items in round robin.
func (p *RoundRobinSet) Total() int {
	return len(p.procs)
}

// Remove removes giving item from set.
func (p *RoundRobinSet) Remove(proc string) {
	if !p.Has(proc) {
		return
	}

	index := p.set[proc]
	delete(p.set, proc)

	last := len(p.procs) - 1
	if last == 1 {
		p.procs = nil
		return
	}

	lastItem := p.procs[last]
	p.procs[index] = lastItem
	p.procs = p.procs[:last]
}

// Add adds giving item into set.
func (p *RoundRobinSet) Add(proc string) {
	if p.Has(proc) {
		return
	}

	pIndex := len(p.procs)
	p.procs = append(p.procs, proc)
	p.set[proc] = pIndex
}

// Has returns true/false if giving item is in set.
func (p *RoundRobinSet) Has(s string) bool {
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
// LocalityMap
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
