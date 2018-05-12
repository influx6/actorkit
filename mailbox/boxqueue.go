package mailbox

import (
	"github.com/gokit/actorkit"
	"sync"
	"sync/atomic"
)

var _ actorkit.Mailbox = &BoxQueue{}

//var nodePool = sync.Pool{New: func() interface{} {
//	return new(node)
//}}

type node struct{
	value *actorkit.Envelope
	next *node
	prev *node
}

// BoxQueue defines a queue implementation safe for concurrent-use
// across go-routines, which provides ability to requeue, pop and push
// new envelop messages. BoxQueue uses lock to guarantee safe concurrent use.
type BoxQueue struct{
	bm sync.Mutex
	head *node
	tail *node
	capped int
	total int64
	strategy Strategy
}

// Strategy defines a int type to represent a giving strategy.
type Strategy int

// constants.
const (
	DropNew Strategy = iota
	DropOld
)

// BoundedBoxQueue returns a new instance of a unbounded box queue.
// Items will be queue till the capped is reached and then old items
// will be dropped till queue has enough space for new item.
func BoundedBoxQueue(capp int, method Strategy) *BoxQueue{
	bq := &BoxQueue{
		capped: capp,
		strategy: method,
	}
	return bq
}

// UnboundedBoxQueue returns a new instance of a unbounded box queue.
// Items will be queue endlessly.
func UnboundedBoxQueue() *BoxQueue{
	bq := &BoxQueue{
		capped: -1,
	}
	return bq
}

// Push adds the item to the back of the queue.
//
// Push can be safely called from multiple goroutines.
// Based on strategy if capped, then a message will be dropped.
func (bq *BoxQueue) Push(env *actorkit.Envelope){
	available := int(atomic.LoadInt64(&bq.total))
	if bq.capped != -1 && available >= bq.capped{
		switch bq.strategy{
		case DropNew:
			return
		case DropOld:
			bq.Pop()
		}
	}

	atomic.AddInt64(&bq.total, 1)
	n := &node{value:env}
	//n := nodePool.Get().(*node)
	//n.value = env


	bq.bm.Lock()
	if bq.head == nil && bq.tail == nil {
		bq.head, bq.tail = n, n
		bq.bm.Unlock()
		return
	}

	bq.tail.next = n
	n.prev = bq.tail
	bq.tail = n
	bq.bm.Unlock()
}

// UnPop adds back item to the font of the queue.
//
// UnPop can be safely called from multiple goroutines.
// If queue is capped and max was reached, then last added
// message is removed to make space for message to be added back.
// This means strategy will be ignored since this is an attempt
// to re-add an item back into the top of the queue.
func (bq *BoxQueue) UnPop(env *actorkit.Envelope){
	available := int(atomic.LoadInt64(&bq.total))
	if bq.capped != -1 && available >= bq.capped{
			bq.unshift()
	}

	atomic.AddInt64(&bq.total, 1)
	n := &node{value:env}
	//n := nodePool.Get().(*node)
	//n.value = env

	bq.bm.Lock()
	head := bq.head
	if head != nil {
		n.next = head
		bq.head = n
		bq.bm.Unlock()
		return
	}

	bq.head = n
	bq.tail = n
	bq.bm.Unlock()
}

// Pops removes the item from the front of the queue.
//
// Pop can be safely called from multiple goroutines.
func (bq *BoxQueue) Pop() *actorkit.Envelope{
	bq.bm.Lock()
	head := bq.head
	if head != nil  {
		atomic.AddInt64(&bq.total, -1)

		v := head.value

		bq.head = head.next
		if bq.tail == head {
			bq.tail = bq.head
		}

		head.next = nil
		head.prev = nil
		head.value = nil
		bq.bm.Unlock()

		//nodePool.Put(head)
		return v
	}
	bq.bm.Unlock()
	return nil
}

func (bq *BoxQueue) unshift() {
	bq.bm.Lock()
	tail := bq.tail
	if tail != nil  {
		atomic.AddInt64(&bq.total, -1)

		bq.tail = tail.prev
		if tail == bq.head {
			bq.head = bq.tail
		}

		tail.next = nil
		tail.prev = nil
		tail.value = nil
	}
	bq.bm.Unlock()
	return
}

// Cap returns current cap of items.
func (bq *BoxQueue) Cap() int {
	return bq.capped
}

// Total returns total of item in mailbox.
func (bq *BoxQueue) Total() int {
	return int(atomic.LoadInt64(&bq.total))
}

// Empty returns true/false if the queue is empty.
func (bq *BoxQueue) Empty() bool {
	var empty bool
	bq.bm.Lock()
	empty = bq.head == nil && bq.tail == nil
	bq.bm.Unlock()
	return empty
}

