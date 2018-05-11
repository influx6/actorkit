package mailbox

import (
	"github.com/gokit/actorkit"
	"sync"
)

//var nodePool = sync.Pool{New: func() interface{} {
//	return new(node)
//}}

type node struct{
	value *actorkit.Envelope
	next *node
}

// BoxQueue defines a queue implementation safe for concurrent-use
// across go-routines, which provides ability to requeue, pop and push
// new envelop messages. BoxQueue uses lock to guarantee safe concurrent use.
type BoxQueue struct{
	bm sync.Mutex
	head *node
	tail *node
	capped int
}

// BoundedBoxQueue returns a new instance of a unbounded box queue.
// Items will be queue till the capped is reached and then old items
// will be dropped till queue has enough space for new item.
func BoundedBoxQueue(capp int) *BoxQueue{
	bq := &BoxQueue{
		capped: capp,
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
func (bq *BoxQueue) Push(env *actorkit.Envelope){
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
	bq.tail = n
	bq.bm.Unlock()
}

func (bq *BoxQueue) UnPop(env *actorkit.Envelope){
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
		v := head.value

		bq.head = head.next
		if bq.tail == head {
			bq.tail = bq.head
		}

		head.next = nil
		head.value = nil
		bq.bm.Unlock()

		//nodePool.Put(head)
		return v
	}
	bq.bm.Unlock()
	return nil
}

// Empty returns true/false if the queue is empty.
func (bq *BoxQueue) Empty() bool {
	var empty bool
	bq.bm.Lock()
	empty = bq.head == nil && bq.tail == nil
	bq.bm.Unlock()
	return empty
}

