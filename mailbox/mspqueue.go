package mailbox

import (
	"sync/atomic"
	"unsafe"
	"github.com/gokit/actorkit"
)

// MSQueue provides an efficient implementation of a multi-producer, single-consumer lock-free queue.
//
// The Push function is safe to call from multiple goroutines. The Pop and Empty APIs must only be
// called from a single, consumer goroutine.
//
// This implementation is based on http://www.1024cores.net/home/lock-free-algorithms/queues/non-intrusive-mpsc-node-based-queue
type MSPQueue struct {
	head, tail *node
}

// NewMSPQueue returns a new instance of MSPQueue.
func NewMSPQueue() *MSPQueue {
	q := &MSPQueue{}
	stub := &node{}
	q.head = stub
	q.tail = stub
	return q
}

// Push adds x to the back of the queue.
//
// Push can be safely called from multiple goroutines
func (q *MSPQueue) Push(x actorkit.Envelope) {
	n := &node{value: x}
	// current producer acquires head node
	prev := (*node)(atomic.SwapPointer((*unsafe.Pointer)(unsafe.Pointer(&q.head)), unsafe.Pointer(n)))

	// release node to consumer
	prev.next = n
}

// UnPop adds x back into the head of the queue.
//
// UnPop can be safely called from multiple goroutines
func (q *MSPQueue) UnPop(x actorkit.Envelope) {
	n := &node{value: x, next: (*node)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&q.tail.next))))}

	// current producer acquires head node
	_ = (*node)(atomic.SwapPointer((*unsafe.Pointer)(unsafe.Pointer(&q.tail.next)), unsafe.Pointer(n)))

	// ensure node points to previous.
	//n.next = prev
}

// Pop removes the item from the front of the queue or nil if the queue is empty
//
// Pop must be called from a single, consumer goroutine
func (q *MSPQueue) Pop() actorkit.Envelope {
	tail := q.tail
	next := (*node)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&tail.next)))) // acquire
	if next != nil {
		q.tail = next
		v := next.value
		next.value = nil
		return v
	}
	return nil
}

// Empty returns true if the queue is empty
//
// Empty must be called from a single, consumer goroutine
func (q *MSPQueue) Empty() bool {
	tail := q.tail
	next := (*node)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&tail.next))))
	return next == nil
}
