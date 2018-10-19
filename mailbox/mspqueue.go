package mailbox

import (
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/gokit/actorkit"
)

// MSPQueue provides an efficient implementation of a multi-producer, single-consumer lock-free queue.
//
// The Push function is safe to call from multiple goroutines. The Pop and Empty APIs must only be
// called from a single, consumer goroutine.
//
// This implementation is based on http://www.1024cores.net/home/lock-free-algorithms/queues/non-intrusive-mpsc-node-based-queue
type MSPQueue struct {
	cap        int
	count      int64
	strategy   Strategy
	head, tail *node
}

// NewMSPQueue returns a new instance of MSPQueue.
// A cap value of -1 means there will be no maximum limit
// of allow messages in queue.
func NewMSPQueue(cap int, strategy Strategy) *MSPQueue {
	q := &MSPQueue{cap: cap, strategy: strategy}
	stub := &node{}
	q.head = stub
	q.tail = stub
	return q
}

// Push adds x to the back of the queue.
//
// Push can be safely called from multiple goroutines
func (q *MSPQueue) Push(x actorkit.Envelope) error {
	available := int(atomic.LoadInt64(&q.count))
	if q.cap != -1 && available >= q.cap {
		switch q.strategy {
		case DropNew:
			return ErrPushFailed
		case DropOld:
			if _, err := q.Pop(); err != nil && err != ErrMailboxEmpty {
				return err
			}
		}
	}

	n := &node{value: &x}
	atomic.AddInt64(&q.count, 1)

	// current producer acquires head node
	prev := (*node)(atomic.SwapPointer((*unsafe.Pointer)(unsafe.Pointer(&q.head)), unsafe.Pointer(n)))

	// release node to consumer
	prev.next = n
	return nil
}

// WaitUntilPush will do a for-loop block until it successfully pushes giving
// envelope into queue.
func (q *MSPQueue) WaitUntilPush(env actorkit.Envelope) {
	for true {
		if err := q.Push(env); err != nil {
			continue
		}
		break
	}
}

// TryPushUntil will do a for-loop block for a giving number of tries with a giving wait duration in between
// that pushing of giving envelope, if tries runs out then an error is returned.
func (q *MSPQueue) TryPushUntil(env actorkit.Envelope, retries int, timeout time.Duration) error {
	for i := 0; i < retries; i++ {
		if err := q.Push(env); err != nil {
			<-time.After(timeout)
			continue
		}
		return nil
	}
	return ErrPushFailed
}

// UnPop adds x back into the head of the queue.
//
// UnPop can be safely called from multiple goroutines
func (q *MSPQueue) UnPop(x actorkit.Envelope) {
	n := &node{value: &x, next: (*node)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&q.tail.next))))}

	// current producer acquires head node
	_ = (*node)(atomic.SwapPointer((*unsafe.Pointer)(unsafe.Pointer(&q.tail.next)), unsafe.Pointer(n)))

	// ensure node points to previous.
	//n.next = prev
}

// Pop removes the item from the front of the queue or nil if the queue is empty
//
// Pop must be called from a single, consumer goroutine
func (q *MSPQueue) Pop() (actorkit.Envelope, error) {
	tail := q.tail
	next := (*node)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&tail.next)))) // acquire
	if next != nil {
		q.tail = next
		v := next.value
		next.value = nil
		return *v, nil
	}
	return actorkit.Envelope{}, ErrMailboxEmpty
}

// Empty returns true if the queue is empty
//
// Empty must be called from a single, consumer goroutine
func (q *MSPQueue) Empty() bool {
	tail := q.tail
	next := (*node)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&tail.next))))
	return next == nil
}
