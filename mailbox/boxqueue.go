package mailbox

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/gokit/actorkit"
	"github.com/gokit/errors"
)

// ErrPushFailed is returned when mailbox has reached storage limit.
var ErrPushFailed = errors.New("failed to push into mailbox")

// ErrMailboxEmpty is returned when mailbox is empty of pending envelopes.
var ErrMailboxEmpty = errors.New("mailbox is empty")

var _ actorkit.Mailbox = &BoxQueue{}

var nodePool = sync.Pool{New: func() interface{} {
	return new(node)
}}

type node struct {
	value *actorkit.Envelope
	next  *node
	prev  *node
}

// BoxQueue defines a queue implementation safe for concurrent-use
// across go-routines, which provides ability to requeue, pop and push
// new envelop messages. BoxQueue uses lock to guarantee safe concurrent use.
type BoxQueue struct {
	bm       sync.Mutex
	cm       *sync.Cond
	head     *node
	tail     *node
	capped   int
	total    int64
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
// A cap value of -1 means there will be no maximum limit
// of allow messages in queue.
func BoundedBoxQueue(capped int, method Strategy) *BoxQueue {
	bq := &BoxQueue{
		capped:   capped,
		strategy: method,
	}
	bq.cm = sync.NewCond(&bq.bm)
	return bq
}

// UnboundedBoxQueue returns a new instance of a unbounded box queue.
// Items will be queue endlessly.
func UnboundedBoxQueue() *BoxQueue {
	bq := &BoxQueue{
		capped: -1,
	}
	bq.cm = sync.NewCond(&bq.bm)
	return bq
}

// WaitUntilPush will do a for-loop block until it successfully pushes giving
// envelope into queue.
func (bq *BoxQueue) WaitUntilPush(env actorkit.Envelope) {
	for true {
		if err := bq.Push(env); err != nil {
			continue
		}
		break
	}
}

// TryPushUntil will do a for-loop block for a giving number of tries with a giving wait duration in between
// that pushing of giving envelope, if tries runs out then an error is returned.
func (bq *BoxQueue) TryPushUntil(env actorkit.Envelope, retries int, timeout time.Duration) error {
	for i := 0; i < retries; i++ {
		if err := bq.Push(env); err != nil {
			<-time.After(timeout)
			continue
		}
		return nil
	}
	return ErrPushFailed
}

// Wait will block current goroutine till there is a message pushed into
// the queue, allowing you to effectively rely on it as a schedule and processing
// signal for when messages are in queue.
func (bq *BoxQueue) Wait() {
	bq.cm.L.Lock()
	if !bq.isEmpty() {
		bq.cm.L.Unlock()
		return
	}
	bq.cm.Wait()
	bq.cm.L.Unlock()
}

// Push adds the item to the back of the queue.
//
// Push can be safely called from multiple goroutines.
// Based on strategy if capped, then a message will be dropped.
func (bq *BoxQueue) Push(env actorkit.Envelope) error {
	available := int(atomic.LoadInt64(&bq.total))
	if bq.capped != -1 && available >= bq.capped {
		switch bq.strategy {
		case DropNew:
			return ErrPushFailed
		case DropOld:
			if _, err := bq.Pop(); err != nil && err != ErrMailboxEmpty {
				return err
			}
		}
	}

	atomic.AddInt64(&bq.total, 1)
	//n := &node{value: env}
	n := nodePool.Get().(*node)
	n.value = &env

	bq.cm.L.Lock()
	if bq.head == nil && bq.tail == nil {
		bq.head, bq.tail = n, n
		bq.cm.L.Unlock()

		bq.cm.Broadcast()
		return nil
	}

	bq.tail.next = n
	n.prev = bq.tail
	bq.tail = n
	bq.cm.L.Unlock()

	bq.cm.Broadcast()
	return nil
}

// UnPop adds back item to the font of the queue.
//
// UnPop can be safely called from multiple goroutines.
// If queue is capped and max was reached, then last added
// message is removed to make space for message to be added back.
// This means strategy will be ignored since this is an attempt
// to re-add an item back into the top of the queue.
func (bq *BoxQueue) UnPop(env actorkit.Envelope) {
	available := int(atomic.LoadInt64(&bq.total))
	if bq.capped != -1 && available >= bq.capped {
		bq.unshift()
	}

	atomic.AddInt64(&bq.total, 1)
	//n := &node{value: env}
	n := nodePool.Get().(*node)
	n.value = &env

	bq.cm.L.Lock()
	head := bq.head
	if head != nil {
		n.next = head
		bq.head = n
		bq.cm.L.Unlock()

		bq.cm.Broadcast()
		return
	}

	bq.head = n
	bq.tail = n
	bq.cm.L.Unlock()

	bq.cm.Broadcast()
}

// Pop removes the item from the front of the queue.
//
// Pop can be safely called from multiple goroutines.
func (bq *BoxQueue) Pop() (actorkit.Envelope, error) {
	bq.cm.L.Lock()
	head := bq.head
	if head != nil {
		atomic.AddInt64(&bq.total, -1)

		v := head.value

		bq.head = head.next
		if bq.tail == head {
			bq.tail = bq.head
		}

		head.next = nil
		head.prev = nil
		head.value = nil
		bq.cm.L.Unlock()

		nodePool.Put(head)
		return *v, nil
	}
	bq.cm.L.Unlock()
	return actorkit.Envelope{}, ErrMailboxEmpty
}

// unshift discards the tail of queue, allowing new space.
func (bq *BoxQueue) unshift() {
	bq.cm.L.Lock()
	tail := bq.tail
	if tail != nil {
		atomic.AddInt64(&bq.total, -1)

		bq.tail = tail.prev
		if tail == bq.head {
			bq.head = bq.tail
		}

		tail.next = nil
		tail.prev = nil
		tail.value = nil
	}
	bq.cm.L.Unlock()
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
	bq.cm.L.Lock()
	empty = bq.isEmpty()
	bq.cm.L.Unlock()
	return empty
}

func (bq *BoxQueue) isEmpty() bool {
	return bq.head == nil && bq.tail == nil
}
