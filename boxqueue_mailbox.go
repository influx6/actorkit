package actorkit

import (
	"sync"
	"sync/atomic"

	"github.com/gokit/errors"
)

// ErrPushFailed is returned when mailbox has reached storage limit.
var ErrPushFailed = errors.New("failed to push into mailbox")

// ErrMailboxEmpty is returned when mailbox is empty of pending envelopes.
var ErrMailboxEmpty = errors.New("mailbox is empty")

var (
	_        Mailbox = &BoxQueue{}
	nodePool         = sync.Pool{New: func() interface{} {
		return new(node)
	}}
)

// Strategy defines a int type to represent a giving strategy.
type Strategy int

// constants.
const (
	DropNew Strategy = iota
	DropOld
)

type node struct {
	addr  Addr
	value *Envelope
	next  *node
	prev  *node
}

// BoxQueue defines a queue implementation safe for concurrent-use
// across go-routines, which provides ability to requeue, pop and push
// new envelop messages. BoxQueue uses lock to guarantee safe concurrent use.
type BoxQueue struct {
	bm       sync.Mutex
	pushCond *sync.Cond
	head     *node
	tail     *node
	capped   int
	total    int64
	strategy Strategy
	invoker  MailInvoker
}

// BoundedBoxQueue returns a new instance of a unbounded box queue.
// Items will be queue till the capped is reached and then old items
// will be dropped till queue has enough space for new item.
// A cap value of -1 means there will be no maximum limit
// of allow messages in queue.
func BoundedBoxQueue(capped int, method Strategy, invoker MailInvoker) *BoxQueue {
	bq := &BoxQueue{
		capped:   capped,
		strategy: method,
		invoker:  invoker,
	}
	bq.pushCond = sync.NewCond(&bq.bm)
	return bq
}

// UnboundedBoxQueue returns a new instance of a unbounded box queue.
// Items will be queue endlessly.
func UnboundedBoxQueue(invoker MailInvoker) *BoxQueue {
	bq := &BoxQueue{
		capped:  -1,
		invoker: invoker,
	}
	bq.pushCond = sync.NewCond(&bq.bm)
	return bq
}

// Signal sends a signal to all listening go-routines to
// attempt checks for new message.
func (bq *BoxQueue) Signal() {
	bq.pushCond.Broadcast()
}

// Clear resets and deletes all elements pending within queue
func (bq *BoxQueue) Clear() {
	bq.pushCond.L.Lock()

	if bq.isEmpty() {
		bq.pushCond.L.Unlock()
		return
	}

	bq.tail = nil
	bq.head = nil
	bq.pushCond.L.Unlock()

	bq.pushCond.Broadcast()
}

// Wait will block current goroutine till there is a message pushed into
// the queue, allowing you to effectively rely on it as a schedule and processing
// signal for when messages are in queue.
func (bq *BoxQueue) Wait() {
	bq.pushCond.L.Lock()
	if !bq.isEmpty() {
		bq.pushCond.L.Unlock()
		return
	}
	bq.pushCond.Wait()
	bq.pushCond.L.Unlock()
}

// Push adds the item to the back of the queue.
//
// Push can be safely called from multiple goroutines.
// Based on strategy if capped, then a message will be dropped.
func (bq *BoxQueue) Push(addr Addr, env Envelope) error {
	available := int(atomic.LoadInt64(&bq.total))
	if bq.capped != -1 && available >= bq.capped {
		if bq.invoker != nil {
			bq.invoker.InvokedFull()
		}

		switch bq.strategy {
		case DropNew:
			if bq.invoker != nil {
				bq.invoker.InvokedDropped(addr, env)
			}
			return errors.Wrap(ErrPushFailed, "")
		case DropOld:
			if addrs, envs, err := bq.Pop(); err == nil {
				if bq.invoker != nil {
					bq.invoker.InvokedDropped(addrs, envs)
				}
			}
		}
	}

	atomic.AddInt64(&bq.total, 1)
	n := nodePool.Get().(*node)
	n.value = &env
	n.addr = addr

	if bq.invoker != nil {
		bq.invoker.InvokedReceived(addr, env)
	}

	bq.pushCond.L.Lock()
	if bq.head == nil && bq.tail == nil {
		bq.head, bq.tail = n, n
		bq.pushCond.L.Unlock()

		bq.pushCond.Broadcast()
		return nil
	}

	bq.tail.next = n
	n.prev = bq.tail
	bq.tail = n
	bq.pushCond.L.Unlock()

	bq.pushCond.Broadcast()
	return nil
}

// Unpop adds back item to the font of the queue.
//
// Unpop can be safely called from multiple goroutines.
// If queue is capped and max was reached, then last added
// message is removed to make space for message to be added back.
// This means strategy will be ignored since this is an attempt
// to re-add an item back into the top of the queue.
func (bq *BoxQueue) Unpop(addr Addr, env Envelope) {
	available := int(atomic.LoadInt64(&bq.total))
	if bq.capped != -1 && available >= bq.capped {
		bq.unshift()
	}

	atomic.AddInt64(&bq.total, 1)
	n := nodePool.Get().(*node)
	n.value = &env
	n.addr = addr

	if bq.invoker != nil {
		bq.invoker.InvokedReceived(addr, env)
	}

	bq.pushCond.L.Lock()
	head := bq.head
	if head != nil {
		n.next = head
		bq.head = n
		bq.pushCond.L.Unlock()

		bq.pushCond.Broadcast()
		return
	}

	bq.head = n
	bq.tail = n
	bq.pushCond.L.Unlock()

	bq.pushCond.Broadcast()
}

// Pop removes the item from the front of the queue.
//
// Pop can be safely called from multiple goroutines.
func (bq *BoxQueue) Pop() (Addr, Envelope, error) {
	bq.pushCond.L.Lock()
	head := bq.head
	if head != nil {
		atomic.AddInt64(&bq.total, -1)

		v := head.value
		addr := head.addr

		if bq.invoker != nil {
			bq.invoker.InvokedDispatched(addr, *v)
		}

		bq.head = head.next
		if bq.tail == head {
			bq.tail = bq.head
		}

		head.next = nil
		head.prev = nil
		head.addr = nil
		head.value = nil
		bq.pushCond.L.Unlock()

		nodePool.Put(head)

		return addr, *v, nil
	}
	bq.pushCond.L.Unlock()

	if bq.invoker != nil {
		bq.invoker.InvokedEmpty()
	}

	return nil, Envelope{}, errors.Wrap(ErrMailboxEmpty, "empty mailbox")
}

// unshift discards the tail of queue, allowing new space.
func (bq *BoxQueue) unshift() {
	bq.pushCond.L.Lock()
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
	bq.pushCond.L.Unlock()
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

// IsEmpty returns true/false if the queue is empty.
func (bq *BoxQueue) IsEmpty() bool {
	var empty bool
	bq.pushCond.L.Lock()
	empty = bq.isEmpty()
	bq.pushCond.L.Unlock()
	return empty
}

func (bq *BoxQueue) isEmpty() bool {
	return bq.head == nil && bq.tail == nil
}
