package actorkit

import (
	"sync/atomic"
	"time"
)

// AtomicBool implements a safe atomic boolean.
type AtomicBool struct {
	flag int32
}

// IsTrue returns true/false if giving atomic bool is in true state.
func (a *AtomicBool) IsTrue() bool {
	return atomic.LoadInt32(&a.flag) == 1
}

// Off sets the atomic bool as false.
func (a *AtomicBool) Off() {
	atomic.StoreInt32(&a.flag, 0)
}

// On sets the atomic bool as true.
func (a *AtomicBool) On() {
	atomic.StoreInt32(&a.flag, 1)
}

// AtomicCounter implements a wrapper around a int32.
type AtomicCounter struct {
	count int64
}

// IncBy Increment counter by provided value.
func (a *AtomicCounter) IncBy(c int64) {
	atomic.AddInt64(&a.count, c)
}

// Set sets counter to value.
func (a *AtomicCounter) Set(n int64) {
	atomic.StoreInt64(&a.count, n)
}

// Swap attempts a compare and swap operation with counter.
func (a *AtomicCounter) Swap(n int64) {
	atomic.CompareAndSwapInt64(&a.count, a.count, n)
}

// GetDuration returns giving counter count value as a time.Duration
func (a *AtomicCounter) GetDuration() time.Duration {
	return time.Duration(atomic.LoadInt64(&a.count))
}

// Get returns giving counter count value.
func (a *AtomicCounter) Get() int64 {
	return atomic.LoadInt64(&a.count)
}

// Inc Increment counter by one.
func (a *AtomicCounter) Inc() {
	atomic.AddInt64(&a.count, 1)
}
