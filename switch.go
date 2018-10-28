package actorkit

import (
	"sync/atomic"
)

const (
	on  uint64 = 1
	off uint64 = 0
)

//***********************************
//  WaiterImpl
//***********************************

// WaiterImpl implements the ErrorWaiter interface.
type WaiterImpl struct {
	err error
}

// NewWaiterImpl returns a new instance of WaiterImpl.
func NewWaiterImpl(err error) *WaiterImpl {
	return &WaiterImpl{err: err}
}

// Wait returns giving error associated with instance.
func (w *WaiterImpl) Wait() error {
	return w.err
}

//***********************************
//  SwitchImpl
//***********************************

// SwitchImpl implements a thread-safe switching mechanism, which
// swaps between a on and off state.
type SwitchImpl struct {
	state uint64
}

// IsOn returns true/false if giving switch is on.
// Must be called only.
func (s *SwitchImpl) IsOn() bool {
	return atomic.LoadUint64(&s.state) == on
}

// Off will flips switch into off state.
func (s *SwitchImpl) Off() {
	atomic.CompareAndSwapUint64(&s.state, on, off)
}

// On will flips switch into on state.
func (s *SwitchImpl) On() {
	atomic.CompareAndSwapUint64(&s.state, off, on)
}
