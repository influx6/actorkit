package actorkit

import (
	"sync"
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
	rm    sync.Mutex
	cond  *sync.Cond
	state bool
}

// NewSwitch returns a new instance of a SwitchImpl.
func NewSwitch() *SwitchImpl {
	var sw SwitchImpl
	sw.cond = sync.NewCond(&sw.rm)
	return &sw
}

// IsOn returns true/false if giving switch is on.
// Must be called only.
func (s *SwitchImpl) IsOn() bool {
	var state bool
	s.cond.L.Lock()
	state = s.state
	s.cond.L.Unlock()
	return state
}

// Wait blocks till it receives signal that the switch has
// changed state, this can be used to await switch change.
func (s *SwitchImpl) Wait() {
	s.cond.L.Lock()
	s.cond.Wait()
	s.cond.L.Unlock()
}

// Off will flips switch into off state.
func (s *SwitchImpl) Off() {
	s.cond.L.Lock()
	s.state = false
	s.cond.L.Unlock()
	s.cond.Broadcast()
}

// On will flips switch into on state.
func (s *SwitchImpl) On() {
	s.cond.L.Lock()
	s.state = true
	s.cond.L.Unlock()
	s.cond.Broadcast()
}
