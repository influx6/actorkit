package actorkit

import (
	"sync/atomic"
)

const (
	on  uint64 = 1
	off uint64 = 0
)

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
