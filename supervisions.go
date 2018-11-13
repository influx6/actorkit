package actorkit

import (
	"math/rand"
	"time"

	"github.com/gokit/errors"
)

//*****************************************************************
// AllForOneSupervisor
//*****************************************************************

// PanicAction defines a function type which embodies the action to
// be done with paniced value.
type PanicAction func(interface{}, Addr, Actor)

// AllForOneSupervisor implements a one-to-one supervising strategy for giving actors.
type AllForOneSupervisor struct {
	Max         int
	Direction   Direction
	PanicAction PanicAction
	Invoker     SupervisionInvoker
}

// Handle implements the Supervisor interface and provides the algorithm logic for the
// all-for-one monitoring strategy, where a failed actor causes the same effect to be applied
// to all siblings and parent.
func (on *AllForOneSupervisor) Handle(err interface{}, targetAddr Addr, target Actor, parent Actor) {
	stats := parent.Stats()
	switch on.Direction(err) {
	case PanicDirective:
		stats.Incr(PanicState)
		waiter := parent.Kill()
		if on.Invoker != nil {
			on.Invoker.InvokedKill(err, stats.Collate(), targetAddr, target)
		}
		waiter.Wait()

		if on.PanicAction != nil {
			on.PanicAction(err, targetAddr, target)
			return
		}

		if pe, ok := err.(ActorPanic); ok {
			panic(string(pe.Stack))
		}
	case KillDirective:
		stats.Incr(KillState)
		waiter := parent.Kill()
		if on.Invoker != nil {
			on.Invoker.InvokedKill(err, stats.Collate(), targetAddr, target)
		}
		waiter.Wait()
	case StopDirective:
		stats.Incr(StopState)
		waiter := parent.Stop()
		if on.Invoker != nil {
			on.Invoker.InvokedStop(err, stats.Collate(), targetAddr, target)
		}
		waiter.Wait()
	case RestartDirective:
		stats.Incr(RestartState)

		if on.Max > 0 && stats.Get(RestartFailureState) >= on.Max {
			return
		}

		waiter := parent.Restart()
		if on.Invoker != nil {
			on.Invoker.InvokedRestart(err, stats.Collate(), targetAddr, target)
		}

		if err := waiter.Wait(); err != nil {
			if errors.IsAny(err, ErrActorState) {
				return
			}

			stats.Incr(RestartFailureState)
			on.Handle(err, targetAddr, target, parent)
			return
		}
		stats.Reset(RestartFailureState)
	case DestroyDirective:
		stats.Incr(DestroyState)
		waiter := parent.Destroy()
		if on.Invoker != nil {
			on.Invoker.InvokedDestroy(err, stats.Collate(), targetAddr, target)
		}
		waiter.Wait()
	case EscalateDirective:
		stats.Incr(EscalatedState)
		parent.Escalate(err, targetAddr)
	case IgnoreDirective:
		stats.Incr(ErrorState)
		return
	}
}

//*****************************************************************
// OneForOneSupervisor
//*****************************************************************

// Direction defines a function which giving a value will return a directive.
type Direction func(interface{}) Directive

// OneForOneSupervisor implements a one-to-one supervising strategy for giving actors.
type OneForOneSupervisor struct {
	Max         int
	Direction   Direction
	PanicAction PanicAction
	Invoker     SupervisionInvoker
}

// Handle implements the Supervisor interface and provides the algorithm logic for the
// one-for-one monitoring strategy, where a failed actor is dealt with singularly without affecting
// it's siblings.
func (on *OneForOneSupervisor) Handle(err interface{}, targetAddr Addr, target Actor, parent Actor) {
	stats := target.Stats()
	switch on.Direction(err) {
	case PanicDirective:
		stats.Incr(PanicState)
		waiter := target.Kill()
		if on.Invoker != nil {
			on.Invoker.InvokedKill(err, stats.Collate(), targetAddr, target)
		}
		waiter.Wait()

		if on.PanicAction != nil {
			on.PanicAction(err, targetAddr, target)
			return
		}

		if pe, ok := err.(ActorPanic); ok {
			panic(string(pe.Stack))
		}
	case KillDirective:
		stats.Incr(KillState)
		waiter := target.Kill()
		if on.Invoker != nil {
			on.Invoker.InvokedKill(err, stats.Collate(), targetAddr, target)
		}
		waiter.Wait()
	case StopDirective:
		stats.Incr(StopState)
		waiter := target.Stop()
		if on.Invoker != nil {
			on.Invoker.InvokedStop(err, stats.Collate(), targetAddr, target)
		}
		waiter.Wait()
	case RestartDirective:
		stats.Incr(RestartState)

		// if we have surpassed maximum allowed restarts, kill actor.
		if on.Max > 0 && stats.Get(RestartFailureState) >= on.Max {
			target.Kill().Wait()
			return
		}

		waiter := target.Restart()
		if on.Invoker != nil {
			on.Invoker.InvokedRestart(err, stats.Collate(), targetAddr, target)
		}

		if err := waiter.Wait(); err != nil {
			if errors.IsAny(err, ErrActorState) {
				return
			}

			stats.Incr(RestartFailureState)
			on.Handle(err, targetAddr, target, parent)
			return
		}

		stats.Reset(RestartFailureState)
	case DestroyDirective:
		stats.Incr(DestroyState)
		waiter := target.Destroy()
		if on.Invoker != nil {
			on.Invoker.InvokedDestroy(err, stats.Collate(), targetAddr, target)
		}
		waiter.Wait()
	case EscalateDirective:
		stats.Incr(EscalatedState)
		parent.Escalate(err, targetAddr)
	case IgnoreDirective:
		stats.Incr(ErrorState)
		return
	}
}

//*****************************************************************
// RestartingSupervisor
//*****************************************************************

// RestartingSupervisor implements a one-to-one supervising strategy for giving actors.
type RestartingSupervisor struct {
	Invoker SupervisionInvoker
}

// Handle implements a restarting supervision strategy where any escalated error will lead to
// a restart of actor.
func (sp *RestartingSupervisor) Handle(err interface{}, targetAddr Addr, target Actor, parent Actor) {
	stats := target.Stats()

	waiter := target.Restart()
	if sp.Invoker != nil {
		sp.Invoker.InvokedRestart(err, stats.Collate(), targetAddr, target)
	}

	stats.Incr(RestartState)

	if err := waiter.Wait(); err != nil {
		if errors.IsAny(err, ErrActorState) {
			return
		}

		stats.Incr(RestartFailureState)
		target.Escalate(err, targetAddr)
	}
	stats.Reset(RestartFailureState)
}

//*****************************************************************
// ExponentialBackOffStrategy
//*****************************************************************

// ExponentialBackOffSupervisor implements a
type ExponentialBackOffSupervisor struct {
	Max     int
	Backoff time.Duration
	Invoker SupervisionInvoker
}

// Handle implements the exponential restart of giving target actor within giving maximum allowed runs.
func (sp *ExponentialBackOffSupervisor) Handle(err interface{}, targetAddr Addr, target Actor, parent Actor) {
	stats := target.Stats()

	if stats.Get(RestartState) >= sp.Max {
		stats.Incr(StopState)

		waiter := target.Stop()
		if sp.Invoker != nil {
			sp.Invoker.InvokedStop(err, stats.Collate(), targetAddr, target)
		}
		waiter.Wait()
		return
	}

	backoff := stats.Get(RestartState) * int(sp.Backoff.Nanoseconds())
	noise := rand.Intn(500)
	dur := time.Duration(backoff + noise)

	time.AfterFunc(dur, func() {
		stats.Incr(RestartState)

		waiter := target.Restart()
		if sp.Invoker != nil {
			sp.Invoker.InvokedRestart(err, stats.Collate(), targetAddr, target)
		}

		if err := waiter.Wait(); err != nil {
			if errors.IsAny(err, ErrActorState) {
				return
			}

			stats.Incr(RestartFailureState)
			sp.Handle(err, targetAddr, target, parent)
		}

		stats.Reset(RestartFailureState)
	})
}
