package actorkit

import (
	"math/rand"
	"time"

	"github.com/gokit/errors"
)

//*****************************************************************
// AllForOneSupervisor
//*****************************************************************

// AllForOneSupervisor implements a one-to-one supervising strategy for giving actors.
type AllForOneSupervisor struct {
	Max       int
	Direction Direction
	Invoker   SupervisionInvoker
}

// Handle implements the Supervisor interface and provides the algorithm logic for the
// all-for-one monitoring strategy, where a failed actor causes the same effect to be applied
// to all siblings and parent.
func (on *AllForOneSupervisor) Handle(err interface{}, targetAddr Addr, target Actor, parent Actor) {
	stats := parent.ActorStat()
	switch on.Direction(err) {
	case PanicDirective:
		stats.Incr(PanicState)
		waiter := parent.Kill()
		if on.Invoker != nil {
			on.Invoker.InvokedKill(err, targetAddr, target)
		}
		waiter.Wait()

		if pe, ok := err.(ActorPanic); ok {
			panic(string(pe.Stack))
		}
	case KillDirective:
		stats.Incr(KillState)
		waiter := parent.Kill()
		if on.Invoker != nil {
			on.Invoker.InvokedKill(err, targetAddr, target)
		}
		waiter.Wait()
	case StopDirective:
		stats.Incr(StopState)
		waiter := parent.Stop()
		if on.Invoker != nil {
			on.Invoker.InvokedStop(err, targetAddr, target)
		}
		waiter.Wait()
	case RestartDirective:
		stats.Incr(RestartState)

		if stats.Get(RestartState) >= on.Max {
			return
		}

		waiter := parent.Restart()
		if on.Invoker != nil {
			on.Invoker.InvokedRestart(err, Stat{Max: on.Max, Count: stats.Get(RestartState)}, targetAddr, target)
		}

		if err := waiter.Wait(); err != nil {
			if errors.IsAny(err, ErrActorState) {
				return
			}

			on.Handle(err, targetAddr, target, parent)
			return
		}
	case DestroyDirective:
		stats.Incr(DestroyState)
		waiter := parent.Destroy()
		if on.Invoker != nil {
			on.Invoker.InvokedDestroy(err, targetAddr, target)
		}
		waiter.Wait()
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
	Max       int
	Direction Direction
	Invoker   SupervisionInvoker
}

// Handle implements the Supervisor interface and provides the algorithm logic for the
// one-for-one monitoring strategy, where a failed actor is dealt with singularly without affecting
// it's siblings.
func (on *OneForOneSupervisor) Handle(err interface{}, targetAddr Addr, target Actor, parent Actor) {
	stats := target.ActorStat()
	switch on.Direction(err) {
	case PanicDirective:
		stats.Incr(PanicState)
		waiter := target.Kill()
		if on.Invoker != nil {
			on.Invoker.InvokedKill(err, targetAddr, target)
		}
		waiter.Wait()

		if pe, ok := err.(ActorPanic); ok {
			panic(string(pe.Stack))
		}
	case KillDirective:
		stats.Incr(KillState)
		waiter := target.Kill()
		if on.Invoker != nil {
			on.Invoker.InvokedKill(err, targetAddr, target)
		}
		waiter.Wait()
	case StopDirective:
		stats.Incr(StopState)
		waiter := target.Stop()
		if on.Invoker != nil {
			on.Invoker.InvokedStop(err, targetAddr, target)
		}
		waiter.Wait()
	case RestartDirective:
		stats.Incr(RestartState)

		// if we have surpassed maximum allowed restarts, kill actor.
		if stats.Get(RestartState) >= on.Max {
			target.Kill().Wait()
			return
		}

		waiter := target.Restart()
		if on.Invoker != nil {
			on.Invoker.InvokedRestart(err, Stat{Max: on.Max, Count: stats.Get(RestartState)}, targetAddr, target)
		}

		if err := waiter.Wait(); err != nil {
			if errors.IsAny(err, ErrActorState) {
				return
			}

			on.Handle(err, targetAddr, target, parent)
			return
		}

		stats.Reset(RestartState)
	case DestroyDirective:
		stats.Incr(DestroyState)
		waiter := target.Destroy()
		if on.Invoker != nil {
			on.Invoker.InvokedDestroy(err, targetAddr, target)
		}
		waiter.Wait()
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
	stats := target.ActorStat()

	waiter := target.Restart()
	if sp.Invoker != nil {
		sp.Invoker.InvokedRestart(err, Stat{Count: 1}, targetAddr, target)
	}

	stats.Incr(RestartState)

	if err := waiter.Wait(); err != nil {
		if errors.IsAny(err, ErrActorState) {
			return
		}
		target.Escalate(err, targetAddr)
	}
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
	var expo exponentialStrategy
	expo.err = err
	expo.actor = target
	expo.addr = targetAddr
	expo.invoker = sp.Invoker
	expo.stat.Max = sp.Max
	expo.stat.Backoff = sp.Backoff

	go expo.Handle()
}

type exponentialStrategy struct {
	addr    Addr
	actor   Actor
	stat    Stat
	err     interface{}
	invoker SupervisionInvoker
}

func (en *exponentialStrategy) Handle() {
	en.stat.Count++
	if en.stat.Count >= en.stat.Max {
		waiter := en.actor.Stop()
		if en.invoker != nil {
			en.invoker.InvokedStop(en.err, en.addr, en.actor)
		}
		waiter.Wait()
		return
	}

	backoff := en.stat.Count * int(en.stat.Backoff.Nanoseconds())
	noise := rand.Intn(500)
	dur := time.Duration(backoff + noise)

	time.AfterFunc(dur, func() {
		waiter := en.actor.Restart()
		if en.invoker != nil {
			en.invoker.InvokedRestart(en.err, en.stat, en.addr, en.actor)
		}

		if err := waiter.Wait(); err != nil {
			if errors.IsAny(err, ErrActorState) {
				return
			}
			en.Handle()
		}
	})
}
