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
func (sp *AllForOneSupervisor) Handle(err interface{}, targetAddr Addr, target Actor, parent Actor) {
	var on allForOne
	on.max = sp.Max
	on.err = err
	on.actor = parent
	on.addr = targetAddr
	on.Decider = sp.Direction
	on.invoker = sp.Invoker
	go on.Handle()
}

type allForOne struct {
	max     int
	count   int
	addr    Addr
	actor   Actor
	err     interface{}
	Decider Direction
	invoker SupervisionInvoker
}

func (on *allForOne) Handle() {
	on.count++
	if on.count >= on.max {
		return
	}

	switch on.Decider(on.err) {
	case KillDirective:
		waiter := on.actor.Kill(nil)
		if on.invoker != nil {
			on.invoker.InvokedKill(on.err, on.addr, on.actor)
		}
		waiter.Wait()
	case StopDirective:
		waiter := on.actor.Stop(nil)
		if on.invoker != nil {
			on.invoker.InvokedStop(on.err, on.addr, on.actor)
		}
		waiter.Wait()
	case RestartDirective:
		waiter := on.actor.Restart(nil)
		if on.invoker != nil {
			on.invoker.InvokedRestart(on.err, Stat{Max: on.max, Count: on.count}, on.addr, on.actor)
		}

		if err := waiter.Wait(); err != nil {
			if errors.IsAny(err, ErrActorState) {
				return
			}
			on.Handle()
			return
		}
	case DestroyDirective:
		waiter := on.actor.Destroy(nil)
		if on.invoker != nil {
			on.invoker.InvokedDestroy(on.err, on.addr, on.actor)
		}
		waiter.Wait()
	case IgnoreDirective:
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
func (sp *OneForOneSupervisor) Handle(err interface{}, targetAddr Addr, target Actor, parent Actor) {
	var on oneForOne
	on.max = sp.Max
	on.err = err
	on.actor = target
	on.addr = targetAddr
	on.Decider = sp.Direction
	on.invoker = sp.Invoker
	go on.Handle()
}

type oneForOne struct {
	max     int
	count   int
	addr    Addr
	actor   Actor
	err     interface{}
	Decider Direction
	invoker SupervisionInvoker
}

func (on *oneForOne) Handle() {
	on.count++
	if on.count >= on.max {
		return
	}

	switch on.Decider(on.err) {
	case KillDirective:
		waiter := on.actor.Kill(nil)
		if on.invoker != nil {
			on.invoker.InvokedKill(on.err, on.addr, on.actor)
		}
		waiter.Wait()
	case StopDirective:
		waiter := on.actor.Stop(nil)
		if on.invoker != nil {
			on.invoker.InvokedStop(on.err, on.addr, on.actor)
		}
		waiter.Wait()
	case RestartDirective:
		waiter := on.actor.Restart(nil)
		if on.invoker != nil {
			on.invoker.InvokedRestart(on.err, Stat{Max: on.max, Count: on.count}, on.addr, on.actor)
		}

		if err := waiter.Wait(); err != nil {
			if errors.IsAny(err, ErrActorState) {
				return
			}
			on.Handle()
			return
		}
	case DestroyDirective:
		waiter := on.actor.Destroy(nil)
		if on.invoker != nil {
			on.invoker.InvokedDestroy(on.err, on.addr, on.actor)
		}
		waiter.Wait()
	case IgnoreDirective:
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
	waiter := target.Restart(nil)
	if sp.Invoker != nil {
		sp.Invoker.InvokedRestart(err, Stat{Count: 1}, targetAddr, target)
	}

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
		waiter := en.actor.Stop(nil)
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
		waiter := en.actor.Restart(nil)
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
