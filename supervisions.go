package actorkit

import (
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
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

	failedRestarts int
	work           sync.Mutex
}

// Handle implements the Supervisor interface and provides the algorithm logic for the
// all-for-one monitoring strategy, where a failed actor causes the same effect to be applied
// to all siblings and parent.
func (on *AllForOneSupervisor) Handle(err interface{}, targetAddr Addr, target Actor, parent Actor) {
	on.work.Lock()
	defer on.work.Unlock()

	switch on.Direction(err) {
	case PanicDirective:
		parent.Kill()

		if on.Invoker != nil {
			on.Invoker.InvokedKill(err, target.Stats(), targetAddr, target)
		}

		if on.PanicAction != nil {
			on.PanicAction(err, targetAddr, target)
			return
		}

		if pe, ok := err.(ActorPanic); ok {
			panic(string(pe.Stack))
		}
	case KillDirective:
		parent.Kill()
		if on.Invoker != nil {
			on.Invoker.InvokedKill(err, target.Stats(), targetAddr, target)
		}
	case StopDirective:
		parent.Stop()
		if on.Invoker != nil {
			on.Invoker.InvokedStop(err, target.Stats(), targetAddr, target)
		}
	case RestartDirective:
		if on.Max > 0 && on.failedRestarts >= on.Max {
			return
		}

		restartErr := parent.Restart()
		if on.Invoker != nil {
			on.Invoker.InvokedRestart(err, target.Stats(), targetAddr, target)
		}

		if restartErr != nil {
			on.failedRestarts++
			on.Handle(err, targetAddr, target, parent)
			return
		}

		on.failedRestarts = 0
	case DestroyDirective:
		parent.Destroy()

		if on.Invoker != nil {
			on.Invoker.InvokedDestroy(err, target.Stats(), targetAddr, target)
		}
	case EscalateDirective:
		parent.Escalate(err, targetAddr)
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
	Max         int
	Direction   Direction
	PanicAction PanicAction
	Invoker     SupervisionInvoker

	failedRestarts int
	work           sync.Mutex
}

// Handle implements the Supervisor interface and provides the algorithm logic for the
// one-for-one monitoring strategy, where a failed actor is dealt with singularly without affecting
// it's siblings.
func (on *OneForOneSupervisor) Handle(err interface{}, targetAddr Addr, target Actor, parent Actor) {
	on.work.Lock()
	defer on.work.Unlock()

	switch on.Direction(err) {
	case PanicDirective:
		target.Kill()

		if on.Invoker != nil {
			on.Invoker.InvokedKill(err, target.Stats(), targetAddr, target)
		}

		if on.PanicAction != nil {
			on.PanicAction(err, targetAddr, target)
			return
		}

		switch tm := err.(type) {
		case ActorPanic:
			panic(string(tm.Stack))
		case ActorRoutinePanic:
			panic(string(tm.Stack))
		default:
			panic(err)
		}
	case KillDirective:
		target.Kill()
		if on.Invoker != nil {
			on.Invoker.InvokedKill(err, target.Stats(), targetAddr, target)
		}
	case StopDirective:
		target.Stop()
		if on.Invoker != nil {
			on.Invoker.InvokedStop(err, target.Stats(), targetAddr, target)
		}
	case RestartDirective:
		if on.Max > 0 && on.failedRestarts >= on.Max {
			return
		}

		restartErr := target.Restart()
		if on.Invoker != nil {
			on.Invoker.InvokedRestart(err, target.Stats(), targetAddr, target)
		}

		if restartErr != nil {
			on.failedRestarts++
			on.Handle(err, targetAddr, target, parent)
			return
		}

		on.failedRestarts = 0
	case DestroyDirective:
		target.Destroy()

		if on.Invoker != nil {
			on.Invoker.InvokedDestroy(err, target.Stats(), targetAddr, target)
		}
	case EscalateDirective:
		parent.Escalate(err, targetAddr)
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
	work    sync.Mutex
}

// Handle implements a restarting supervision strategy where any escalated error will lead to
// a restart of actor.
func (sp *RestartingSupervisor) Handle(err interface{}, targetAddr Addr, target Actor, parent Actor) {
	sp.work.Lock()
	defer sp.work.Unlock()

	restartErr := target.Restart()
	if sp.Invoker != nil {
		sp.Invoker.InvokedRestart(err, target.Stats(), targetAddr, target)
	}

	if restartErr == nil {
		return
	}

	sp.Handle(err, targetAddr, target, parent)
}

//*****************************************************************
// ExponentialBackOffStrategy
//*****************************************************************

// ExponentialBackOffSupervisor implements a
type ExponentialBackOffSupervisor struct {
	Max     int
	Backoff time.Duration
	Invoker SupervisionInvoker

	failedRestart int64
	work          sync.Mutex
}

// Handle implements the exponential restart of giving target actor within giving maximum allowed runs.
func (sp *ExponentialBackOffSupervisor) Handle(err interface{}, targetAddr Addr, target Actor, parent Actor) {
	sp.work.Lock()
	defer sp.work.Unlock()

	failed := atomic.LoadInt64(&sp.failedRestart)
	if int(failed) >= sp.Max {
		target.Stop()
		if sp.Invoker != nil {
			sp.Invoker.InvokedStop(err, target.Stats(), targetAddr, target)
		}

		return
	}

	var backoff int64
	if failed > 0 {
		backoff = failed * sp.Backoff.Nanoseconds()
	} else {
		backoff = sp.Backoff.Nanoseconds()
	}

	noise := rand.Int63n(500)
	dur := time.Duration(backoff + noise)

	time.AfterFunc(dur, func() {
		restartErr := target.Restart()
		if sp.Invoker != nil {
			sp.Invoker.InvokedRestart(err, target.Stats(), targetAddr, target)
		}

		if restartErr != nil {
			sp.failedRestart++
			sp.Handle(err, targetAddr, target, parent)
			return
		}

		atomic.AddInt64(&sp.failedRestart, 1)
	})
}
