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
// be done with panic'ed value.
type PanicAction func(interface{}, Addr, Actor)

// AllForOneSupervisor implements a one-to-one supervising strategy for giving actors.
type AllForOneSupervisor struct {
	Max         int
	Direction   Direction
	PanicAction PanicAction
	Invoker     SupervisionInvoker

	failedRestarts int64
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
		linearDoUntil(parent.KillChildren, 100, time.Second)

		if on.Invoker != nil {
			on.Invoker.InvokedKill(err, target.Stats(), targetAddr, target)
		}

		if on.PanicAction != nil {
			on.PanicAction(err, targetAddr, target)
			return
		}

		switch tm := err.(type) {
		case ActorPanic:
			panic(tm.Error())
		case ActorRoutineError:
			panic(tm.Error())
		case ActorRoutinePanic:
			panic(tm.Error())
		default:
			panic(err)
		}
	case KillDirective:
		linearDoUntil(parent.KillChildren, 100, time.Second)
		if on.Invoker != nil {
			on.Invoker.InvokedKill(err, target.Stats(), targetAddr, target)
		}
	case StopDirective:
		linearDoUntil(parent.StopChildren, 100, time.Second)
		if on.Invoker != nil {
			on.Invoker.InvokedStop(err, target.Stats(), targetAddr, target)
		}
	case RestartDirective:
		failed := int(atomic.LoadInt64(&on.failedRestarts))
		if on.Max > 0 && failed >= on.Max {
			return
		}

		restartErr := target.Restart()
		if on.Invoker != nil {
			on.Invoker.InvokedRestart(err, target.Stats(), targetAddr, target)
		}

		if restartErr != nil {
			atomic.AddInt64(&on.failedRestarts, 1)
			on.Handle(err, targetAddr, target, parent)
			return
		}

		atomic.StoreInt64(&on.failedRestarts, 0)
	case DestroyDirective:
		linearDoUntil(parent.DestroyChildren, 100, time.Second)

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

	failedRestarts int64
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
		linearDoUntil(target.Kill, 100, time.Second)

		if on.Invoker != nil {
			on.Invoker.InvokedKill(err, target.Stats(), targetAddr, target)
		}

		if on.PanicAction != nil {
			on.PanicAction(err, targetAddr, target)
			return
		}

		switch tm := err.(type) {
		case ActorPanic:
			panic(tm.Error())
		case ActorRoutineError:
			panic(tm.Error())
		case ActorRoutinePanic:
			panic(tm.Error())
		default:
			panic(err)
		}
	case KillDirective:
		linearDoUntil(target.Kill, 100, time.Second)
		if on.Invoker != nil {
			on.Invoker.InvokedKill(err, target.Stats(), targetAddr, target)
		}
	case StopDirective:
		linearDoUntil(target.Stop, 100, time.Second)
		if on.Invoker != nil {
			on.Invoker.InvokedStop(err, target.Stats(), targetAddr, target)
		}
	case RestartDirective:
		failed := int(atomic.LoadInt64(&on.failedRestarts))
		if on.Max > 0 && failed >= on.Max {
			return
		}

		restartErr := target.Restart()
		if on.Invoker != nil {
			on.Invoker.InvokedRestart(err, target.Stats(), targetAddr, target)
		}

		if restartErr != nil {
			on.failedRestarts++
			atomic.AddInt64(&on.failedRestarts, 1)
			on.Handle(err, targetAddr, target, parent)
			return
		}

		atomic.StoreInt64(&on.failedRestarts, 0)
	case DestroyDirective:
		linearDoUntil(target.Destroy, 100, time.Second)

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
// ExponentialBackOffRestartStrategy
//*****************************************************************

// ExponentialBackOffRestartStrategy returns a new ExponentialBackOffSupervisor which will attempt to restart target actor
// where error occurred. If restart fail, it will continuously attempt till it has maxed out chances.
func ExponentialBackOffRestartStrategy(max int, backoff time.Duration, invoker SupervisionInvoker) *ExponentialBackOffSupervisor {
	return &ExponentialBackOffSupervisor{
		Max:     max,
		Backoff: backoff,
		Invoker: invoker,
		Action: func(err interface{}, targetAddr Addr, target Actor, parent Actor) error {
			return target.Restart()
		},
	}
}

// ExponentialBackOffStopStrategy returns a new ExponentialBackOffSupervisor which will attempt to stop target actor
// where error occurred. If restart fail, it will continuously attempt till it has maxed out chances.
func ExponentialBackOffStopStrategy(max int, backoff time.Duration, invoker SupervisionInvoker) *ExponentialBackOffSupervisor {
	return &ExponentialBackOffSupervisor{
		Max:     max,
		Backoff: backoff,
		Invoker: invoker,
		Action: func(err interface{}, targetAddr Addr, target Actor, parent Actor) error {
			return target.Stop()
		},
	}
}

//*****************************************************************
// ExponentialBackOffStrategy
//*****************************************************************

// ExponentialBackOffSupervisor implements a supervisor which will attempt to
// exponentially run a giving action function continuously with an increasing
// backoff time, until it's maximum tries is reached.
type ExponentialBackOffSupervisor struct {
	Max     int
	Backoff time.Duration
	Invoker SupervisionInvoker
	Action  func(err interface{}, targetAddr Addr, target Actor, parent Actor) error

	failed int64
	work   sync.Mutex
}

// Handle implements the exponential restart of giving target actor within giving maximum allowed runs.
func (sp *ExponentialBackOffSupervisor) Handle(err interface{}, targetAddr Addr, target Actor, parent Actor) {
	sp.work.Lock()
	defer sp.work.Unlock()

	failed := atomic.LoadInt64(&sp.failed)
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
		actionErr := sp.Action(err, targetAddr, target, parent)
		if sp.Invoker != nil {
			sp.Invoker.InvokedRestart(err, target.Stats(), targetAddr, target)
		}

		if actionErr != nil {
			atomic.AddInt64(&sp.failed, 1)
			sp.Handle(err, targetAddr, target, parent)
			return
		}

		atomic.StoreInt64(&sp.failed, 0)
	})
}
