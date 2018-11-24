package actorkit

import (
	"context"
	"time"

	"github.com/gokit/errors"
)

var (
	// ErrOpenedCircuit is returned when circuit breaker is in opened state.
	ErrOpenedCircuit = errors.New("Circuit is opened")

	// ErrOpAfterTimeout is returned when operation call executes longer than
	// timeout duration.
	ErrOpAfterTimeout = errors.New("operation finished after timeout")
)

//***********************************************************
// CircuitBreaker
//***********************************************************

// Circuit defines configuration values which will be used
// by CircuitBreaker for it's operations.
type Circuit struct {
	// Timeout sets giving timeout duration for execution of
	// giving operation.
	Timeout time.Duration

	// MaxFailures sets giving maximum failure threshold allowed
	// before circuit enters open state.
	MaxFailures int64

	// HalfOpenSuccess sets giving minimum successfully calls to
	// circuit operation before entering closed state.
	//
	// Defaults to 1
	HalfOpenSuccess int64

	// MinCoolDown sets minimum time for circuit to be in open state
	// before we allow another attempt into half open state.
	//
	// Defaults to 15 seconds.
	MinCoolDown time.Duration

	// Maximum time to allow circuit to be in open state before
	// allowing another attempt.
	//
	// Defaults to 60 seconds.
	MaxCoolDown time.Duration

	// Now provides a function which can be used to provide
	// the next time (time.Time).
	//
	// Defaults to time.Now().
	Now func() time.Time

	// CanTrigger defines a function to be called to verify if
	// giving error falls under errors that count against
	// the circuit breaker, incrementing failure and can cause
	// circuit tripping.
	//
	// Defaults to a function that always returns true.
	CanTrigger func(error) bool

	// OnTrip sets giving callback to be called every time circuit
	// is tripped into open state.
	OnTrip func(name string, lastError error)

	// OnClose sets giving callback to be called on when
	// circuit entering closed state.
	OnClose func(name string, lastCoolDown time.Duration)

	// OnRun sets giving callback to be called on when
	// circuit is executed with function it is provided
	// with start, end time of function and possible error
	// that occurred either from function or due to time out.
	OnRun func(name string, start time.Time, end time.Time, err error)

	// OnHalfOpen sets giving callback to be called every time
	// circuit enters half open state.
	OnHalfOpen func(name string, lastCoolDown time.Duration, lastOpenedTime time.Time)
}

func (cb *Circuit) init() {
	if cb.MaxFailures <= 0 {
		cb.MaxFailures = 5
	}

	if cb.Now == nil {
		cb.Now = time.Now
	}

	if cb.HalfOpenSuccess <= 0 {
		cb.HalfOpenSuccess = 1
	}

	if cb.MinCoolDown <= 0 {
		cb.MaxCoolDown = 15 * time.Second
	}

	if cb.MaxCoolDown <= 0 {
		cb.MaxCoolDown = 60 * time.Second
	}

	if cb.CanTrigger == nil {
		cb.CanTrigger = func(e error) bool {
			return true
		}
	}
}

//***********************************************************
// CircuitBreaker
//***********************************************************

// CircuitBreaker implements the CircuitBreaker pattern for use
// within actor project.
type CircuitBreaker struct {
	name         string
	circuit      Circuit
	lastOpened   time.Time
	nextCoolDown AtomicCounter

	isOpened        AtomicBool
	currentFailures AtomicCounter

	isHalfOpened            AtomicBool
	halfOpenedPasses        AtomicCounter
	currentHalfOpenFailures AtomicCounter
}

// NewCircuitBreaker returns a new instance of CircuitBreaker.
func NewCircuitBreaker(name string, circuit Circuit) *CircuitBreaker {
	circuit.init()

	return &CircuitBreaker{
		name:    name,
		circuit: circuit,
	}
}

// IsOpened returns true/false if circuit is in opened state.
func (dm *CircuitBreaker) IsOpened() bool {
	return dm.isOpened.IsTrue()
}

// Do will attempt to execute giving function with a timed function if CircuitBreaker provides
// a timeout.
//
// But the fallback if provided will be executed on the following rules:
//
// 1. The circuit is already opened, hence receiving a ErrOpenedCircuit error.
//
// 2. The function failed during execution with an error, which increases failed count and
// forces calling of fallback with received error.
//
func (dm *CircuitBreaker) Do(parentCtx context.Context, fn func(ctx context.Context) error, fallback func(context.Context, error) error) error {
	var cancel func()
	var ctx context.Context

	if dm.circuit.Timeout > 0 {
		ctx, cancel = context.WithTimeout(parentCtx, dm.circuit.Timeout)
	} else {
		ctx, cancel = context.WithCancel(parentCtx)
	}

	if !dm.shouldTry() {
		if fallback == nil {
			cancel()
			return errors.WrapOnly(ErrOpenedCircuit)
		}

		return func() error {
			defer cancel()
			return fallback(ctx, ErrOpenedCircuit)
		}()
	}

	if runErr := dm.run(ctx, parentCtx, fn); runErr != nil {
		if fallback != nil {
			return fallback(ctx, runErr)
		}
		return runErr
	}

	return nil
}

func (dm *CircuitBreaker) run(ctx context.Context, parentCtx context.Context, fn func(context.Context) error) error {
	// starting time.
	start := dm.circuit.Now()

	// run the giving function with context.
	runErr := fn(ctx)

	// end time for call.
	end := dm.circuit.Now()

	// get duration of call.
	elapsed := end.Sub(start)

	if runErr != nil {
		if dm.circuit.OnRun != nil {
			dm.circuit.OnRun(dm.name, start, end, runErr)
		}

		// if we have an error, then register error.
		if dm.isOpened.IsTrue() {
			dm.recordHalfOpenFailure(runErr)
		} else {
			dm.recordFailure(runErr)
		}

		return runErr
	}

	// if we were cancelled by parent timeout, then we do not
	// need to consider this a possible failure, as it was the parent that
	// request a immediate stop, we can simply ignore and let another retry.
	if perr := parentCtx.Err(); perr != nil && perr == context.DeadlineExceeded {
		if dm.circuit.OnRun != nil {
			dm.circuit.OnRun(dm.name, start, end, nil)
		}
		return nil
	}

	// run OnRun with no error
	if elapsed > dm.circuit.Timeout {
		if dm.circuit.OnRun != nil {
			dm.circuit.OnRun(dm.name, start, end, ErrOpAfterTimeout)
		}

		if dm.isOpened.IsTrue() {
			dm.recordHalfOpenFailure(ErrOpAfterTimeout)
		} else {
			dm.recordFailure(ErrOpAfterTimeout)
		}

		return errors.WrapOnly(ErrOpAfterTimeout)
	}

	if dm.isOpened.IsTrue() {
		dm.recordHalfOpenSuccess()
	}

	if dm.circuit.OnRun != nil {
		dm.circuit.OnRun(dm.name, start, end, nil)
	}

	return nil
}

func (dm *CircuitBreaker) recordFailure(err error) {
	// If we have a non-nil error value, should this be
	// something that can trigger us?
	if dm.circuit.CanTrigger != nil {
		// is this a error which can not trigger our failure, then skip.
		if !dm.circuit.CanTrigger(err) {
			return
		}
	}

	dm.currentFailures.Inc()

	// if we have maxed possible failures then put us into open state.
	if dm.currentFailures.Get() >= dm.circuit.MaxFailures {
		dm.isOpened.On()
		dm.halfOpenedPasses.Set(0)
		dm.lastOpened = dm.circuit.Now()
		dm.currentHalfOpenFailures.Set(0)
		dm.nextCoolDown.Set(dm.circuit.MinCoolDown.Nanoseconds())
		if dm.circuit.OnTrip != nil {
			dm.circuit.OnTrip(dm.name, err)
		}
	}
}

func (dm *CircuitBreaker) recordHalfOpenSuccess() {
	dm.halfOpenedPasses.Inc()
	if dm.halfOpenedPasses.Get() >= dm.circuit.HalfOpenSuccess {
		dm.isOpened.Off()

		if dm.circuit.OnClose != nil {
			dm.circuit.OnClose(dm.name, dm.nextCoolDown.GetDuration())
		}

		dm.currentFailures.Set(0)
		dm.halfOpenedPasses.Set(0)
		dm.currentHalfOpenFailures.Set(0)
		dm.nextCoolDown.Set(dm.circuit.MinCoolDown.Nanoseconds())
	}
}

func (dm *CircuitBreaker) recordHalfOpenFailure(err error) {
	// If we have a non-nil error value, should this be
	// something that can trigger us?
	if dm.circuit.CanTrigger != nil {
		// is this a error which can not trigger our failure, then skip.
		if !dm.circuit.CanTrigger(err) {
			return
		}
	}

	// increment half opened count.
	dm.currentHalfOpenFailures.Inc()

	// update last opened timestamp.
	dm.lastOpened = dm.circuit.Now()

	// increment next cool down time.
	nextCoolDown := dm.circuit.MinCoolDown * dm.currentHalfOpenFailures.GetDuration()
	dm.nextCoolDown.Set(nextCoolDown.Nanoseconds())

	if dm.circuit.OnTrip != nil {
		dm.circuit.OnTrip(dm.name, err)
	}
}

func (dm *CircuitBreaker) shouldTry() bool {
	if !dm.isOpened.IsTrue() {
		return true
	}

	past := dm.circuit.Now().Sub(dm.lastOpened)
	nextCool := dm.nextCoolDown.GetDuration()

	// if we have reached or maxed current next cool down,
	// enter half opened state, as we should be opened.
	if past >= nextCool {

		// Trigger update for half opened state.
		if dm.circuit.OnHalfOpen != nil {
			dm.circuit.OnHalfOpen(dm.name, nextCool, dm.lastOpened)
		}

		dm.lastOpened = dm.circuit.Now()
		return true
	}

	return false
}

//***********************************************************
// Delivery Circuits
//***********************************************************

// CircuitAddr implements a circuit breaker Addr wrapper, which will
// implement a circuit breaker pattern on message delivery to a giving
// origin address. If giving address fails to accept messages over a certain
// period, this will count till a threshold is met, then all messages will be
// declined, till the circuit has reached
type CircuitAddr struct {
	addr Addr

	// circuit is the underline circuit breaker to be used for
	// giving address.
	circuit *CircuitBreaker

	// Fallback defines function to be called as fallback
	// when giving envelope could not be delivered to underline
	// address.
	fallback func(error, Envelope) error
}

// NewCircuitAddr returns a new instance of a CircuitAddr.
func NewCircuitAddr(addr Addr, circuit Circuit, fallback func(error, Envelope) error) *CircuitAddr {
	return &CircuitAddr{
		addr:     addr,
		fallback: fallback,
		circuit:  NewCircuitBreaker(addr.Addr(), circuit),
	}
}

// Forward attempts to forward giving envelope to underline address.
// It returns an error if giving circuit is opened, hence passing
// envelope to fallback if provided.
func (dm *CircuitAddr) Forward(env Envelope) error {
	return dm.circuit.Do(context.Background(), func(_ context.Context) error {
		return dm.addr.Forward(env)
	}, func(i context.Context, err error) error {
		if dm.fallback == nil {
			return err
		}
		return dm.fallback(err, env)
	})
}

// Send delivers giving data as a envelope to provided underline address.
// It returns an error if giving circuit is opened, hence passing
// envelope to fallback if provided.
func (dm *CircuitAddr) Send(data interface{}, addr Addr) error {
	return dm.circuit.Do(context.Background(), func(_ context.Context) error {
		return dm.addr.Send(data, addr)
	}, func(i context.Context, err error) error {
		if dm.fallback == nil {
			return err
		}
		return dm.fallback(err, CreateEnvelope(addr, Header{}, data))
	})
}

// SendWithHeader delivers data as a enveloped with attached headers to underline
// address.
// It returns an error if giving circuit is opened, hence passing
// envelope to fallback if provided.
func (dm *CircuitAddr) SendWithHeader(data interface{}, h Header, addr Addr) error {
	return dm.circuit.Do(context.Background(), func(_ context.Context) error {
		return dm.addr.SendWithHeader(data, h, addr)
	}, func(i context.Context, err error) error {
		if dm.fallback == nil {
			return err
		}
		return dm.fallback(err, CreateEnvelope(addr, h, data))
	})
}

//**********************************************************
// BehaviourCircuit
//**********************************************************

// BehaviourCircuit implements the circuit breaker pattern for
// execution of a implementer of the ErrorBehaviour interface which
// returns errors for the execution of a operation.
//
// Usually this is suitable if the implementer only ever performs
// tasks are that very similar which can then be treated as the same
// or a implementer that ever works on the same type of task every time
// as the breaker once tripped will ignore all messages without a
// care for it's type.
//
type BehaviourCircuit struct {
	behaviour ErrorBehaviour
	circuit   *CircuitBreaker
	fallback  func(Addr, Envelope) error
}

// Action implements the Behaviour interface.
func (bc *BehaviourCircuit) Action(addr Addr, msg Envelope) {
	bc.circuit.Do(context.Background(), func(ctx context.Context) error {
		return bc.behaviour.Action(addr, msg)
	}, func(ctx context.Context, err error) error {
		if bc.fallback == nil {
			return err
		}
		return bc.fallback(addr, msg)
	})
}
