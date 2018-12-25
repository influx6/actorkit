package actorkit

//*****************************************************************
// DrainLog
//*****************************************************************

// DrainLog implements the actorkit.Logs interface.
type DrainLog struct{}

// Emit does nothing with provided arguments, it implements
// actorkit.Logs Emit method.
func (DrainLog) Emit(_ Level, _ LogEvent) {}

//*****************************************************************
// ContextLogFn
//*****************************************************************

// ContextLogFn implements the ContextLogs interface. It uses
// a provided function which returns a appropriate logger for
// a giving actor.
type ContextLogFn struct {
	Fn func(Actor) Logs
}

// NewContextLogFn returns a new instance of ContextLogFn
func NewContextLogFn(fn func(Actor) Logs) *ContextLogFn {
	return &ContextLogFn{Fn: fn}
}

// Get calls the underline function and returns the produced logger
// for the passed in actor.
func (c ContextLogFn) Get(a Actor) Logs {
	return c.Fn(a)
}
