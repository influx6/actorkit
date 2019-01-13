package actorkit

//***************************************************************************
// LogEvent
//***************************************************************************

// Level defines different level warnings for giving
// log events.
type Level uint8

// constants of log levels this package respect.
// They are capitalize to ensure no naming conflict.
const (
	INFO Level = 1 << iota
	DEBUG
	WARN
	ERROR
	PANIC
)

// String implements the Stringer interface.
func (l Level) String() string {
	switch l {
	case INFO:
		return "INFO"
	case ERROR:
		return "ERROR"
	case DEBUG:
		return "DEBUG"
	case WARN:
		return "WARN"
	case PANIC:
		return "PANIC"
	}
	return "UNKNOWN"
}

// LogMessage defines an interface which exposes a method for retrieving
// log details for giving log item.
type LogMessage interface {
	Message() string
}

// Logs defines a acceptable logging interface which all elements and sub packages
// will respect and use to deliver logs for different parts and ops, this frees
// this package from specifying or locking a giving implementation and contaminating
// import paths. Implement this and pass in to elements that provide for it.
type Logs interface {
	Emit(Level, LogMessage)
}

// ContextLogs defines an interface that returns a Logs which exposes a method to return
// a logger which contextualizes the provided actor as a base for it's logger.
type ContextLogs interface {
	Get(Actor) Logs
}

//*****************************************************************
// DrainLog
//*****************************************************************

// DrainLog implements the actorkit.Logs interface.
type DrainLog struct{}

// Emit does nothing with provided arguments, it implements
// actorkit.Logs Emit method.
func (DrainLog) Emit(_ Level, _ LogMessage) {}

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
