package actorkit

// DrainLog implements the actorkit.Logs interface.
type DrainLog struct{}

// Emit does nothing with provided arguments, it implements
// actorkit.Logs Emit method.
func (DrainLog) Emit(_ Level, _ LogEvent) {}
