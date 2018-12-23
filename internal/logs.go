package internal

import (
	"fmt"
	"time"

	"github.com/gokit/actorkit"
)

// DrainLog implements the actorkit.Logs interface.
type DrainLog struct{}

// Emit does nothing with provided arguments, it implements
// actorkit.Logs Emit method.
func (DrainLog) Emit(_ actorkit.Level, _ actorkit.LogEvent) {}

// TLog implements the actorkit.Logs interface, printing
// out basic type and value contents with log.
type TLog struct{}

// Emit prints type implement log event and type data, it implements
// actorkit.Logs Emit method.
func (TLog) Emit(l actorkit.Level, e actorkit.LogEvent) {
	fmt.Printf("[%s : %s : %T] %s %#v\n", time.Now().Format(time.RFC3339), l, e, e.Message(), e)
}
