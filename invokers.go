package actorkit

import (
	"time"

	"github.com/gokit/es"
)

//****************************************
// EventSupervisingInvoker
//****************************************

// EventSupervisingInvoker implements the SupervisorInvoker interface and simply
// invokes events for all invocation received.
type EventSupervisingInvoker struct {
	Event *es.EventStream
}

// InvokedStop emits event containing stopped details.
func (es *EventSupervisingInvoker) InvokedStop(cause interface{}, stat map[string]int, addr Addr, target Actor) {
	es.Event.Publish(SupervisorEvent{
		Addr:      addr,
		Stat:      stat,
		Cause:     cause,
		Time:      time.Now(),
		Actor:     target.Addr(),
		Directive: StopDirective,
	})
}

// InvokedKill emits event containing killed details.
func (es *EventSupervisingInvoker) InvokedKill(cause interface{}, stat map[string]int, addr Addr, target Actor) {
	es.Event.Publish(SupervisorEvent{
		Addr:      addr,
		Stat:      stat,
		Cause:     cause,
		Time:      time.Now(),
		Actor:     target.Addr(),
		Directive: KillDirective,
	})
}

// InvokedDestroy emits event containing destroyed details.
func (es *EventSupervisingInvoker) InvokedDestroy(cause interface{}, stat map[string]int, addr Addr, target Actor) {
	es.Event.Publish(SupervisorEvent{
		Addr:      addr,
		Stat:      stat,
		Cause:     cause,
		Time:      time.Now(),
		Actor:     target.Addr(),
		Directive: DestroyDirective,
	})
}

// InvokedRestart emits event containing restart details.
func (es *EventSupervisingInvoker) InvokedRestart(cause interface{}, stat map[string]int, addr Addr, target Actor) {
	es.Event.Publish(SupervisorEvent{
		Addr:      addr,
		Stat:      stat,
		Cause:     cause,
		Time:      time.Now(),
		Actor:     target.Addr(),
		Directive: RestartDirective,
	})
}

//***********************************
//  Supervisor Events
//***********************************

// SupervisorEvent defines an event type which is published by the EventSupervisingInvoker.
type SupervisorEvent struct {
	Stat      map[string]int
	Addr      Addr
	Actor     string
	Time      time.Time
	Directive Directive
	Cause     interface{}
}
