package actors

import (
	"github.com/gokit/actorkit"
	"github.com/stretchr/testify/assert"
	"testing"
)

type HelloOp struct {
	Started          chan bool
	ShuttingDown     chan bool
	FinishedShutdown chan bool
	Envelope         chan actorkit.Envelope
}

type HelloMessage struct {
	Name string
}

func (h HelloOp) Respond(me actorkit.Mask, e actorkit.Envelope, d actorkit.Distributor) {
	switch e.Data().(type) {
	case *actorkit.ProcessStarted:
		h.Started <- true
	case *actorkit.ProcessShuttingDown:
		h.ShuttingDown <- true
	case *actorkit.ProcessFinishedShutDown:
		h.FinishedShutdown <- true
	case *HelloMessage:
		h.Envelope <- e
	}
}

func TestFromActorWithMessage(t *testing.T) {
	started := make(chan bool, 1)
	shutdown := make(chan bool, 1)
	finished := make(chan bool, 1)
	envelope := make(chan actorkit.Envelope, 1)

	ax := FromActor(&HelloOp{
		Started:          started,
		ShuttingDown:     shutdown,
		FinishedShutdown: finished,
		Envelope:         envelope,
	})

	axMask := actorkit.ForceMaskWithProcess("local", "yay", ax)
	axMask.Send(&HelloMessage{Name: "Wally"}, actorkit.GetDeadletter())

	env := <-envelope
	assert.NotNil(t, env.Data())
	assert.IsType(t, &HelloMessage{}, env.Data())

	ax.GracefulStop().Wait()

	assert.NotEmpty(t, started)
	assert.NotEmpty(t, shutdown)
	assert.NotEmpty(t, finished)
}

func TestFromActor(t *testing.T) {
	started := make(chan bool, 1)
	shutdown := make(chan bool, 1)
	finished := make(chan bool, 1)
	ax := FromActor(&HelloOp{
		Started:          started,
		ShuttingDown:     shutdown,
		FinishedShutdown: finished,
	})

	ax.GracefulStop().Wait()

	assert.NotEmpty(t, started)
	assert.NotEmpty(t, shutdown)
	assert.NotEmpty(t, finished)
}

func TestFromFunc(t *testing.T) {
	started := make(chan bool, 1)
	shutdown := make(chan bool, 1)
	finished := make(chan bool, 1)
	ax := FromFunc(func(my actorkit.Mask, env actorkit.Envelope, d actorkit.Distributor) {
		switch env.Data().(type) {
		case *actorkit.ProcessStarted:
			started <- true
		case *actorkit.ProcessShuttingDown:
			shutdown <- true
		case *actorkit.ProcessFinishedShutDown:
			finished <- true
		}
	})

	ax.GracefulStop().Wait()

	assert.NotEmpty(t, started)
	assert.NotEmpty(t, shutdown)
	assert.NotEmpty(t, finished)
}
