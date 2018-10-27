package actorkit_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/gokit/actorkit"
)

type basic struct{}

func (b basic) Action(addr actorkit.Addr, env actorkit.Envelope) {
}

func TestActorImpl(t *testing.T) {
	am := actorkit.NewActorImpl(
		"kit",
		"127.0.0.1:2000",
		actorkit.UseBehaviour(basic{}),
	)

	specs := []func(interface{}){
		func(i interface{}) {
			assert.IsType(t, actorkit.ActorStartRequested{}, i)
		},
		func(i interface{}) {
			assert.IsType(t, actorkit.ActorStarted{}, i)
		},
		func(i interface{}) {
			assert.IsType(t, actorkit.ActorRestartRequested{}, i)
		},
		func(i interface{}) {
			assert.IsType(t, actorkit.ActorStopRequested{}, i)
		},
		func(i interface{}) {
			assert.IsType(t, actorkit.ActorStopped{}, i)
		},
		func(i interface{}) {
			assert.IsType(t, actorkit.ActorStartRequested{}, i)
		},
		func(i interface{}) {
			assert.IsType(t, actorkit.ActorStarted{}, i)
		},
		func(i interface{}) {
			assert.IsType(t, actorkit.ActorRestarted{}, i)
		},
		func(i interface{}) {
			assert.IsType(t, actorkit.ActorStopRequested{}, i)
		},
		func(i interface{}) {
			assert.IsType(t, actorkit.ActorStopped{}, i)
		},
	}

	var events []interface{}
	sub := am.Watch(func(i interface{}) {
		events = append(events, i)
	})

	defer sub.Stop()

	assert.NoError(t, am.Start("start").Wait())
	assert.False(t, am.Stopped())

	assert.NoError(t, am.Restart("restart").Wait())
	assert.False(t, am.Stopped())

	assert.NoError(t, am.Stop("stop").Wait())
	assert.True(t, am.Stopped())

	for ind, elem := range events {
		spec := specs[ind]
		spec(elem)
	}
}
