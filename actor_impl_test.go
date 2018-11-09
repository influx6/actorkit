package actorkit_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/gokit/actorkit"
)

type basic struct{}

func (b basic) Action(addr actorkit.Addr, env actorkit.Envelope) {
}

func TestActorWithChildStates(t *testing.T) {
	am := actorkit.NewActorImpl(
		"kit",
		"127.0.0.1:2000",
		actorkit.UseBehaviour(&basic{}),
	)

	childAddr, err := am.Spawn("child", &basic{})
	assert.NoError(t, err)

	assert.NoError(t, am.Start().Wait())
	assert.False(t, am.Stopped())

	assert.NoError(t, am.Restart().Wait())
	assert.False(t, am.Stopped())

	assert.NoError(t, am.Stop().Wait())
	assert.True(t, am.Stopped())

	assert.True(t, childAddr.ID() != am.ID())
	assert.Error(t, childAddr.Send("a", actorkit.Header{}, nil))
}

func TestActorImpl(t *testing.T) {
	am := actorkit.NewActorImpl(
		"kit",
		"127.0.0.1:2000",
		actorkit.UseBehaviour(basic{}),
	)

	specs := []func(int, interface{}){
		func(d int, i interface{}) {
			assert.IsType(t, actorkit.ActorStartRequested{}, i, "index %d", d)
		},
		func(d int, i interface{}) {
			assert.IsType(t, actorkit.ActorStarted{}, i, "index %d", d)
		},
		func(d int, i interface{}) {
			assert.IsType(t, actorkit.ActorRestartRequested{}, i, "index %d", d)
		},
		func(d int, i interface{}) {
			assert.IsType(t, actorkit.ActorStopRequested{}, i, "index %d", d)
		},
		func(d int, i interface{}) {
			assert.IsType(t, actorkit.ActorStopped{}, i, "index %d", d)
		},
		func(d int, i interface{}) {
			assert.IsType(t, actorkit.ActorRestarted{}, i, "index %d", d)
		},
		func(d int, i interface{}) {
			assert.IsType(t, actorkit.ActorStopRequested{}, i, "index %d", d)
		},
		func(d int, i interface{}) {
			assert.IsType(t, actorkit.ActorStopped{}, i, "index %d", d)
		},
		func(d int, i interface{}) {
			assert.IsType(t, actorkit.ActorStopRequested{}, i, "index %d", d)
		},
		func(d int, i interface{}) {
			assert.IsType(t, actorkit.ActorStopped{}, i, "index %d", d)
		},
	}

	var events []interface{}
	sub := am.Watch(func(i interface{}) {
		events = append(events, i)
	})

	defer sub.Stop()

	assert.NoError(t, am.Start().Wait())
	assert.False(t, am.Stopped())

	assert.NoError(t, am.Restart().Wait())
	assert.False(t, am.Stopped())

	assert.NoError(t, am.Stop().Wait())
	assert.True(t, am.Stopped())

	for ind, elem := range events {
		spec := specs[ind]
		spec(ind, elem)
	}
}
