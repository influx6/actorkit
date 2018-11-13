package actorkit_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/gokit/actorkit"
)

type basic struct{}

func (b basic) Action(addr actorkit.Addr, env actorkit.Envelope) {
}

func TestActorWithChildTreeStates(t *testing.T) {
	am := actorkit.NewActorImpl(actorkit.UseBehaviour(&basic{}))

	assert.NoError(t, am.Start().Wait())
	assert.False(t, am.Stopped())

	childAddr, err := am.Spawn("child", &basic{})
	assert.NoError(t, err)

	grandChild, err := childAddr.Spawn("grand-child", &basic{})
	assert.NoError(t, err)

	assert.NoError(t, am.Restart().Wait())
	assert.False(t, am.Stopped())

	assert.NoError(t, am.Stop().Wait())
	assert.True(t, am.Stopped())

	assert.True(t, childAddr.ID() != am.ID())
	assert.Error(t, childAddr.Send("a", actorkit.Header{}, nil))

	assert.True(t, grandChild.ID() != childAddr.ID())
	assert.Error(t, grandChild.Send("a", actorkit.Header{}, nil))
}

func TestActorWithChildStates(t *testing.T) {
	am := actorkit.NewActorImpl(
		actorkit.UseBehaviour(&basic{}),
	)

	assert.NoError(t, am.Start().Wait())
	assert.False(t, am.Stopped())

	childAddr, err := am.Spawn("child", &basic{})
	assert.NoError(t, err)

	assert.NoError(t, am.Restart().Wait())
	assert.False(t, am.Stopped())

	assert.NoError(t, am.Stop().Wait())
	assert.True(t, am.Stopped())

	assert.True(t, childAddr.ID() != am.ID())
	assert.Error(t, childAddr.Send("a", actorkit.Header{}, nil))
}

func TestActorImpl(t *testing.T) {
	am := actorkit.NewActorImpl(
		actorkit.UseBehaviour(basic{}),
	)

	assert.NoError(t, am.Start().Wait())
	assert.False(t, am.Stopped())

	assert.NoError(t, am.Restart().Wait())
	assert.False(t, am.Stopped())

	assert.NoError(t, am.Stop().Wait())
	assert.True(t, am.Stopped())
}
