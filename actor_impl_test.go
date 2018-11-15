package actorkit_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/gokit/actorkit"
)

type basic struct {
	Message chan *actorkit.Envelope
}

func (b *basic) Action(addr actorkit.Addr, env actorkit.Envelope) {
	b.Message <- &env
}

func TestActorImpl(t *testing.T) {
	base := &basic{
		Message: make(chan *actorkit.Envelope, 1),
	}
	am := actorkit.NewActorImpl(
		actorkit.UseBehaviour(base),
	)

	assert.NoError(t, am.Start())
	assert.True(t, am.Running())

	assert.NoError(t, am.Restart())
	assert.True(t, am.Running())

	assert.NoError(t, am.Stop())
	assert.False(t, am.Running())
}

func TestActorImplMessaging(t *testing.T) {
	base := &basic{
		Message: make(chan *actorkit.Envelope, 1),
	}
	am := actorkit.NewActorImpl(
		actorkit.UseBehaviour(base),
	)

	assert.NoError(t, am.Start())
	assert.True(t, am.Running())

	addr := actorkit.AddressOf(am, "basic")
	assert.NoError(t, addr.Send(2, nil))

	assert.NoError(t, am.Stop())
	assert.False(t, am.Running())

	assert.Len(t, base.Message, 1)
	content := <-base.Message
	assert.NotNil(t, content)
	assert.Equal(t, content.Data, 2)

}

func TestActorWithChildTreeStates(t *testing.T) {
	base := &basic{
		Message: make(chan *actorkit.Envelope, 1),
	}
	am := actorkit.NewActorImpl(
		actorkit.UseBehaviour(base),
	)

	assert.NoError(t, am.Start())
	assert.True(t, am.Running())

	childAddr, err := am.Spawn("child", &basic{})
	assert.NoError(t, err)

	grandChild, err := childAddr.Spawn("grand-child", &basic{})
	assert.NoError(t, err)

	assert.NoError(t, am.Restart())
	assert.True(t, am.Running())

	assert.NoError(t, am.Stop())
	assert.False(t, am.Running())

	assert.True(t, childAddr.ID() != am.ID())
	assert.Error(t, childAddr.Send("a", nil))

	assert.True(t, grandChild.ID() != childAddr.ID())
	assert.Error(t, grandChild.Send("a", nil))
}

func TestActorWithChildStates(t *testing.T) {
	base := &basic{
		Message: make(chan *actorkit.Envelope, 1),
	}
	am := actorkit.NewActorImpl(
		actorkit.UseBehaviour(base),
	)

	assert.NoError(t, am.Start())
	assert.True(t, am.Running())

	childAddr, err := am.Spawn("child", &basic{})
	assert.NoError(t, err)

	assert.NoError(t, am.Restart())
	assert.True(t, am.Running())

	assert.NoError(t, am.Stop())
	assert.False(t, am.Running())

	assert.True(t, childAddr.ID() != am.ID())
	assert.Error(t, childAddr.Send("a", nil))
}
