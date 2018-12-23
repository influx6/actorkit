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
	am := actorkit.NewActorImpl("ns", "ds", actorkit.Prop{Behaviour: base})

	assert.NoError(t, am.Start())
	assert.True(t, am.Running())

	assert.NoError(t, am.Restart())
	assert.True(t, am.Running())

	assert.NoError(t, am.Stop())
	assert.False(t, am.Running())
}

func TestActorImplMessaging(t *testing.T) {
	base := &basic{Message: make(chan *actorkit.Envelope, 1)}
	am := actorkit.NewActorImpl("ns", "ds", actorkit.Prop{Behaviour: base})

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

func TestActorChildMessaging(t *testing.T) {
	system, err := actorkit.Ancestor("kit", "localhost", actorkit.Prop{})
	assert.NoError(t, err)
	assert.NotNil(t, system)

	base := &basic{
		Message: make(chan *actorkit.Envelope, 1),
	}

	addr, err := system.Spawn("basic", actorkit.Prop{Behaviour: base})
	assert.NoError(t, err)
	assert.NotNil(t, addr)

	assert.NoError(t, addr.Send(2, nil))

	assert.NoError(t, actorkit.Poison(system))
	assert.False(t, system.Actor().Running())

	assert.Len(t, base.Message, 1)
	content := <-base.Message
	assert.NotNil(t, content)
	assert.Equal(t, content.Data, 2)
}

func TestActorImplPanic(t *testing.T) {
	supervisor := &actorkit.OneForOneSupervisor{
		Max: 30,
		PanicAction: func(i interface{}, addr actorkit.Addr, actor actorkit.Actor) {
			assert.NotNil(t, i)
			assert.IsType(t, actorkit.PanicEvent{}, i)
		},
		Decider: func(tm interface{}) actorkit.Directive {
			switch tm.(type) {
			case actorkit.PanicEvent:
				return actorkit.PanicDirective
			default:
				return actorkit.IgnoreDirective
			}
		},
	}

	am := actorkit.FromFunc("ns", "br", func(addr actorkit.Addr, envelope actorkit.Envelope) {
		panic("Error occured")
	}, actorkit.UseSupervisor(supervisor))

	assert.NoError(t, am.Start())
	assert.True(t, am.Running())

	addr := actorkit.AddressOf(am, "basic")
	assert.NoError(t, addr.Send(2, nil))

	assert.NoError(t, am.Stop())
	assert.False(t, am.Running())
}

func TestActorWithChildTreeStates(t *testing.T) {
	base := &basic{
		Message: make(chan *actorkit.Envelope, 1),
	}
	am := actorkit.NewActorImpl("ns", "ds", actorkit.Prop{Behaviour: base})

	assert.NoError(t, am.Start())
	assert.True(t, am.Running())

	childAddr, err := am.Spawn("child", actorkit.Prop{Behaviour: base})
	assert.NoError(t, err)

	assert.Len(t, am.Children(), 1)

	grandChild, err := childAddr.Spawn("grand-child", actorkit.Prop{Behaviour: base})
	assert.NoError(t, err)

	assert.Len(t, childAddr.Children(), 1)

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

	am := actorkit.NewActorImpl("ns", "ds", actorkit.Prop{Behaviour: base})

	assert.NoError(t, am.Start())
	assert.True(t, am.Running())

	childAddr, err := am.Spawn("child", actorkit.Prop{Behaviour: &basic{}})
	assert.NoError(t, err)

	assert.NoError(t, am.Restart())
	assert.True(t, am.Running())

	assert.NoError(t, am.Stop())
	assert.False(t, am.Running())

	assert.True(t, childAddr.ID() != am.ID())
	assert.Error(t, childAddr.Send("a", nil))
}
