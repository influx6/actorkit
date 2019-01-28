package actorkit_test

import (
	"testing"

	"github.com/gokit/actorkit"
	"github.com/stretchr/testify/require"
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

	require.NoError(t, am.Start())
	require.True(t, isRunning(am))

	require.NoError(t, am.Restart())
	require.True(t, isRunning(am))

	require.NoError(t, am.Stop())
	require.False(t, isRunning(am))
}

func TestActorImplMessaging(t *testing.T) {
	base := &basic{Message: make(chan *actorkit.Envelope, 1)}
	am := actorkit.NewActorImpl("ns", "ds", actorkit.Prop{Behaviour: base})

	require.NoError(t, am.Start())
	require.True(t, isRunning(am))

	addr := actorkit.AddressOf(am, "basic")
	require.NoError(t, addr.Send(2, nil))

	require.NoError(t, am.Stop())
	require.False(t, isRunning(am))

	require.Len(t, base.Message, 1)
	content := <-base.Message
	require.NotNil(t, content)
	require.Equal(t, content.Data, 2)
}

func TestActorChildMessaging(t *testing.T) {
	system, err := actorkit.Ancestor("kit", "localhost", actorkit.Prop{})
	require.NoError(t, err)
	require.NotNil(t, system)

	base := &basic{
		Message: make(chan *actorkit.Envelope, 1),
	}

	addr, err := system.Spawn("basic", actorkit.Prop{Behaviour: base})
	require.NoError(t, err)
	require.NotNil(t, addr)

	require.NoError(t, addr.Send(2, nil))

	require.NoError(t, actorkit.Poison(system))
	require.False(t, isRunning(system))

	require.Len(t, base.Message, 1)
	content := <-base.Message
	require.NotNil(t, content)
	require.Equal(t, content.Data, 2)
}

func TestActorImplPanic(t *testing.T) {
	supervisor := &actorkit.OneForOneSupervisor{
		Max: 30,
		PanicAction: func(i interface{}, addr actorkit.Addr, actor actorkit.Actor) {
			require.NotNil(t, i)
			require.IsType(t, actorkit.PanicEvent{}, i)
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

	require.NoError(t, am.Start())
	require.True(t, isRunning(am))

	addr := actorkit.AddressOf(am, "basic")
	require.NoError(t, addr.Send(2, nil))

	require.NoError(t, am.Stop())
	require.False(t, isRunning(am))
}

func TestActorWithChildTreeStates(t *testing.T) {
	base := &basic{
		Message: make(chan *actorkit.Envelope, 1),
	}
	am := actorkit.NewActorImpl("ns", "ds", actorkit.Prop{Behaviour: base})

	require.NoError(t, am.Start())
	require.True(t, isRunning(am))

	childAddr, err := am.Spawn("child", actorkit.Prop{Behaviour: base})
	require.NoError(t, err)

	require.Len(t, am.Children(), 1)

	grandChild, err := childAddr.Spawn("grand-child", actorkit.Prop{Behaviour: base})
	require.NoError(t, err)

	require.Len(t, childAddr.Children(), 1)

	require.NoError(t, am.Restart())
	require.True(t, isRunning(am))

	require.NoError(t, am.Stop())
	require.False(t, isRunning(am))

	require.True(t, childAddr.ID() != am.ID())
	require.Error(t, childAddr.Send("a", nil))

	require.True(t, grandChild.ID() != childAddr.ID())
	require.Error(t, grandChild.Send("a", nil))
}

func TestActorWithChildStates(t *testing.T) {
	base := &basic{
		Message: make(chan *actorkit.Envelope, 1),
	}

	am := actorkit.NewActorImpl("ns", "ds", actorkit.Prop{Behaviour: base})

	require.NoError(t, am.Start())
	require.True(t, isRunning(am))

	childAddr, err := am.Spawn("child", actorkit.Prop{Behaviour: &basic{}})
	require.NoError(t, err)

	require.NoError(t, am.Restart())
	require.True(t, isRunning(am))

	require.NoError(t, am.Stop())
	require.False(t, isRunning(am))

	require.True(t, childAddr.ID() != am.ID())
	require.Error(t, childAddr.Send("a", nil))
}
