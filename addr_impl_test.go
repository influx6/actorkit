package actorkit_test

import (
	"strings"
	"testing"

	"github.com/gokit/actorkit"
	"github.com/stretchr/testify/require"
)

func TestAddrAddressFormat(t *testing.T) {
	system, err := actorkit.Ancestor("kitkat", "10.10.10.10:2020", actorkit.Prop{})
	require.NoError(t, err)
	require.NotNil(t, system)

	require.Equal(t, "kitkat@10.10.10.10:2020/"+system.ID()+"/access", system.Addr())

	child, err := system.Spawn("runner", actorkit.Prop{Op: &basic{}})
	require.NoError(t, err)
	require.NotNil(t, child)

	require.Equal(t, "kitkat@10.10.10.10:2020/"+system.ID()+"/"+child.ID()+"/runner", child.Addr())

	actorkit.Destroy(system)
}

func TestDeadLetterAddr(t *testing.T) {
	addr := actorkit.DeadLetters()

	require.True(t, strings.HasPrefix(addr.Addr(), "kit@localhost"), "should have prefix %q", addr.Addr())

	_, err := addr.Spawn("wap", actorkit.Prop{Op: &basic{}})
	require.Error(t, err)

	_, err = addr.AddressOf("wap", true)
	require.Error(t, err)

	signal := make(chan struct{}, 1)
	defer addr.Watch(func(_ interface{}) {
		signal <- struct{}{}
	}).Stop()

	addr.Send("Welcome", addr)
	require.Len(t, signal, 1)
	<-signal
}
