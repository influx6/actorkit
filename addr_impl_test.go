package actorkit_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/gokit/actorkit"
)

func TestAddrAddressFormat(t *testing.T) {
	system, err := actorkit.Ancestor("kitkat", "10.10.10.10:2020", actorkit.Prop{})
	assert.NoError(t, err)
	assert.NotNil(t, system)

	assert.Equal(t, "kitkat@10.10.10.10:2020/"+system.ID()+"/access", system.Addr())

	child, err := system.Spawn("runner", actorkit.Prop{Behaviour: &basic{}})
	assert.NoError(t, err)
	assert.NotNil(t, child)

	assert.Equal(t, "kitkat@10.10.10.10:2020/"+system.ID()+"/"+child.ID()+"/runner", child.Addr())

	actorkit.Destroy(system)
}

func TestDeadLetterAddr(t *testing.T) {
	addr := actorkit.DeadLetters()

	assert.True(t, strings.HasPrefix(addr.Addr(), "kit@localhost"), "should have prefix %q", addr.Addr())

	_, err := addr.Spawn("wap", actorkit.Prop{Behaviour: &basic{}})
	assert.Error(t, err)

	_, err = addr.AddressOf("wap", true)
	assert.Error(t, err)

	signal := make(chan struct{}, 1)
	defer addr.Watch(func(_ interface{}) {
		signal <- struct{}{}
	}).Stop()

	addr.Send("Welcome", addr)
	assert.Len(t, signal, 1)
	<-signal
}
