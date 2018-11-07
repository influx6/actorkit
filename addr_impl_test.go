package actorkit_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/gokit/actorkit"
)

func TestDeadLetterAddr(t *testing.T) {
	addr := actorkit.DeadLetters()

	assert.True(t, strings.HasPrefix(addr.Addr(), "kit://localhost"), "should have prefix %q", addr.Addr())

	_, err := addr.Spawn("wap", &basic{})
	assert.Error(t, err)

	_, err = addr.AddressOf("wap", true)
	assert.Error(t, err)

	signal := make(chan struct{}, 1)
	defer addr.Watch(func(_ interface{}) {
		signal <- struct{}{}
	}).Stop()

	addr.Send("Welcome", nil, addr)
	assert.Len(t, signal, 1)
	<-signal
}
