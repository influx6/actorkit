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

	assert.NoError(t, am.Start("ready").Wait())
	assert.NoError(t, am.Stop("end").Wait())
}
