package actorkit_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/gokit/actorkit"
)

func TestFutureResolved(t *testing.T) {
	newFuture := actorkit.NewFuture(eb)
	assert.NoError(t, newFuture.Send("ready", nil, eb))
	assert.NoError(t, newFuture.Err())
	assert.Equal(t, newFuture.Result().Data, "ready")
}

func TestFutureEscalate(t *testing.T) {
}

func TestFutureTimeout(t *testing.T) {
}
