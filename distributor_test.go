package actorkit

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestProcessDistributor(t *testing.T) {
	dist := GetDistributor()

	assert.NotNil(t, dist.Deadletter())
	assert.Len(t, dist.FindAll("wicked"), 1)
	assert.NotNil(t, dist.FindAny("wicked"))
	assert.Equal(t, dist.FindAny("wicked"), deadMask)
	assert.Equal(t, dist.Deadletter(), deadMask)

	op := NewLocalResolver()
	op.Register(newNoActorProcess(), "wicka")
	dist.AddResolver(op)

	assert.NotNil(t, dist.FindAny("wicka"))
	assert.NotEqual(t, dist.FindAny("wicka"), deadMask)
}
