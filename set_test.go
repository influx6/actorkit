package actorkit_test

import (
	"testing"

	"github.com/gokit/actorkit"
	"github.com/stretchr/testify/assert"
)

func TestRandomSet(t *testing.T) {
	rm := actorkit.NewRandomSet()
	rm.Add("a")
	rm.Add("b")
	rm.Add("c")

	elem := rm.Get()
	assert.NotEqual(t, elem, rm.Get())
}

func TestHashedSet(t *testing.T) {
	rm := actorkit.NewHashedSet([]string{"a", "b", "c"})

	elem, ok := rm.Get("a")
	assert.True(t, ok)
	assert.Equal(t, "a", elem)
}

func TestRoundRobin(t *testing.T) {
	rm := actorkit.NewRoundRobinSet()
	rm.Add("a")
	rm.Add("b")
	rm.Add("c")

	seen := map[string]bool{}

	c := rm.Get()
	assert.False(t, seen[c])
	seen[c] = true

	c = rm.Get()
	assert.False(t, seen[c])
	seen[c] = true

	c = rm.Get()
	assert.False(t, seen[c])
	seen[c] = true
}
