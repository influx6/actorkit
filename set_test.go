package actorkit_test

import (
	"testing"

	"github.com/gokit/actorkit"
	"github.com/stretchr/testify/require"
)

func TestRandomSet(t *testing.T) {
	rm := actorkit.NewRandomSet()
	rm.Add("a")
	rm.Add("b")
	rm.Add("c")

	elem := rm.Get()
	require.NotEqual(t, elem, rm.Get())
}

func TestHashedSet(t *testing.T) {
	rm := actorkit.NewHashedSet([]string{"a", "b", "c"})

	elem, ok := rm.Get("a")
	require.True(t, ok)
	require.Equal(t, "a", elem)
}

func TestRoundRobin(t *testing.T) {
	rm := actorkit.NewRoundRobinSet()
	rm.Add("a")
	rm.Add("b")
	rm.Add("c")

	seen := map[string]bool{}

	c := rm.Get()
	require.False(t, seen[c])
	seen[c] = true

	c = rm.Get()
	require.False(t, seen[c])
	seen[c] = true

	c = rm.Get()
	require.False(t, seen[c])
	seen[c] = true
}
