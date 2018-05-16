package actorkit

import (
	"github.com/rs/xid"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestMask(t *testing.T) {
	ac := noActorProcess{id: xid.New()}
	rs := ResolveAlways(ac)
	mask := GetMask(AnyNetworkAddr, "no-actor", rs)

	// send one asset into go-routine to add
	// situation for possible race, so -race
	// can catch it.
	c := make(chan struct{})
	go func() {
		assert.Equal(t, mask.Service(), "no-actor")
		close(c)
	}()

	assert.Equal(t, ac.ID(), mask.ID())
	assert.Equal(t, mask.Address(), AnyNetworkAddr)
	<-c
}
