package actorkit_test

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/gokit/actorkit"
)

func TestFutureResolved(t *testing.T) {
	addr := new(AddrImpl)
	newFuture := actorkit.NewFuture(addr)
	assert.NoError(t, newFuture.Send("ready", nil, eb))
	assert.NoError(t, newFuture.Err())
	assert.Equal(t, newFuture.Result().Data, "ready")
}

func TestFuturePipe(t *testing.T) {
	addr := new(AddrImpl)
	newFuture := actorkit.NewFuture(addr)
	newFuture2 := actorkit.NewFuture(addr)
	newFuture3 := actorkit.NewFuture(addr)

	newFuture.Pipe(newFuture2, newFuture3)

	failure := errors.New("bad error")
	newFuture.Escalate(failure)

	assert.Equal(t, newFuture.Wait(), failure)
	assert.Equal(t, newFuture2.Wait(), failure)
	assert.Equal(t, newFuture3.Wait(), failure)

	assert.Equal(t, newFuture.Err(), failure)
	assert.Equal(t, newFuture2.Err(), failure)
	assert.Equal(t, newFuture3.Err(), failure)
}

func TestFutureEscalate(t *testing.T) {
	addr := new(AddrImpl)
	newFuture := actorkit.NewFuture(addr)
	newFuture.Escalate("wake")
	assert.Equal(t, newFuture.Err(), actorkit.ErrFutureEscalatedFailure)
}

func TestFutureTimeout(t *testing.T) {
	addr := new(AddrImpl)
	newFuture := actorkit.TimedFuture(addr, 1*time.Second)
	<-time.After(2 * time.Second)
	assert.Equal(t, newFuture.Err(), actorkit.ErrFutureTimeout)
	assert.Error(t, newFuture.Send("ready", nil, eb))
}
