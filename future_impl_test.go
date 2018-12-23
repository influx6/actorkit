package actorkit_test

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/gokit/actorkit"
	"github.com/gokit/actorkit/mocks"
)

func TestFutureResolved(t *testing.T) {
	addr := &mocks.AddrImpl{}
	newFuture := actorkit.NewFuture(addr)
	assert.NoError(t, newFuture.Send("ready", eb))
	assert.NoError(t, newFuture.Err())
	assert.Equal(t, newFuture.Result().Data, "ready")
}

func TestFuturePipe(t *testing.T) {
	addr := &mocks.AddrImpl{}
	newFuture := actorkit.NewFuture(addr)
	newFuture2 := actorkit.NewFuture(addr)
	newFuture3 := actorkit.NewFuture(addr)

	newFuture.Pipe(newFuture2, newFuture3)

	failure := errors.New("bad error")
	newFuture.Escalate(failure)

	assert.Equal(t, failure.Error(), newFuture.Wait().Error())
	assert.Equal(t, failure.Error(), newFuture2.Wait().Error())
	assert.Equal(t, failure.Error(), newFuture3.Wait().Error())

	assert.Equal(t, failure.Error(), newFuture.Err().Error())
	assert.Equal(t, failure.Error(), newFuture2.Err().Error())
	assert.Equal(t, failure.Error(), newFuture3.Err().Error())
}

func TestFutureEscalate(t *testing.T) {
	addr := &mocks.AddrImpl{}
	newFuture := actorkit.NewFuture(addr)
	newFuture.Escalate("wake")
	assert.Equal(t, newFuture.Err().Error(), actorkit.ErrFutureEscalatedFailure.Error())
}

func TestFutureTimeout(t *testing.T) {
	addr := &mocks.AddrImpl{}
	newFuture := actorkit.TimedFuture(addr, 1*time.Second)
	<-time.After(2 * time.Second)
	assert.Equal(t, newFuture.Err().Error(), actorkit.ErrFutureTimeout.Error())
	assert.Error(t, newFuture.Send("ready", eb))
}
