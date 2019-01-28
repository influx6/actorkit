package actorkit_test

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/gokit/actorkit"
	"github.com/gokit/actorkit/mocks"
)

func TestFutureResolved(t *testing.T) {
	addr := &mocks.AddrImpl{}
	newFuture := actorkit.NewFuture(addr)
	require.NoError(t, newFuture.Send("ready", eb))
	require.NoError(t, newFuture.Err())
	require.Equal(t, newFuture.Result().Data, "ready")
}

func TestFuturePipe(t *testing.T) {
	addr := &mocks.AddrImpl{}
	newFuture := actorkit.NewFuture(addr)
	newFuture2 := actorkit.NewFuture(addr)
	newFuture3 := actorkit.NewFuture(addr)

	newFuture.Pipe(newFuture2, newFuture3)

	failure := errors.New("bad error")
	newFuture.Escalate(failure)

	require.Equal(t, failure.Error(), newFuture.Wait().Error())
	require.Equal(t, failure.Error(), newFuture2.Wait().Error())
	require.Equal(t, failure.Error(), newFuture3.Wait().Error())

	require.Equal(t, failure.Error(), newFuture.Err().Error())
	require.Equal(t, failure.Error(), newFuture2.Err().Error())
	require.Equal(t, failure.Error(), newFuture3.Err().Error())
}

func TestFutureEscalate(t *testing.T) {
	addr := &mocks.AddrImpl{}
	newFuture := actorkit.NewFuture(addr)
	newFuture.Escalate("wake")
	require.Equal(t, newFuture.Err().Error(), actorkit.ErrFutureEscalatedFailure.Error())
}

func TestFutureTimeout(t *testing.T) {
	addr := &mocks.AddrImpl{}
	newFuture := actorkit.TimedFuture(addr, 1*time.Second)
	<-time.After(2 * time.Second)
	require.Equal(t, newFuture.Err().Error(), actorkit.ErrFutureTimeout.Error())
	require.Error(t, newFuture.Send("ready", eb))
}
