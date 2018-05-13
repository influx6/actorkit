package actorkit

import (
	"testing"
	"time"
	"errors"
	"github.com/stretchr/testify/assert"
)

var (
	eb = NewMask(AnyNetworkAddr, "bob")
)

func TestFutureResolvedWithError(t *testing.T){
	tf := resolvedFutureWithError(ErrDeadDoesNotStopp, &localMask{})
	assert.Nil(t, tf.Result())
	assert.NotNil(t, tf.Err())
	assert.Equal(t, tf.Err(), ErrDeadDoesNotStopp)
	tf.Wait()
}

func TestFutureResolved(t *testing.T){
	env := NEnvelope("bob", Header{}, eb, 1)
	tf := resolvedFuture(env, &localMask{})
	assert.NotNil(t, tf.Result())
	assert.Nil(t, tf.Err())
	tf.Wait()
}

func TestFutureTimeout(t *testing.T){
	tf := newFutureActor(1 * time.Second, &localMask{})
	tf.start()
	time.Sleep(2 * time.Second)
	assert.Nil(t, tf.Result())
	assert.NotNil(t, tf.Err())
	assert.Equal(t, tf.Err(), ErrFutureTimeout)
	tf.Wait()
}

func TestFutureResolutionCloseToTimeout(t *testing.T){
	tf := newFutureActor(1 * time.Second, &localMask{})
	tf.start()

	time.Sleep(800 * time.Millisecond)
	assert.Nil(t, tf.Err())
	assert.Nil(t, tf.Result())

	env := NEnvelope("bob", Header{}, eb, 1)
	tf.Receive(env)
	tf.Wait()

	assert.Equal(t,tf.Result().Data(), 1)
}

func TestFutureResolutionCloseToTimeoutErr(t *testing.T){
	tf := newFutureActor(1 * time.Second, &localMask{})
	tf.start()

	time.Sleep(800 * time.Millisecond)
	assert.Nil(t, tf.Err())
	assert.Nil(t, tf.Result())

	err := errors.New("no")

	env := NEnvelope("bob", Header{}, eb, err)
	tf.Receive(env)
	tf.Wait()

	assert.Equal(t,tf.Err(), nil)
	assert.Equal(t,tf.Result().Data(), err)
}
