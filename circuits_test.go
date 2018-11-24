package actorkit_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/gokit/actorkit"
)

func TestCircuitBreakerOpenHalfOpenCloseState(t *testing.T) {
	cb := actorkit.NewCircuitBreaker("mycircuit", actorkit.Circuit{
		Timeout:     time.Second * 1,
		MaxFailures: 3,
		MinCoolDown: 2 * time.Second,
		MaxCoolDown: 4 * time.Second,
	})

	assert.False(t, cb.IsOpened())

	for i := 3; i > 0; i-- {
		assert.Error(t, cb.Do(context.Background(), func(ctx context.Context) error {
			return errors.New("bad")
		}, nil))
	}

	assert.True(t, cb.IsOpened())

	assert.Error(t, cb.Do(context.Background(), func(ctx context.Context) error {
		assert.Fail(t, "Should not be executed")
		return nil
	}, nil))

	<-time.After(time.Second * 3)

	assert.True(t, cb.IsOpened())
	assert.NoError(t, cb.Do(context.Background(), func(ctx context.Context) error {
		return nil
	}, nil))
	assert.False(t, cb.IsOpened())
}

func TestCircuitBreakerOpenCloseState(t *testing.T) {
	cb := actorkit.NewCircuitBreaker("mycircuit", actorkit.Circuit{
		Timeout:     time.Second * 1,
		MaxFailures: 3,
		MinCoolDown: 2 * time.Second,
		MaxCoolDown: 4 * time.Second,
	})

	assert.False(t, cb.IsOpened())

	for i := 3; i > 0; i-- {
		assert.Error(t, cb.Do(context.Background(), func(ctx context.Context) error {
			return errors.New("bad")
		}, nil))
	}

	assert.True(t, cb.IsOpened())

	assert.Error(t, cb.Do(context.Background(), func(ctx context.Context) error {
		assert.Fail(t, "Should not be executed")
		return nil
	}, nil))

	<-time.After(time.Second * 3)

	assert.True(t, cb.IsOpened())
	assert.Error(t, cb.Do(context.Background(), func(ctx context.Context) error {
		return errors.New("we bad")
	}, nil))
	assert.True(t, cb.IsOpened())

	<-time.After(time.Second * 3)

	assert.True(t, cb.IsOpened())
	assert.Error(t, cb.Do(context.Background(), func(ctx context.Context) error {
		return errors.New("we bad")
	}, nil))
	assert.True(t, cb.IsOpened())

	<-time.After(time.Second * 6)
	assert.True(t, cb.IsOpened())
	assert.NoError(t, cb.Do(context.Background(), func(ctx context.Context) error {
		return nil
	}, nil))
	assert.False(t, cb.IsOpened())
}

func TestCircuitBreaker_Hooks(t *testing.T) {
	var w sync.WaitGroup
	w.Add(5)

	cb := actorkit.NewCircuitBreaker("mycircuit", actorkit.Circuit{
		Timeout:     time.Second * 1,
		MaxFailures: 1,
		MinCoolDown: 2 * time.Second,
		MaxCoolDown: 3 * time.Second,
		OnRun: func(name string, start time.Time, end time.Time, err error) {
			w.Done()
		},
		OnClose: func(name string, lastCoolDown time.Duration) {
			w.Done()
		},
		OnTrip: func(name string, lastError error) {
			w.Done()
		},
		OnHalfOpen: func(name string, lastCoolDown time.Duration, lastOpenedTime time.Time) {
			w.Done()
		},
	})

	assert.False(t, cb.IsOpened())
	cb.Do(context.Background(), func(ctx context.Context) error {
		return errors.New("bad")
	}, nil)

	assert.True(t, cb.IsOpened())

	<-time.After(time.Second * 3)

	assert.True(t, cb.IsOpened())
	assert.NoError(t, cb.Do(context.Background(), func(ctx context.Context) error {
		return nil
	}, nil))
	assert.False(t, cb.IsOpened())

	w.Wait()
}

func TestCircuitBreaker_OneFailure(t *testing.T) {
	cb := actorkit.NewCircuitBreaker("mycircuit", actorkit.Circuit{
		Timeout:     time.Second * 1,
		MaxFailures: 1,
	})

	assert.False(t, cb.IsOpened())
	cb.Do(context.Background(), func(ctx context.Context) error {
		<-time.After(time.Second * 2)
		return nil
	}, nil)

	assert.True(t, cb.IsOpened())
}

func TestCircuitBreaker_TwoFailure(t *testing.T) {
	cb := actorkit.NewCircuitBreaker("mycircuit", actorkit.Circuit{
		Timeout:     time.Second * 1,
		MaxFailures: 2,
	})

	assert.False(t, cb.IsOpened())
	cb.Do(context.Background(), func(ctx context.Context) error {
		<-time.After(time.Second * 2)
		return nil
	}, nil)
	assert.False(t, cb.IsOpened())

	cb.Do(context.Background(), func(ctx context.Context) error {
		<-time.After(time.Second * 2)
		return nil
	}, nil)
	assert.True(t, cb.IsOpened())
}

func TestCircuitBreaker_FailureDueToError(t *testing.T) {
	cb := actorkit.NewCircuitBreaker("mycircuit", actorkit.Circuit{
		Timeout:     time.Second * 10,
		MaxFailures: 1,
	})

	assert.False(t, cb.IsOpened())

	cb.Do(context.Background(), func(ctx context.Context) error {
		<-time.After(time.Second * 2)
		return nil
	}, nil)

	assert.False(t, cb.IsOpened())

	cb.Do(context.Background(), func(ctx context.Context) error {
		<-time.After(time.Second * 2)
		return errors.New("bad")
	}, nil)
	assert.True(t, cb.IsOpened())
}
