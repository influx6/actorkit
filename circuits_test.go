package actorkit_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/gokit/actorkit"
	"github.com/stretchr/testify/require"
)

func TestCircuitBreakerOpenHalfOpenCloseState(t *testing.T) {
	cb := actorkit.NewCircuitBreaker("mycircuit", actorkit.Circuit{
		Timeout:     time.Second * 1,
		MaxFailures: 3,
		MinCoolDown: 2 * time.Second,
		MaxCoolDown: 4 * time.Second,
	})

	require.False(t, cb.IsOpened())

	for i := 3; i > 0; i-- {
		require.Error(t, cb.Do(context.Background(), func(ctx context.Context) error {
			return errors.New("bad")
		}, nil))
	}

	require.True(t, cb.IsOpened())

	require.Error(t, cb.Do(context.Background(), func(ctx context.Context) error {
		require.Fail(t, "Should not be executed")
		return nil
	}, nil))

	<-time.After(time.Second * 3)

	require.True(t, cb.IsOpened())
	require.NoError(t, cb.Do(context.Background(), func(ctx context.Context) error {
		return nil
	}, nil))
	require.False(t, cb.IsOpened())
}

func TestCircuitBreakerOpenCloseState(t *testing.T) {
	cb := actorkit.NewCircuitBreaker("mycircuit", actorkit.Circuit{
		Timeout:     time.Second * 1,
		MaxFailures: 3,
		MinCoolDown: 2 * time.Second,
		MaxCoolDown: 4 * time.Second,
	})

	require.False(t, cb.IsOpened())

	for i := 3; i > 0; i-- {
		require.Error(t, cb.Do(context.Background(), func(ctx context.Context) error {
			return errors.New("bad")
		}, nil))
	}

	require.True(t, cb.IsOpened())

	require.Error(t, cb.Do(context.Background(), func(ctx context.Context) error {
		require.Fail(t, "Should not be executed")
		return nil
	}, nil))

	<-time.After(time.Second * 3)

	require.True(t, cb.IsOpened())
	require.Error(t, cb.Do(context.Background(), func(ctx context.Context) error {
		return errors.New("we bad")
	}, nil))
	require.True(t, cb.IsOpened())

	<-time.After(time.Second * 3)

	require.True(t, cb.IsOpened())
	require.Error(t, cb.Do(context.Background(), func(ctx context.Context) error {
		return errors.New("we bad")
	}, nil))
	require.True(t, cb.IsOpened())

	<-time.After(time.Second * 6)
	require.True(t, cb.IsOpened())
	require.NoError(t, cb.Do(context.Background(), func(ctx context.Context) error {
		return nil
	}, nil))
	require.False(t, cb.IsOpened())
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

	require.False(t, cb.IsOpened())
	cb.Do(context.Background(), func(ctx context.Context) error {
		return errors.New("bad")
	}, nil)

	require.True(t, cb.IsOpened())

	<-time.After(time.Second * 3)

	require.True(t, cb.IsOpened())
	require.NoError(t, cb.Do(context.Background(), func(ctx context.Context) error {
		return nil
	}, nil))
	require.False(t, cb.IsOpened())

	w.Wait()
}

func TestCircuitBreaker_OneFailure(t *testing.T) {
	cb := actorkit.NewCircuitBreaker("mycircuit", actorkit.Circuit{
		Timeout:     time.Second * 1,
		MaxFailures: 1,
	})

	require.False(t, cb.IsOpened())
	cb.Do(context.Background(), func(ctx context.Context) error {
		<-time.After(time.Second * 2)
		return nil
	}, nil)

	require.True(t, cb.IsOpened())
}

func TestCircuitBreaker_TwoFailure(t *testing.T) {
	cb := actorkit.NewCircuitBreaker("mycircuit", actorkit.Circuit{
		Timeout:     time.Second * 1,
		MaxFailures: 2,
	})

	require.False(t, cb.IsOpened())
	cb.Do(context.Background(), func(ctx context.Context) error {
		<-time.After(time.Second * 2)
		return nil
	}, nil)
	require.False(t, cb.IsOpened())

	cb.Do(context.Background(), func(ctx context.Context) error {
		<-time.After(time.Second * 2)
		return nil
	}, nil)
	require.True(t, cb.IsOpened())
}

func TestCircuitBreaker_FailureDueToError(t *testing.T) {
	cb := actorkit.NewCircuitBreaker("mycircuit", actorkit.Circuit{
		Timeout:     time.Second * 10,
		MaxFailures: 1,
	})

	require.False(t, cb.IsOpened())

	cb.Do(context.Background(), func(ctx context.Context) error {
		<-time.After(time.Second * 2)
		return nil
	}, nil)

	require.False(t, cb.IsOpened())

	cb.Do(context.Background(), func(ctx context.Context) error {
		<-time.After(time.Second * 2)
		return errors.New("bad")
	}, nil)
	require.True(t, cb.IsOpened())
}
