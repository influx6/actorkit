package actorkit_test

import (
	"sync"
	"testing"

	"github.com/gokit/actorkit"
	"github.com/stretchr/testify/require"
)

var (
	eb   = &actorkit.AddrImpl{}
	env  = actorkit.CreateEnvelope(eb, actorkit.Header{}, 1)
	env2 = actorkit.CreateEnvelope(eb, actorkit.Header{}, 2)
)

func BenchmarkBoxQueue_PushPopUnPop(b *testing.B) {
	b.ReportAllocs()

	q := actorkit.UnboundedBoxQueue(nil)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		q.Push(nil, env)
		go func() {
			q.Pop()
			q.Unpop(nil, env)
		}()
	}
	b.StopTimer()
}

func BenchmarkBoxQueue_PushPop(b *testing.B) {
	b.ReportAllocs()

	q := actorkit.UnboundedBoxQueue(nil)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		q.Push(nil, env)
		q.Pop()
	}
	b.StopTimer()
}

func BenchmarkBoxQueue_PushAndPop(b *testing.B) {
	b.ReportAllocs()

	q := actorkit.UnboundedBoxQueue(nil)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		q.Push(nil, env)
	}

	for i := 0; i < b.N; i++ {
		q.Pop()
	}
	b.StopTimer()
}

func TestBoxQueue_PushPopUnPop(t *testing.T) {
	q := actorkit.UnboundedBoxQueue(nil)

	q.Push(nil, env)
	q.Push(nil, env2)

	_, popped, err := q.Pop()
	require.NoError(t, err)
	require.Equal(t, 1, popped.Data)

	q.Push(nil, env)
	q.Push(nil, env2)

	_, popped2, err := q.Pop()
	require.NoError(t, err)
	require.Equal(t, 2, popped2.Data)

	_, popped3, err := q.Pop()
	require.NoError(t, err)
	require.NotNil(t, popped3)

	require.False(t, q.IsEmpty())

	_, popped4, err := q.Pop()
	require.NoError(t, err)
	require.NotNil(t, popped4)

	require.True(t, q.IsEmpty())
}

func TestBoxQueue_WaitLoop(t *testing.T) {
	var w sync.WaitGroup
	w.Add(1)

	q := actorkit.UnboundedBoxQueue(nil)
	require.True(t, q.IsEmpty())

	go func() {
		defer w.Done()

		var c int
		for {
			if c >= 100 {
				require.True(t, q.IsEmpty())
				require.Equal(t, 100, c)
				return
			}

			q.Wait()
			q.Pop()
			c++
		}
	}()

	for i := 100; i > 0; i-- {
		q.Push(nil, env)
	}

	w.Wait()
}
func TestBoxQueue_Wait(t *testing.T) {
	var w sync.WaitGroup
	w.Add(1)

	q := actorkit.UnboundedBoxQueue(nil)
	require.True(t, q.IsEmpty())

	go func() {
		defer w.Done()
		q.Wait()
		require.False(t, q.IsEmpty())
	}()

	q.Push(nil, env)
	w.Wait()
}

func TestBoxQueue_Empty(t *testing.T) {
	q := actorkit.UnboundedBoxQueue(nil)
	require.True(t, q.IsEmpty())
	q.Push(nil, env)
	require.False(t, q.IsEmpty())
}

func TestBoundedBoxQueue_Empty(t *testing.T) {
	q := actorkit.BoundedBoxQueue(10, actorkit.DropOld, nil)
	require.True(t, q.IsEmpty())
	q.Push(nil, env)
	require.False(t, q.IsEmpty())
}

func TestBoundedBoxQueue_DropOldest(t *testing.T) {
	q := actorkit.BoundedBoxQueue(1, actorkit.DropOld, nil)
	require.True(t, q.IsEmpty())

	q.Push(nil, env)
	require.Equal(t, q.Total(), 1)
	q.Push(nil, env2)
	require.Equal(t, q.Total(), 1)

	_, popped2, err := q.Pop()
	require.NoError(t, err)
	require.NotEqual(t, popped2.Data, 1)
}

func TestBoundedBoxQueue_DropNewest(t *testing.T) {
	q := actorkit.BoundedBoxQueue(1, actorkit.DropNew, nil)
	require.True(t, q.IsEmpty())

	q.Push(nil, env)
	require.Equal(t, q.Total(), 1)
	q.Push(nil, env2)
	require.Equal(t, q.Total(), 1)

	_, popped, err := q.Pop()
	require.NoError(t, err)
	require.NotEqual(t, popped.Data, 2)
}

func TestBoundedBoxQueue_Drop_Unpop(t *testing.T) {
	q := actorkit.BoundedBoxQueue(1, actorkit.DropNew, nil)
	require.True(t, q.IsEmpty())

	q.Push(nil, env)
	require.Equal(t, q.Total(), 1)
	q.Unpop(nil, env2)
	require.Equal(t, q.Total(), 1)

	_, popped, err := q.Pop()
	require.NoError(t, err)
	require.NotEqual(t, popped.Data, 1)
	require.Equal(t, popped.Data, 2)
}
