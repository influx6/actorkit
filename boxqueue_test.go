package actorkit_test

import (
	"sync"
	"testing"

	"github.com/gokit/actorkit"
	"github.com/stretchr/testify/assert"
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
	assert.NoError(t, err)
	assert.Equal(t, 1, popped.Data)

	q.Push(nil, env)
	q.Push(nil, env2)

	_, popped2, err := q.Pop()
	assert.NoError(t, err)
	assert.Equal(t, 2, popped2.Data)

	_, popped3, err := q.Pop()
	assert.NoError(t, err)
	assert.NotNil(t, popped3)

	assert.False(t, q.Empty())

	_, popped4, err := q.Pop()
	assert.NoError(t, err)
	assert.NotNil(t, popped4)

	assert.True(t, q.Empty())
}

func TestBoxQueue_WaitLoop(t *testing.T) {
	var w sync.WaitGroup
	w.Add(1)

	q := actorkit.UnboundedBoxQueue(nil)
	assert.True(t, q.Empty())

	go func() {
		defer w.Done()

		var c int
		for {
			if c >= 100 {
				assert.True(t, q.Empty())
				assert.Equal(t, 100, c)
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
	assert.True(t, q.Empty())

	go func() {
		defer w.Done()
		q.Wait()
		assert.False(t, q.Empty())
	}()

	q.Push(nil, env)
	w.Wait()
}

func TestBoxQueue_Empty(t *testing.T) {
	q := actorkit.UnboundedBoxQueue(nil)
	assert.True(t, q.Empty())
	q.Push(nil, env)
	assert.False(t, q.Empty())
}

func TestBoundedBoxQueue_Empty(t *testing.T) {
	q := actorkit.BoundedBoxQueue(10, actorkit.DropOld, nil)
	assert.True(t, q.Empty())
	q.Push(nil, env)
	assert.False(t, q.Empty())
}

func TestBoundedBoxQueue_DropOldest(t *testing.T) {
	q := actorkit.BoundedBoxQueue(1, actorkit.DropOld, nil)
	assert.True(t, q.Empty())

	q.Push(nil, env)
	assert.Equal(t, q.Total(), 1)
	q.Push(nil, env2)
	assert.Equal(t, q.Total(), 1)

	_, popped2, err := q.Pop()
	assert.NoError(t, err)
	assert.NotEqual(t, popped2.Data, 1)
}

func TestBoundedBoxQueue_DropNewest(t *testing.T) {
	q := actorkit.BoundedBoxQueue(1, actorkit.DropNew, nil)
	assert.True(t, q.Empty())

	q.Push(nil, env)
	assert.Equal(t, q.Total(), 1)
	q.Push(nil, env2)
	assert.Equal(t, q.Total(), 1)

	_, popped, err := q.Pop()
	assert.NoError(t, err)
	assert.NotEqual(t, popped.Data, 2)
}

func TestBoundedBoxQueue_Drop_Unpop(t *testing.T) {
	q := actorkit.BoundedBoxQueue(1, actorkit.DropNew, nil)
	assert.True(t, q.Empty())

	q.Push(nil, env)
	assert.Equal(t, q.Total(), 1)
	q.Unpop(nil, env2)
	assert.Equal(t, q.Total(), 1)

	_, popped, err := q.Pop()
	assert.NoError(t, err)
	assert.NotEqual(t, popped.Data, 1)
	assert.Equal(t, popped.Data, 2)
}
