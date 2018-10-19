package mailbox

import (
	"sync"
	"testing"

	"github.com/gokit/actorkit"
	"github.com/stretchr/testify/assert"
)

var (
	eb   = actorkit.AddrImpl{}
	env  = actorkit.CreateEnvelope(eb, actorkit.Header{}, 1)
	env2 = actorkit.CreateEnvelope(eb, actorkit.Header{}, 2)
)

func BenchmarkBoxQueue_PushPopUnPop(b *testing.B) {
	b.ReportAllocs()

	q := UnboundedBoxQueue()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		q.Push(env)
		go func() {
			q.Pop()
			q.UnPop(env)
		}()
	}
	b.StopTimer()
}

func BenchmarkBoxQueue_PushPop(b *testing.B) {
	b.ReportAllocs()

	q := UnboundedBoxQueue()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		q.Push(env)
		q.Pop()
	}
	b.StopTimer()
}

func BenchmarkBoxQueue_PushAndPop(b *testing.B) {
	b.ReportAllocs()

	q := UnboundedBoxQueue()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		q.Push(env)
	}

	for i := 0; i < b.N; i++ {
		q.Pop()
	}
	b.StopTimer()
}

func TestBoxQueue_PushPopUnPop(t *testing.T) {
	q := UnboundedBoxQueue()

	q.Push(env)
	q.Push(env2)

	popped, err := q.Pop()
	assert.NoError(t, err)
	assert.Equal(t, 1, popped.Data)

	q.Push(env)
	q.Push(env2)

	popped2, err := q.Pop()
	assert.NoError(t, err)
	assert.Equal(t, 2, popped2.Data)

	popped3, err := q.Pop()
	assert.NoError(t, err)
	assert.NotNil(t, popped3)

	assert.False(t, q.Empty())

	popped4, err := q.Pop()
	assert.NoError(t, err)
	assert.NotNil(t, popped4)

	assert.True(t, q.Empty())
}

func TestBoxQueue_WaitLoop(t *testing.T) {
	var w sync.WaitGroup
	w.Add(1)

	q := UnboundedBoxQueue()
	assert.True(t, q.Empty())

	go func() {
		defer w.Done()

		var c int
		for {
			q.Wait()
			q.Pop()
			c++

			if q.Empty() {
				break
			}
		}
		assert.True(t, q.Empty())
		assert.Equal(t, 100, c)
	}()

	for i := 100; i > 0; i-- {
		q.Push(env)
	}

	w.Wait()
}
func TestBoxQueue_Wait(t *testing.T) {
	var w sync.WaitGroup
	w.Add(1)

	q := UnboundedBoxQueue()
	assert.True(t, q.Empty())

	go func() {
		defer w.Done()
		q.Wait()
		assert.False(t, q.Empty())
	}()

	q.Push(env)
	w.Wait()
}

func TestBoxQueue_Empty(t *testing.T) {
	q := UnboundedBoxQueue()
	assert.True(t, q.Empty())
	q.Push(env)
	assert.False(t, q.Empty())
}

func TestBoundedBoxQueue_Empty(t *testing.T) {
	q := BoundedBoxQueue(10, DropOld)
	assert.True(t, q.Empty())
	q.Push(env)
	assert.False(t, q.Empty())
}

func TestBoundedBoxQueue_DropOldest(t *testing.T) {
	q := BoundedBoxQueue(1, DropOld)
	assert.True(t, q.Empty())

	q.Push(env)
	assert.Equal(t, q.Total(), 1)
	q.Push(env2)
	assert.Equal(t, q.Total(), 1)

	popped2, err := q.Pop()
	assert.NoError(t, err)
	assert.NotEqual(t, popped2.Data, 1)
}

func TestBoundedBoxQueue_DropNewest(t *testing.T) {
	q := BoundedBoxQueue(1, DropNew)
	assert.True(t, q.Empty())

	q.Push(env)
	assert.Equal(t, q.Total(), 1)
	q.Push(env2)
	assert.Equal(t, q.Total(), 1)

	popped, err := q.Pop()
	assert.NoError(t, err)
	assert.NotEqual(t, popped.Data, 2)
}

func TestBoundedBoxQueue_Drop_Unpop(t *testing.T) {
	q := BoundedBoxQueue(1, DropNew)
	assert.True(t, q.Empty())

	q.Push(env)
	assert.Equal(t, q.Total(), 1)
	q.UnPop(env2)
	assert.Equal(t, q.Total(), 1)

	popped, err := q.Pop()
	assert.NoError(t, err)
	assert.NotEqual(t, popped.Data, 1)
	assert.Equal(t, popped.Data, 2)
}
