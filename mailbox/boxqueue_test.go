package mailbox

import (
	"testing"
	"github.com/stretchr/testify/assert"
	"github.com/gokit/actorkit"
)

func BenchmarkBoxQueue_PushPopUnPop(b *testing.B) {
	b.ReportAllocs()

	q := UnboundedBoxQueue()
	m := &actorkit.Envelope{Data:1}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		q.Push(m)
		go func(){
			q.Pop()
			q.UnPop(m)
		}()
	}
	b.StopTimer()
}

func BenchmarkBoxQueue_PushPop(b *testing.B) {
	b.ReportAllocs()

	q := UnboundedBoxQueue()
	m := &actorkit.Envelope{Data:1}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		q.Push(m)
		q.Pop()
	}
	b.StopTimer()
}

func BenchmarkBoxQueue_PushAndPop(b *testing.B) {
	b.ReportAllocs()

	q := UnboundedBoxQueue()
	m := &actorkit.Envelope{Data:1}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		q.Push(m)
	}

	for i := 0; i < b.N; i++ {
		q.Pop()
	}
	b.StopTimer()
}

func TestBoxQueue_PushPopUnPop(t *testing.T) {
	q := UnboundedBoxQueue()

	q.Push(&actorkit.Envelope{Data:1})
	q.Push(&actorkit.Envelope{Data:2})
	assert.Equal(t, 1, q.Pop().Data)
	assert.Equal(t, 2, q.Pop().Data)

	q.Push(&actorkit.Envelope{Data:1})
	q.Push(&actorkit.Envelope{Data:2})

	popped := q.Pop()
	assert.Equal(t, 1, popped.Data)
	q.UnPop(popped)

	popped1 := q.Pop()
	assert.False(t, q.Empty())
	popped2 := q.Pop()
	assert.NotNil(t, popped2)
	assert.True(t,  popped1 == popped)
	assert.True(t, popped2 != popped)

	assert.True(t, q.Empty())
}

func TestBoxQueue_Empty(t *testing.T) {
	q := UnboundedBoxQueue()
	assert.True(t, q.Empty())
	q.Push(&actorkit.Envelope{Data:1})
	assert.False(t, q.Empty())
}

func TestBoundedBoxQueue_PushPopUnPop(t *testing.T) {
	q := BoundedBoxQueue(3, DropOld)

	q.Push(&actorkit.Envelope{Data:1})
	q.Push(&actorkit.Envelope{Data:2})
	assert.Equal(t, 1, q.Pop().Data)
	assert.Equal(t, 2, q.Pop().Data)

	q.Push(&actorkit.Envelope{Data:1})
	q.Push(&actorkit.Envelope{Data:2})

	popped := q.Pop()
	assert.Equal(t, 1, popped.Data)
	q.UnPop(popped)

	popped1 := q.Pop()
	assert.False(t, q.Empty())
	popped2 := q.Pop()
	assert.NotNil(t, popped2)
	assert.True(t,  popped1 == popped)
	assert.True(t, popped2 != popped)

	assert.True(t, q.Empty())
}

func TestBoundedBoxQueue_Empty(t *testing.T) {
	q := BoundedBoxQueue(10, DropOld)
	assert.True(t, q.Empty())
	q.Push(&actorkit.Envelope{Data:1})
	assert.False(t, q.Empty())
}

func TestBoundedBoxQueue_DropOldest(t *testing.T) {
	q := BoundedBoxQueue(1, DropOld)
	assert.True(t, q.Empty())

	q.Push(&actorkit.Envelope{Data:1})
	assert.Equal(t, q.Total(), 1)
	q.Push(&actorkit.Envelope{Data:2})
	assert.Equal(t, q.Total(), 1)
	assert.NotEqual(t, q.Pop().Data, 1)
}

func TestBoundedBoxQueue_DropNewest(t *testing.T) {
	q := BoundedBoxQueue(1, DropNew)
	assert.True(t, q.Empty())

	q.Push(&actorkit.Envelope{Data:1})
	assert.Equal(t, q.Total(), 1)
	q.Push(&actorkit.Envelope{Data:2})
	assert.Equal(t, q.Total(), 1)
	assert.NotEqual(t, q.Pop().Data, 2)
}

func TestBoundedBoxQueue_Drop_Unpop(t *testing.T) {
	q := BoundedBoxQueue(1, DropNew)
	assert.True(t, q.Empty())

	q.Push(&actorkit.Envelope{Data:1})
	assert.Equal(t, q.Total(), 1)
	q.UnPop(&actorkit.Envelope{Data:2})
	assert.Equal(t, q.Total(), 1)

	data := q.Pop().Data
	assert.NotEqual(t, data, 1)
	assert.Equal(t, data, 2)
}
