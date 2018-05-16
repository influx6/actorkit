package mailbox

import (
	"testing"
	"github.com/stretchr/testify/assert"
)

func BenchmarkQueue_PushPopUnPop(b *testing.B) {
	b.ReportAllocs()

	q := NewMSPQueue()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		go func(){
			q.Push(env)
		}()
		q.Pop()
	}
	b.StopTimer()
}

func TestMSPQueue_PushPopUnPop(t *testing.T) {
	q := NewMSPQueue()

	q.Push(env)
	q.Push(env2)
	assert.Equal(t, 1, q.Pop().Data())
	assert.Equal(t, 2, q.Pop().Data())

	q.Push(env)
	q.Push(env2)

	popped := q.Pop()
	assert.Equal(t, 1, popped.Data())
	q.UnPop(popped)

	popped1 := q.Pop()
	assert.False(t, q.Empty())
	popped2 := q.Pop()
	assert.NotNil(t, popped2)
	assert.True(t,  popped1 == popped)
	assert.True(t, popped2 != popped)

	assert.True(t, q.Empty())
}

func TestMSPQueue_Empty(t *testing.T) {
	q := NewMSPQueue()
	assert.True(t, q.Empty())
	q.Push(env)
	assert.False(t, q.Empty())
}
