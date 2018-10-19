package mailbox

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func BenchmarkQueue_PushPopUnPop(b *testing.B) {
	b.ReportAllocs()

	q := NewMSPQueue(-1, DropNew)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		go func() {
			q.Push(env)
		}()
		q.Pop()
	}
	b.StopTimer()
}

func TestMSPQueue_PushPopUnPop(t *testing.T) {
	q := NewMSPQueue(-1, DropNew)

	q.Push(env)
	q.Push(env2)

	popped, err := q.Pop()
	assert.NoError(t, err)
	assert.Equal(t, 1, popped.Data)
	assert.Equal(t, 2, popped.Data)

	q.Push(env)
	q.Push(env2)

	popped, err = q.Pop()
	assert.NoError(t, err)
	assert.Equal(t, 1, popped.Data)
	q.UnPop(popped)

	popped1, err := q.Pop()
	assert.NoError(t, err)
	assert.False(t, q.Empty())
	popped2, err := q.Pop()
	assert.NoError(t, err)
	assert.NotNil(t, popped2)
	assert.True(t, popped1.Data == popped.Data)
	assert.True(t, popped2.Data != popped.Data)

	assert.True(t, q.Empty())
}

func TestMSPQueue_Empty(t *testing.T) {
	q := NewMSPQueue(-1, DropNew)
	assert.True(t, q.Empty())
	q.Push(env)
	assert.False(t, q.Empty())
}
