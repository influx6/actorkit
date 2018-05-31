package actorkit

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewLocalResolver(t *testing.T) {
	lr := NewLocalResolver()

	proc1 := newNoActorProcess()
	proc2 := newNoActorProcess()
	proc3 := newNoActorProcess()

	lr.Register(proc1, "sum")
	lr.Register(proc2, "sum")
	lr.Register(proc3, "mut")

	sumAddr := NewMask("sum")
	mutAddr := NewMask("mut")

	proc, found := lr.Resolve(sumAddr)
	assert.True(t, found)
	assert.NotNil(t, proc)
	assert.NotEqual(t, proc, proc3)
	assert.True(t, proc == proc1 || proc == proc2)

	procx, found := lr.Resolve(sumAddr)
	assert.True(t, found)
	assert.NotNil(t, procx)
	assert.NotEqual(t, procx, proc3)
	assert.True(t, procx == proc1 || procx == proc2)

	procy, found := lr.Resolve(mutAddr)
	assert.True(t, found)
	assert.NotNil(t, procy)
	assert.NotEqual(t, procy, proc1)
	assert.NotEqual(t, procy, proc2)
}
