package actorkit_test

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/gokit/actorkit"
	"github.com/stretchr/testify/assert"
)

func TestExponentialBackoffRestartSupervisor(t *testing.T) {
	supervisor := actorkit.ExponentialBackOffRestartStrategy(10, 1*time.Second, nil)
	system, err := actorkit.Ancestor("kit", "localhost")
	assert.NoError(t, err)
	assert.NotNil(t, system)

	child1, err := system.Spawn("basic", &basic{})
	assert.NoError(t, err)
	assert.NotNil(t, child1)

	child2, err := system.Spawn("basic", &basic{})
	assert.NoError(t, err)
	assert.NotNil(t, child2)

	assert.True(t, child1.Actor().Running())
	assert.True(t, child2.Actor().Running())

	var w sync.WaitGroup
	w.Add(2)
	sub := child1.Watch(func(i interface{}) {
		switch i.(type) {
		case actorkit.ActorRestartRequested:
			w.Done()
		case actorkit.ActorRestarted:
			w.Done()
		}
	})

	supervisor.Handle(errors.New("bad day"), child1, child1.Actor(), system.Actor())

	assert.True(t, child1.Actor().Running())
	assert.True(t, child2.Actor().Running())

	w.Wait()
	sub.Stop()

	child1.Actor().Destroy()
	child2.Actor().Destroy()
}

func TestRestartSupervisor(t *testing.T) {
	supervisor := &actorkit.RestartingSupervisor{}
	system, err := actorkit.Ancestor("kit", "localhost")
	assert.NoError(t, err)
	assert.NotNil(t, system)

	child1, err := system.Spawn("basic", &basic{})
	assert.NoError(t, err)
	assert.NotNil(t, child1)

	child2, err := system.Spawn("basic", &basic{})
	assert.NoError(t, err)
	assert.NotNil(t, child2)

	assert.True(t, child1.Actor().Running())
	assert.True(t, child2.Actor().Running())

	var w sync.WaitGroup
	w.Add(2)
	sub := child1.Watch(func(i interface{}) {
		switch i.(type) {
		case actorkit.ActorRestartRequested:
			w.Done()
		case actorkit.ActorRestarted:
			w.Done()
		}
	})

	supervisor.Handle(errors.New("bad day"), child1, child1.Actor(), system.Actor())

	assert.True(t, child1.Actor().Running())
	assert.True(t, child2.Actor().Running())

	w.Wait()
	sub.Stop()

	child1.Actor().Destroy()
	child2.Actor().Destroy()
}

func TestOneForOneSupervisor(t *testing.T) {
	var supervisingAction func(interface{}) actorkit.Directive

	supervisor := &actorkit.OneForOneSupervisor{
		Max: 30,
		PanicAction: func(i interface{}, addr actorkit.Addr, actor actorkit.Actor) {
			assert.NotNil(t, i)
			assert.IsType(t, actorkit.ActorRoutinePanic{}, i)
		},
		Direction: func(tm interface{}) actorkit.Directive {
			return supervisingAction(tm)
		},
	}

	system, err := actorkit.Ancestor("kit", "localhost")
	assert.NoError(t, err)
	assert.NotNil(t, system)

	t.Logf("When supervisor is told destroy")
	{
		child1, err := system.Spawn("basic", &basic{})
		assert.NoError(t, err)
		assert.NotNil(t, child1)

		child2, err := system.Spawn("basic", &basic{})
		assert.NoError(t, err)
		assert.NotNil(t, child2)

		assert.True(t, child1.Actor().Running())
		assert.True(t, child2.Actor().Running())

		var w sync.WaitGroup
		w.Add(2)
		sub := child1.Watch(func(i interface{}) {
			switch i.(type) {
			case actorkit.ActorDestroyRequested:
				w.Done()
			case actorkit.ActorDestroyed:
				w.Done()
			}
		})

		supervisingAction = func(i interface{}) actorkit.Directive {
			return actorkit.DestroyDirective
		}

		supervisor.Handle(errors.New("bad day"), child1, child1.Actor(), system.Actor())

		assert.False(t, child1.Actor().Running())
		assert.True(t, child2.Actor().Running())

		w.Wait()
		sub.Stop()
	}

	t.Logf("When supervisor is told kill")
	{
		child1, err := system.Spawn("basic", &basic{})
		assert.NoError(t, err)
		assert.NotNil(t, child1)

		child2, err := system.Spawn("basic", &basic{})
		assert.NoError(t, err)
		assert.NotNil(t, child2)

		assert.True(t, child1.Actor().Running())
		assert.True(t, child2.Actor().Running())

		var w sync.WaitGroup
		w.Add(2)
		sub := child1.Watch(func(i interface{}) {
			switch i.(type) {
			case actorkit.ActorKillRequested:
				w.Done()
			case actorkit.ActorKilled:
				w.Done()
			}
		})

		supervisingAction = func(i interface{}) actorkit.Directive {
			return actorkit.KillDirective
		}

		supervisor.Handle(errors.New("bad day"), child1, child1.Actor(), system.Actor())

		assert.False(t, child1.Actor().Running())
		assert.True(t, child2.Actor().Running())

		w.Wait()
		sub.Stop()
		child1.Actor().Destroy()
		child2.Actor().Destroy()
	}

	t.Logf("When supervisor is told stop")
	{
		child1, err := system.Spawn("basic", &basic{})
		assert.NoError(t, err)
		assert.NotNil(t, child1)

		child2, err := system.Spawn("basic", &basic{})
		assert.NoError(t, err)
		assert.NotNil(t, child2)

		assert.True(t, child1.Actor().Running())
		assert.True(t, child2.Actor().Running())

		var w sync.WaitGroup
		w.Add(2)
		sub := child1.Watch(func(i interface{}) {
			switch i.(type) {
			case actorkit.ActorStopRequested:
				w.Done()
			case actorkit.ActorStopped:
				w.Done()
			}
		})

		supervisingAction = func(i interface{}) actorkit.Directive {
			return actorkit.StopDirective
		}

		supervisor.Handle(errors.New("bad day"), child1, child1.Actor(), system.Actor())

		assert.False(t, child1.Actor().Running())
		assert.True(t, child2.Actor().Running())

		w.Wait()
		sub.Stop()
		child1.Actor().Destroy()
		child2.Actor().Destroy()
	}

	t.Logf("When supervisor is told restart")
	{
		child1, err := system.Spawn("basic", &basic{})
		assert.NoError(t, err)
		assert.NotNil(t, child1)

		child2, err := system.Spawn("basic", &basic{})
		assert.NoError(t, err)
		assert.NotNil(t, child2)

		assert.True(t, child1.Actor().Running())
		assert.True(t, child2.Actor().Running())

		var w sync.WaitGroup
		w.Add(2)
		sub := child1.Watch(func(i interface{}) {
			switch i.(type) {
			case actorkit.ActorRestartRequested:
				w.Done()
			case actorkit.ActorRestarted:
				w.Done()
			}
		})

		supervisingAction = func(i interface{}) actorkit.Directive {
			return actorkit.RestartDirective
		}

		supervisor.Handle(errors.New("bad day"), child1, child1.Actor(), system.Actor())

		assert.True(t, child1.Actor().Running())
		assert.True(t, child2.Actor().Running())

		w.Wait()
		sub.Stop()

		child1.Actor().Destroy()
		child2.Actor().Destroy()
	}
}

func TestAllForOneSupervisor(t *testing.T) {
	var supervisingAction func(interface{}) actorkit.Directive

	supervisor := &actorkit.AllForOneSupervisor{
		Max: 30,
		PanicAction: func(i interface{}, addr actorkit.Addr, actor actorkit.Actor) {
			assert.NotNil(t, i)
			assert.IsType(t, actorkit.ActorRoutinePanic{}, i)
		},
		Direction: func(tm interface{}) actorkit.Directive {
			return supervisingAction(tm)
		},
	}

	system, err := actorkit.Ancestor("kit", "localhost")
	assert.NoError(t, err)
	assert.NotNil(t, system)

	t.Logf("When supervisor is told destroy")
	{
		child1, err := system.Spawn("basic", &basic{})
		assert.NoError(t, err)
		assert.NotNil(t, child1)

		child2, err := system.Spawn("basic", &basic{})
		assert.NoError(t, err)
		assert.NotNil(t, child2)

		assert.True(t, child1.Actor().Running())
		assert.True(t, child2.Actor().Running())

		var w sync.WaitGroup
		w.Add(2)
		sub := child1.Watch(func(i interface{}) {
			switch i.(type) {
			case actorkit.ActorDestroyRequested:
				w.Done()
			case actorkit.ActorDestroyed:
				w.Done()
			}
		})

		supervisingAction = func(i interface{}) actorkit.Directive {
			return actorkit.DestroyDirective
		}

		supervisor.Handle(errors.New("bad day"), child1, child1.Actor(), system.Actor())

		assert.False(t, child1.Actor().Running())
		assert.False(t, child2.Actor().Running())

		w.Wait()
		sub.Stop()
	}

	t.Logf("When supervisor is told kill")
	{
		child1, err := system.Spawn("basic", &basic{})
		assert.NoError(t, err)
		assert.NotNil(t, child1)

		child2, err := system.Spawn("basic", &basic{})
		assert.NoError(t, err)
		assert.NotNil(t, child2)

		assert.True(t, child1.Actor().Running())
		assert.True(t, child2.Actor().Running())

		var w sync.WaitGroup
		w.Add(2)
		sub := child1.Watch(func(i interface{}) {
			switch i.(type) {
			case actorkit.ActorKillRequested:
				w.Done()
			case actorkit.ActorKilled:
				w.Done()
			}
		})

		supervisingAction = func(i interface{}) actorkit.Directive {
			return actorkit.KillDirective
		}

		supervisor.Handle(errors.New("bad day"), child1, child1.Actor(), system.Actor())

		assert.False(t, child1.Actor().Running())
		assert.False(t, child2.Actor().Running())

		w.Wait()
		sub.Stop()
		child1.Actor().Destroy()
		child2.Actor().Destroy()
	}

	t.Logf("When supervisor is told stop")
	{
		child1, err := system.Spawn("basic", &basic{})
		assert.NoError(t, err)
		assert.NotNil(t, child1)

		child2, err := system.Spawn("basic", &basic{})
		assert.NoError(t, err)
		assert.NotNil(t, child2)

		assert.True(t, child1.Actor().Running())
		assert.True(t, child2.Actor().Running())

		var w sync.WaitGroup
		w.Add(2)
		sub := child1.Watch(func(i interface{}) {
			switch i.(type) {
			case actorkit.ActorStopRequested:
				w.Done()
			case actorkit.ActorStopped:
				w.Done()
			}
		})

		supervisingAction = func(i interface{}) actorkit.Directive {
			return actorkit.StopDirective
		}

		supervisor.Handle(errors.New("bad day"), child1, child1.Actor(), system.Actor())

		assert.False(t, child1.Actor().Running())
		assert.False(t, child2.Actor().Running())

		w.Wait()
		sub.Stop()
		child1.Actor().Destroy()
		child2.Actor().Destroy()
	}

	t.Logf("When supervisor is told restart")
	{
		child1, err := system.Spawn("basic", &basic{})
		assert.NoError(t, err)
		assert.NotNil(t, child1)

		child2, err := system.Spawn("basic", &basic{})
		assert.NoError(t, err)
		assert.NotNil(t, child2)

		assert.True(t, child1.Actor().Running())
		assert.True(t, child2.Actor().Running())

		var w sync.WaitGroup
		w.Add(2)
		sub := child1.Watch(func(i interface{}) {
			switch i.(type) {
			case actorkit.ActorRestartRequested:
				w.Done()
			case actorkit.ActorRestarted:
				w.Done()
			}
		})

		supervisingAction = func(i interface{}) actorkit.Directive {
			return actorkit.RestartDirective
		}

		supervisor.Handle(errors.New("bad day"), child1, child1.Actor(), system.Actor())

		assert.True(t, child1.Actor().Running())
		assert.True(t, child2.Actor().Running())

		w.Wait()
		sub.Stop()

		child1.Actor().Destroy()
		child2.Actor().Destroy()
	}
}
