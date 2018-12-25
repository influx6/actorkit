package actorkit_test

import "github.com/gokit/actorkit"

func isRunning(s actorkit.State) bool {
	return actorkit.RUNNING == s.State()
}

func isKilled(s actorkit.State) bool {
	return actorkit.KILLED == s.State()
}

func isStopped(s actorkit.State) bool {
	return actorkit.STOPPED == s.State()
}

func isDestroyed(s actorkit.State) bool {
	return actorkit.DESTROYED == s.State()
}
