package actorkit

// OneForOneSupervisor implements a one-to-one supervising strategy for giving actors.
type OneForOneSupervisor struct {
}

// Handle implements the Supervisor interface and provides the algorithm logic for the
// one-for-one monitoring strategy, where a failed actor is dealt with singularly without affecting
// it's children.
func (sp *OneForOneSupervisor) Handle(err interface{}, targetAddr Addr, target Actor, parent Actor) {

}
