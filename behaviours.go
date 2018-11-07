package actorkit

// DeadLetterBehaviour implements a behaviour which forwards
// all messages to te deadletter mail box.
type DeadLetterBehaviour struct{}

// Action implements the Behaviour interface.
func (DeadLetterBehaviour) Action(_ Addr, msg Envelope) {
	DeadLetters().Forward(msg)
}
