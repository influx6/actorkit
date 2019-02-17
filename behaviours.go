package actorkit

// DeadLetterOp implements a behaviour which forwards
// all messages to te deadletter mail box.
type DeadLetterOp struct{}

// Action implements the Op interface.
func (DeadLetterOp) Action(_ Addr, msg Envelope) {
	DeadLetters().Forward(msg)
}
