package actorkit

//***********************************************************
// RoundRobinRouter
//***********************************************************

// RoundRobinRouter implements a router which delivers messages to giving address
// in a round robin version.
type RoundRobinRouter struct {
}

// Action implements the Behaviour interface.
func (rr *RoundRobinRouter) Action(addr Addr, msg Envelope) {}

//***********************************************************
// BroadcastRouter
//***********************************************************

// BroadcastRouter implements a router which delivers messages in a fan-out
// manager to giving router.
type BroadcastRouter struct {
}

// Action implements the Behaviour interface.
func (br *BroadcastRouter) Action(addr Addr, msg Envelope) {}

//***********************************************************
// RandomRouter
//***********************************************************

// RandomRouter implements a router which delivers messages to giving address
// based on hash value from message to possible address.
type RandomRouter struct {
}

// Action implements the Behaviour interface.
func (br *RandomRouter) Action(addr Addr, msg Envelope) {}

//***********************************************************
// HashedRouter
//***********************************************************

// HashedRouter implements a router which delivers messages to giving address
// based on hash value from message to possible address.
type HashedRouter struct {
}

// Action implements the Behaviour interface.
func (br *HashedRouter) Action(addr Addr, msg Envelope) {}
