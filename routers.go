package actorkit

//***********************************************************
// RoundRobinRouter
//***********************************************************

// RoundRobinRouter implements a router which delivers messages to giving address
// in a round robin version.
type RoundRobinRouter struct {
}

//***********************************************************
// BroadcastRouter
//***********************************************************

// BroadcastRouter implements a router which delivers messages in a fan-out
// manager to giving router.
type BroadcastRouter struct {
}

//***********************************************************
// RandomRouter
//***********************************************************

// RandomRouter implements a router which delivers messages to giving address
// based on hash value from message to possible address.
type RandomRouter struct {
}

//***********************************************************
// HashedRouter
//***********************************************************

// HashedRouter implements a router which delivers messages to giving address
// based on hash value from message to possible address.
type HashedRouter struct {
}
