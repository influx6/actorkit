package actorkit

//***********************************************************
// RoundRobinRouter
//***********************************************************

// RoundRobinRouter implements a address router which delivers messages to giving address
// in a round robin version.
type RoundRobinRouter struct {
}

//***********************************************************
// RandomRouter
//***********************************************************

// RandomRouter implements a address router which delivers messages to giving address
// based on hash value from message to possible address.
type RandomRouter struct {
}

//***********************************************************
// HashRouter
//***********************************************************

// HashedRouter implements a address router which delivers messages to giving address
// based on hash value from message to possible address.
type HashedRouter struct {
}
