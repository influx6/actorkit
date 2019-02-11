// Package runtimes implements different management runtime which connects to different communication channels to initiate
// and distribute work for a deployed actorkit system.
package runtimes

type Proc struct {
	Event string `json:"event"`
	Reservation
}
