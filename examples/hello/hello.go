package main

import (
	"github.com/gokit/actorkit"
	"github.com/gokit/actorkit/actors"
	"fmt"
)

type HelloOp struct{
	Started chan bool
	ShuttingDown chan bool
	FinishedShutdown chan bool
	Envelope chan actorkit.Envelope
}

type HelloMessage struct{
	Name string
}

func (h HelloOp) Respond(me actorkit.Mask,e actorkit.Envelope, d actorkit.Distributor){
	switch e.Data().(type) {
	case *actorkit.ProcessStarted:
		h.Started <- true
	case *actorkit.ProcessShuttingDown:
		h.ShuttingDown <- true
	case *actorkit.ProcessFinishedShutDown:
		h.FinishedShutdown <- true
	case *HelloMessage:
		h.Envelope <- e
	}
}

func main(){

	started := make(chan bool, 1)
	shutdown := make(chan bool, 1)
	finished := make(chan bool, 1)
	envelope := make(chan actorkit.Envelope,1)

	ax := actors.FromActor(&HelloOp{
		Started: started,
		ShuttingDown: shutdown,
		FinishedShutdown: finished,
		Envelope: envelope,
	})

	axMask := actorkit.ForceMaskWithProcess("local", "yay", ax)
	axMask.Send(&HelloMessage{Name:"Wally"}, actorkit.GetDeadletter())

	env := <-envelope
	ax.GracefulStop().Wait()

	fmt.Printf("Received: %#v\n", env.Data())
}
