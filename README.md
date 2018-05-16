Actorkit
------------
[![Go Report Card](https://goreportcard.com/badge/github.com/gokit/actorkit)](https://goreportcard.com/report/github.com/gokit/actorkit)
[![Travis Build](https://travis-ci.org/gokit/actorkit.svg?branch=master)](https://travis-ci.org/gokit/actorkit#)

Actorkit implements a helper package for writing actor based concurrency.


## Install

```bash
go get github.com/gokit/actorkit
```


## Hello World

Using actorkit is pretty easy and below is the usual hello word sample with a twist.

```go
import (
	"github.com/gokit/actorkit"
	"github.com/gokit/actorkit/actors"
	"fmt"
)

type HelloOp struct{
	Started chan bool
	ShuttingDown chan bool
	FinishedShutdown chan bool
	Envelope chan string
}

type HelloMessage struct{
	Name string
}

func (h HelloOp) Respond(me actorkit.Mask,e actorkit.Envelope, d actorkit.Distributor){
	switch mo := e.Data().(type) {
	case *actorkit.ProcessStarted:
		h.Started <- true
	case *actorkit.ProcessShuttingDown:
		h.ShuttingDown <- true
	case *actorkit.ProcessFinishedShutDown:
		h.FinishedShutdown <- true
	case *HelloMessage:
		h.Envelope <- fmt.Sprintf("Hello World %q", mo.Name)
	}
}

func main(){

	started := make(chan bool, 1)
	shutdown := make(chan bool, 1)
	finished := make(chan bool, 1)
	envelope := make(chan string,1)

	ax := actors.FromActor(&HelloOp{
		Started: started,
		ShuttingDown: shutdown,
		FinishedShutdown: finished,
		Envelope: envelope,
	})

	axMask := actorkit.ForceMaskWithProcess("local:0", "hello.service", ax)
	axMask.Send(&HelloMessage{Name:"Wally"}, actorkit.GetDeadletter())

	env := <-envelope
	fmt.Printf("Received: %#v\n", env)

	ax.GracefulStop().Wait()
}

```
