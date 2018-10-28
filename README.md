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

type HelloMessage struct{
	Name string
}

type HelloOp struct{}


func (h HelloOp) Action(me actorkit.Addr, e actorkit.Envelope){
	switch mo := e.Data().(type) {
	case *HelloMessage:
		fmt.Sprintf("Hello World %q", mo.Name)
	}
}

func main(){

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
