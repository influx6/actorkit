Actorkit
------------
[![Go Report Card](https://goreportcard.com/badge/github.com/gokit/actorkit)](https://goreportcard.com/report/github.com/gokit/actorkit)
[![Travis Build](https://travis-ci.org/gokit/actorkit.svg?branch=master)](https://travis-ci.org/gokit/actorkit#)

Actorkit implements a helper package for writing actor based concurrency. Actorkit took alot of inspirations from
the [Akka](https://akka.io) and [Proto.Actor](http://proto.actor/) projects.

## Install

```bash
go get github.com/gokit/actorkit
```


## Hello World

```go
import (
	"fmt"

	"github.com/gokit/actorkit"
)

type HelloMessage struct {
	Name string
}

type HelloOp struct {
	Done chan struct{}
}

func (h *HelloOp) Action(me actorkit.Addr, e actorkit.Envelope) {
	switch mo := e.Data.(type) {
	case HelloMessage:
		fmt.Printf("Hello World %q\n", mo.Name)
	}
}

func main() {
	addr, _, err := actorkit.System(&HelloOp{}, "kit", "localhos:0", nil)
	if err != nil {
		panic(err)
	}

	addr.Send(HelloMessage{Name: "Wally"}, actorkit.Header{}, actorkit.DeadLetters())
	actorkit.Destroy(addr, nil).Wait()
}
```
