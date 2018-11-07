package main

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
	addr, _, err := actorkit.System(&HelloOp{}, "kit", "localhost:0")
	if err != nil {
		panic(err)
	}

	addr.Send(HelloMessage{Name: "Wally"}, actorkit.Header{}, actorkit.DeadLetters())
	actorkit.Destroy(addr).Wait()
}
