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
	addr, err := actorkit.Ancestor("kit", "localhost:0")
	if err != nil {
		panic(err)
	}

	hello, err := addr.Spawn("hello", &HelloOp{})
	if err != nil {
		panic(err)
	}

	if err := hello.Send(HelloMessage{Name: "Wally"}, actorkit.DeadLetters()); err != nil {
		panic(err)
	}

	if err := actorkit.Poison(addr); err != nil {
		panic(err)
	}
}
