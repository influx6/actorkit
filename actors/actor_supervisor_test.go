package actors

import (
	"testing"
	"github.com/gokit/actorkit"
)

type HelloOp struct{}

func (h HelloOp) Respond(e actorkit.Envelope, d actorkit.Distributor){
	switch e.Data().(type) {
	case *actorkit.ProcessStarted:
	case *actorkit.ProcessShuttingDown:
	case *actorkit.ProcessFinishedShutDown:
	}
}

func TestFromActor(t *testing.T) {

}

func TestFromFunc(t *testing.T) {

}
