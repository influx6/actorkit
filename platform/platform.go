package platform

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/gokit/actorkit"
)

// AwaitActorInterrupt will setup signalling for use in a commandline application
// waiting for ctrl-c or a SIGTERM, SIGINT or SIGKILL signal to destroy provided actor.
// It is a blocking call and will block till signal is received.
func AwaitActorInterrupt(actor actorkit.Actor) {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Kill, syscall.SIGTERM, syscall.SIGINT, syscall.SIGKILL, syscall.SIGQUIT)
	<-signals
	actor.Destroy().Wait()
}

// AwaitAddrInterrupt will setup signalling for use in a commandline application
// waiting for ctrl-c or a SIGTERM, SIGINT or SIGKILL signal to destroy provided actor targeted by
// provided address.
// It is a blocking call and will block till signal is received.
func AwaitAddrInterrupt(addr actorkit.Addr) {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Kill, syscall.SIGTERM, syscall.SIGINT, syscall.SIGKILL, syscall.SIGQUIT)
	<-signals
	fmt.Println("Stopping")
	actorkit.Destroy(addr).Wait()
}
