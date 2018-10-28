package platform

import (
	"os"
	"os/signal"
	"syscall"

	"github.com/gokit/actorkit"
)

// WaitTillInterrupt will setup signalling for use in a commandline application
// waiting for ctrl-c or a SIGTERM, SIGINT or SIGKILL signal to destroy provided actor.
// It is a blocking call and will block till signal is received.
func WaitTillInterrupt(actor actorkit.Actor) {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Kill, syscall.SIGTERM, syscall.SIGINT, syscall.SIGKILL, syscall.SIGQUIT)
	<-signals
	actor.Destroy(nil).Wait()
}

// WaitTillAddrInterrupt will setup signalling for use in a commandline application
// waiting for ctrl-c or a SIGTERM, SIGINT or SIGKILL signal to destroy provided actor targeted by
// provided address.
// It is a blocking call and will block till signal is received.
func WaitTillAddrInterrupt(addr actorkit.Addr) {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Kill, syscall.SIGTERM, syscall.SIGINT, syscall.SIGKILL, syscall.SIGQUIT)
	<-signals
	actorkit.Destroy(addr, nil)
}
