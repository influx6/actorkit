package platform

import (
	"os"
	"os/signal"
	"syscall"

	"github.com/gokit/actorkit"
)

// AwaitActorInterrupt will setup signalling for use in a commandline application
// waiting for ctrl-c or a SIGTERM, SIGINT or SIGKILL signal to gracefully stop provided actor.
// It is a blocking call and will block till signal is received.
func AwaitActorInterrupt(actor actorkit.Actor) {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Kill, syscall.SIGTERM, syscall.SIGINT, syscall.SIGKILL, syscall.SIGQUIT)
	<-signals
	actor.Stop().Wait()
}

// AwaitAddrInterrupt will setup signalling for use in a commandline application
// waiting for ctrl-c or a SIGTERM, SIGINT or SIGKILL signal to gracefully stop provided actor targeted by
// provided address.
// It is a blocking call and will block till signal is received.
func AwaitAddrInterrupt(addr actorkit.Addr) {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Kill, syscall.SIGTERM, syscall.SIGINT, syscall.SIGKILL, syscall.SIGQUIT)
	<-signals
	actorkit.Poison(addr).Wait()
}

// KillAwaitActorInterrupt will setup signalling for use in a commandline application
// waiting for ctrl-c or a SIGTERM, SIGINT or SIGKILL signal to kill provided actor.
// It is a blocking call and will block till signal is received.
func KillAwaitActorInterrupt(actor actorkit.Actor) {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Kill, syscall.SIGTERM, syscall.SIGINT, syscall.SIGKILL, syscall.SIGQUIT)
	<-signals
	actor.Kill().Wait()
}

// KillAwaitAddrInterrupt will setup signalling for use in a commandline application
// waiting for ctrl-c or a SIGTERM, SIGINT or SIGKILL signal to kill provided actor targeted by
// provided address.
// It is a blocking call and will block till signal is received.
func KillAwaitAddrInterrupt(addr actorkit.Addr) {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Kill, syscall.SIGTERM, syscall.SIGINT, syscall.SIGKILL, syscall.SIGQUIT)
	<-signals
	actorkit.Kill(addr).Wait()
}

// DestroyAwaitActorInterrupt will setup signalling for use in a commandline application
// waiting for ctrl-c or a SIGTERM, SIGINT or SIGKILL signal to destroy provided actor.
// It is a blocking call and will block till signal is received.
func DestroyAwaitActorInterrupt(actor actorkit.Actor) {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Kill, syscall.SIGTERM, syscall.SIGINT, syscall.SIGKILL, syscall.SIGQUIT)
	<-signals
	actor.Destroy().Wait()
}

// DestroyAwaitAddrInterrupt will setup signalling for use in a commandline application
// waiting for ctrl-c or a SIGTERM, SIGINT or SIGKILL signal to destroy provided actor targeted by
// provided address.
// It is a blocking call and will block till signal is received.
func DestroyAwaitAddrInterrupt(addr actorkit.Addr) {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Kill, syscall.SIGTERM, syscall.SIGINT, syscall.SIGKILL, syscall.SIGQUIT)
	<-signals
	actorkit.Destroy(addr).Wait()
}
