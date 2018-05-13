package actorkit

import "github.com/rs/xid"

//**********************************************
//  noActorProcess implements Wait and Process
//**********************************************

// noActorProcess implements a no-action Process
// which can be used as stand-in for processes that
// do nothing.
type noActorProcess struct{
	id xid.ID
}

func newNoActorProcess() *noActorProcess{
	return &noActorProcess{id:xid.New()}
}

func (m noActorProcess) Wait()  {}
func (m noActorProcess) Stop()  {}
func (m noActorProcess) Receive(env Envelope) {}
func (m noActorProcess) Stopped() bool  {return true}
func (m noActorProcess) GracefulStop()  Waiter { return m}
func (m noActorProcess) ID() string  {
	return m.id.String()
}
