package actorkit_test

import (
	"time"

	"github.com/gokit/actorkit"
)

//****************************************
// Test Addr Implementation
//****************************************

type AddrImpl struct{}

func (*AddrImpl) Forward(actorkit.Envelope) error {
	return nil
}

func (*AddrImpl) Send(interface{}, actorkit.Header, actorkit.Addr) error {
	return nil
}

func (am *AddrImpl) Future() actorkit.Future {
	return actorkit.NewFuture(am)
}

func (am *AddrImpl) TimedFuture(d time.Duration) actorkit.Future {
	return actorkit.TimedFuture(am, d)
}

func (*AddrImpl) Service() string {
	return "addr"
}

func (*AddrImpl) Spawn(service string, bh actorkit.Behaviour, initial interface{}) (actorkit.Addr, error) {
	return &AddrImpl{}, nil
}

func (*AddrImpl) ID() string {
	return "aaabb"
}

func (*AddrImpl) Watch(func(interface{})) actorkit.Subscription {
	return nil
}

func (*AddrImpl) Children() []actorkit.Addr {
	return nil
}

func (*AddrImpl) Addr() string {
	return "aaa-bbb"
}

func (*AddrImpl) Escalate(interface{}) {
}

func (am *AddrImpl) Parent() actorkit.Addr {
	return am
}

func (am *AddrImpl) Ancestor() actorkit.Addr {
	return am
}

func (am *AddrImpl) AddressOf(string, bool) (actorkit.Addr, error) {
	return am, nil
}