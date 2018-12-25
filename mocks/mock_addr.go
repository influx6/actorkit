package mocks

import (
	"time"

	"github.com/gokit/actorkit"
	"github.com/gokit/errors"
)

//****************************************
// Test Addr Implementation
//****************************************

type AddrImpl struct{}

func (am *AddrImpl) DeathWatch(addr actorkit.Addr) error {
	return errors.New("not supported")
}

func (am *AddrImpl) AddDiscovery(service actorkit.DiscoveryService) error {
	return errors.New("not supported")
}

func (*AddrImpl) Forward(actorkit.Envelope) error {
	return nil
}

func (*AddrImpl) SendWithHeader(interface{}, actorkit.Header, actorkit.Addr) error {
	return nil
}

func (*AddrImpl) Send(interface{}, actorkit.Addr) error {
	return nil
}

func (am *AddrImpl) Future() actorkit.Future {
	return actorkit.NewFuture(am)
}

func (am *AddrImpl) TimedFuture(d time.Duration) actorkit.Future {
	return actorkit.TimedFuture(am, d)
}

func (*AddrImpl) State() actorkit.Signal {
	return actorkit.RUNNING
}

func (*AddrImpl) Actor() actorkit.Actor {
	return nil
}

func (*AddrImpl) Spawn(service string, ops actorkit.Prop) (actorkit.Addr, error) {
	return &AddrImpl{}, nil
}

func (*AddrImpl) Service() string {
	return "mocks"
}

func (am *AddrImpl) Namespace() string {
	return "localhost"
}

func (*AddrImpl) Protocol() string {
	return "testkit"
}

func (*AddrImpl) ID() string {
	return "13232534-43434-3434345-343434343"
}

func (am *AddrImpl) ProtocolAddr() string {
	return actorkit.FormatNamespace(am.Protocol(), am.Namespace())
}

func (m *AddrImpl) Addr() string {
	return actorkit.FormatAddrService(m.Protocol(), m.Namespace(), m.ID(), m.Service())
}

func (*AddrImpl) Watch(func(interface{})) actorkit.Subscription {
	return nil
}

func (am *AddrImpl) GetAddr(addr string) (actorkit.Addr, error) {
	return am, nil
}

func (am *AddrImpl) GetChild(id string, subID ...string) (actorkit.Addr, error) {
	return am, nil
}

func (*AddrImpl) Children() []actorkit.Addr {
	return nil
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
