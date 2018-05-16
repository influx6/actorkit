package actorkit

import (
	"sync"
	"errors"
	"time"
	"github.com/rs/xid"
)

var (
	ErrFutureTimeout = errors.New("future has timed out")
)

type futureActor struct{
	id xid.ID
	mask Mask
	target Mask

	fl sync.Mutex
	err error
	do sync.Once
	result Envelope

	timeout time.Duration
	wg sync.WaitGroup
	res chan Envelope
}

func resolvedFuture(value Envelope, m Mask) *futureActor {
	fa := new(futureActor)
	fa.target = m
	fa.id = xid.New()
	fa.result = value
	fa.mask = newMask(AnyNetworkAddr, "future-srv", ResolveAlways(fa))
	return fa
}

func resolvedFutureWithError(value error, m Mask) *futureActor {
	fa := new(futureActor)
	fa.target = m
	fa.id = xid.New()
	fa.err = value
	fa.mask = newMask(AnyNetworkAddr, "future-srv", ResolveAlways(fa))
	return fa
}

func newFutureActor(d time.Duration, target Mask) *futureActor {
	fa := new(futureActor)
	fa.timeout = d
	fa.id = xid.New()
	fa.target = target
	fa.res = make(chan Envelope, 1)
	fa.mask = newMask(AnyNetworkAddr, "future-srv", ResolveAlways(fa))
	return fa
}

func (f *futureActor) ID()string  {
	return f.id.String()
}

func (f *futureActor) Wait()  {
	f.wg.Wait()
}

// GracefulStop is not supported for a future, has it must
// be either resolved by a timeout or by a response.
func (f *futureActor) GracefulStop() Waiter  {
	return f
}

func (f *futureActor) Stopped() bool  {
	return false
}

// Stop is not supported for a future, has it must
// be either resolved by a timeout or by a response.
func (f *futureActor) Stop()  {
	return
}

// Mask returns the address of future resolver.
func (f *futureActor) Mask() Mask {
	return f.mask
}

// Addr returns the address of future resolver.
func (f *futureActor) Addr() Mask {
	return f.target
}

// Receive resolves future with Envelope.Data.
func (f *futureActor) Receive(through Mask, en Envelope)  {
	f.do.Do(func(){
		f.res <- en
	})
}

// Err returns associated error received for
// future if failed or due timeout.
func (f *futureActor) Err() error {
	f.fl.Lock()
	defer f.fl.Unlock()
	return f.err
}

// Result returns associated result which resolved
// future.
func (f *futureActor) Result() Envelope {
	f.fl.Lock()
	defer f.fl.Unlock()
	return f.result
}

func (f *futureActor) start()  {
	f.wg.Add(1)
	go f.run()
}

func (f *futureActor) run()  {
	defer f.wg.Done()
	for {
		select {
		 case <-time.After(f.timeout):
		 	f.fl.Lock()
			f.err = ErrFutureTimeout
			f.fl.Unlock()
			 return
		 case res := <-f.res:
			 f.fl.Lock()
			 f.result = res
			 f.fl.Unlock()
			 return
		}
	}
}
