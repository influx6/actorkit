package actorkit

import (
	"sync"
	"time"
)

const (
	digits = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ~+"
)

func uint64ToID(u uint64) string {
	var buf [13]byte
	i := 13
	// base is power of 2: use shifts and addresss instead of / and %
	for u >= 64 {
		i--
		buf[i] = digits[uintptr(u)&0x3f]
		u >>= 6
	}
	// u < base
	i--
	buf[i] = digits[uintptr(u)]
	i--
	buf[i] = '$'

	return string(buf[i:])
}

// waitTillRunned will executed function in goroutine but will
// block till when goroutine is scheduled and started.
func waitTillRunned(fx func()) {
	var w sync.WaitGroup
	w.Add(1)
	go func() {
		w.Done()
		fx()
	}()
	w.Wait()
}

// linearDoUntil will continuously run the giving function until no error is returned.
// If duration is supplied, the goroutine is made to sleep before making next run.
// The same duration is consistently used for each sleep.
func linearDoUntil(fx func() error, total int, elapse time.Duration) error {
	var err error
	for i := total; i > 0; i-- {
		if err = fx(); err == nil {
			return nil
		}

		if elapse > 0 {
			time.Sleep(elapse)
		}
	}
	return nil
}
