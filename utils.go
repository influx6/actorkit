package actorkit

import "time"

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

// LinearDoUntil will continuously run the giving function until no error is returned.
// If duration is supplied, the goroutine is made to sleep before making next run.
// The same duration is consistently used for each sleep.
func LinearDoUntil(fx func() error, total int, elapse time.Duration) {
	for i := total; i > 0; i-- {
		if err := fx(); err == nil {
			return
		}

		if elapse > 0 {
			time.Sleep(elapse)
		}
	}
}
