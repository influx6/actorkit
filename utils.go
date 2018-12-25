package actorkit

import (
	"sync"
	"time"
)

const (
	digits = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ~+"
)

//****************************************************************
// Formatter functions
//****************************************************************

// FormatAddrChild returns the official address format for which
// the actorkit package uses for representing the parent + child address
// value.
func FormatAddrChild(parentAddr string, childID string) string {
	return parentAddr + "/" + childID
}

// FormatNamespace returns the official namespace format for which
// the actorkit package uses for representing the protocol+namespace
// value for a actor or it's addresses.
func FormatNamespace(protocol string, namespace string) string {
	return protocol + "@" + namespace
}

// FormatService returns the official ProtocolAddr format for which
// the actorkit package uses for representing the protocol+namespace+service
// value for a actor or it's addresses.
func FormatService(protocol string, namespace string, service string) string {
	return protocol + "@" + namespace + "/" + service
}

// FormatAddr returns the official address format for which
// the actorkit package uses for representing the protocol+namespace+uuid
// value for a actor or it's addresses.
func FormatAddr(protocol string, namespace string, id string) string {
	return protocol + "@" + namespace + "/" + id
}

// FormatAddrService returns the official address format for which the
// actorkit package uses for formatting a actor's service address format.
func FormatAddrService(protocol string, namespace string, id string, service string) string {
	return protocol + "@" + namespace + "/" + id + "/" + service
}

// formatAddr2 returns the second official address format for which
// the actorkit package uses for representing the formatted_addr+uuid
// value for a actor.
func formatAddr2(addr string, id string) string {
	return addr + "/" + id
}

// formatAddrService2 returns the official address format for which the
// actorkit package uses the formatted addr value and service name.
func formatAddrService2(addr string, service string) string {
	return addr + "/" + service
}

//****************************************************************
// Internal functions
//****************************************************************

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
