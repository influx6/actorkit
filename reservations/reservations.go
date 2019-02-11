package reservations

import (
	"time"

	"github.com/gokit/actorkit"
)

// Reservation exposes a interface where the provided subscriber is able to
// reserve a giving received message for processing based on it's desired behaviour.
//
// It is expected  to provide methods which will attempt to reserve a giving message
// for itself as the message sole processor. It leaves the issue of processing guarantee
// even in midst of failure to the implementer to handle as it desires, the idea is that
// we can consistently ensure that a message will not be processes by another subscriber
// in the case when such feature is not supported. The idea is Subscribers will attempt
// a race against each other to win a message processing rights, how that works is up to
// the implementer.
type Reservation interface {
	// ReserveFor reserves giving envelope for a set duration, which will be expired after
	// giving time.
	ReserveFor(actorkit.Envelope, time.Duration) error

	// Reserve reserves giving envelope for ever, ensuring no other can attempt to take giving
	// envelope anymore.
	Reserve(actorkit.Envelope) error
}
