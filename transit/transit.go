package transit

import "github.com/gokit/actorkit"

// Message defines a type which embodies a topic to be published to and the associated
// envelope for that topic.
type Message struct {
	Topic    string
	Envelope actorkit.Envelope
}

// Marshal exposes a method to turn an envelope into a byte slice.
type Marshaler interface {
	Marshal(envelope actorkit.Envelope) ([]byte, error)
}

// Unmarshal exposes a method to turn an byte slice into a envelope.
type Unmarshaler interface {
	Unmarshal([]byte) (actorkit.Envelope, error)
}
