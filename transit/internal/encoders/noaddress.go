package encoders

import (
	"errors"

	"github.com/gokit/actorkit"
)

// NoAddressMarshaler implements the transit.Marshaler which encodes envelopes into byte slices
// without care for the actotkit.Addr fields within encodoed version.
//
// Only to be used in tests.
type NoAddressMarshaler struct{}

// Marshal implements the transit.Marshaler interface.
func (NoAddressMarshaler) Marshal(msg actorkit.Envelope) ([]byte, error) {
	return nil, errors.New("not implemented")
}

// NoAddressUnmarshaler implements the transit.Unmarshaler which decodes envelopes byte slices
// into actorkit.Envelopes without care for the actotkit.Addr fields within encoded version.
//
// Only to be used in tests.
type NoAddressUnmarshaler struct{}

// Unmarshal implements the transit.Unmarshaler interface.
func (NoAddressUnmarshaler) Unmarshal(data []byte) (actorkit.Envelope, error) {
	var decoded actorkit.Envelope
	return decoded, errors.New("not implemented")
}
