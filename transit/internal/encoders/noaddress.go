package encoders

import (
	"encoding/json"

	"github.com/gokit/actorkit"
)

// Encodable defines a format more friendly to serializers.
type Encodable struct {
	Sender string
	Ref    string
	Data   interface{}
	Attr   map[string]string
}

// NoAddressMarshaler implements the transit.Marshaler which encodes envelopes into byte slices
// without care for the actotkit.Addr fields within encodoed version.
//
// Only to be used in tests.
type NoAddressMarshaler struct{}

// Marshal implements the transit.Marshaler interface.
func (NoAddressMarshaler) Marshal(msg actorkit.Envelope) ([]byte, error) {
	return json.Marshal(Encodable{
		Sender: msg.Sender.Addr(),
		Ref:    msg.Ref.String(),
		Data:   msg.Data,
		Attr:   msg.Header,
	})
}

// NoAddressUnmarshaler implements the transit.Unmarshaler which decodes envelopes byte slices
// into actorkit.Envelopes without care for the actotkit.Addr fields within encoded version.
//
// Only to be used in tests.
type NoAddressUnmarshaler struct{}

// Unmarshal implements the transit.Unmarshaler interface.
func (NoAddressUnmarshaler) Unmarshal(data []byte) (actorkit.Envelope, error) {
	var msg actorkit.Envelope

	var decoded Encodable
	if err := json.Unmarshal(data, &decoded); err != nil {
		return msg, err
	}

	return msg, nil
}
