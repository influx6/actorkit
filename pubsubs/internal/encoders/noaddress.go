package encoders

import (
	"encoding/json"

	"github.com/gokit/xid"

	"github.com/gokit/actorkit"
)

// Encodable defines a format more friendly to serializers.
type Encodable struct {
	Sender string
	Ref    string
	Data   interface{}
	Attr   map[string]string
}

// NoAddressMarshaler implements the pubsubs.Marshaler which encodes envelopes into byte slices
// without care for the actotkit.Addr fields within encodoed version.
//
// Only to be used in tests.
type NoAddressMarshaler struct{}

// Marshal implements the pubsubs.Marshaler interface.
func (NoAddressMarshaler) Marshal(msg actorkit.Envelope) ([]byte, error) {
	return json.Marshal(Encodable{
		Sender: msg.Sender.Addr(),
		Ref:    msg.Ref.String(),
		Data:   msg.Data,
		Attr:   msg.Header,
	})
}

// NoAddressUnmarshaler implements the pubsubs.Unmarshaler which decodes envelopes byte slices
// into actorkit.Envelopes without care for the actotkit.Addr fields within encoded version.
//
// Only to be used in tests.
type NoAddressUnmarshaler struct{}

// Unmarshal implements the pubsubs.Unmarshaler interface.
func (NoAddressUnmarshaler) Unmarshal(data []byte) (actorkit.Envelope, error) {
	var msg actorkit.Envelope

	var decoded Encodable
	if err := json.Unmarshal(data, &decoded); err != nil {
		return msg, err
	}

	id, err := xid.FromString(decoded.Ref)
	if err != nil {
		return msg, err
	}

	msg.Ref = id
	msg.Data = decoded.Data
	msg.Header = decoded.Attr
	msg.Sender = actorkit.DeadLetters()

	return msg, nil
}
