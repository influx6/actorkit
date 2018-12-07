package natstreaming_test

import (
	"context"
	"testing"

	"github.com/gokit/actorkit/transit/internal/encoders"
	"github.com/stretchr/testify/assert"

	nats "github.com/gokit/actorkit/transit/natstreaming"

	"github.com/gokit/actorkit"
	"github.com/gokit/actorkit/transit"
	"github.com/gokit/actorkit/transit/internal/benches"
)

func TestNATS(t *testing.T) {
	natspub, err := nats.NewPublisherSubscriberFactory(context.Background(), nats.Config{
		URL:         "",
		ClusterID:   "",
		Marshaler:   encoders.NoAddressMarshaler{},
		Unmarshaler: encoders.NoAddressUnmarshaler{},
	})

	assert.NoError(t, err)
	assert.NotNil(t, natspub)

	factory := nats.PubSubFactory(func(factory *nats.PublisherSubscriberFactory, topic string) (transit.Publisher, error) {
		return factory.Publisher(topic)
	}, func(factory *nats.PublisherSubscriberFactory, topic string, id string, receiver transit.Receiver) (actorkit.Subscription, error) {
		return factory.Subscribe(topic, id, receiver, func(_ error) nats.Directive {
			return nats.Nack
		}, nil)
	})(natspub)

	benches.PubSubFactoryTestSuite(t, factory)
}
