package nats_test

import (
	"context"
	"testing"

	"github.com/gokit/actorkit/transit/internal/encoders"
	"github.com/stretchr/testify/assert"

	"github.com/gokit/actorkit/transit/nats"

	"github.com/gokit/actorkit"
	"github.com/gokit/actorkit/transit"
	"github.com/gokit/actorkit/transit/internal/benches"
)

func TestNATS(t *testing.T) {
	natspub, err := nats.NewPublisherSubscriberFactory(context.Background(), nats.Config{
		URL:         "localhost:4222",
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
		})
	})(natspub)

	benches.PubSubFactoryTestSuite(t, factory)
}
