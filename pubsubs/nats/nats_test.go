package nats_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/gokit/actorkit/internal"

	"github.com/gokit/actorkit/pubsubs/internal/encoders"

	"github.com/gokit/actorkit/pubsubs/nats"

	"github.com/gokit/actorkit/pubsubs"
	"github.com/gokit/actorkit/pubsubs/internal/benches"
)

func TestNATS(t *testing.T) {
	natspub, err := nats.NewPublisherSubscriberFactory(context.Background(), nats.Config{
		URL:         "localhost:4222",
		Log:         &internal.TLog{},
		Marshaler:   encoders.NoAddressMarshaler{},
		Unmarshaler: encoders.NoAddressUnmarshaler{},
	})

	defer natspub.Close()

	require.NoError(t, err)
	require.NotNil(t, natspub)

	factory := nats.PubSubFactory(func(factory *nats.PublisherSubscriberFactory, topic string) (pubsubs.Publisher, error) {
		return factory.Publisher(topic)
	}, func(factory *nats.PublisherSubscriberFactory, topic string, id string, receiver pubsubs.Receiver) (pubsubs.Subscription, error) {
		return factory.Subscribe(topic, id, receiver)
	})(natspub)

	benches.PubSubFactoryTestSuite(t, factory)
}
