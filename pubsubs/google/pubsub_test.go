package google_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	gpubsub "cloud.google.com/go/pubsub"
	"github.com/gokit/actorkit/pubsubs"
	"github.com/gokit/actorkit/pubsubs/google"
	"github.com/gokit/actorkit/pubsubs/internal/benches"
	"github.com/gokit/actorkit/pubsubs/internal/encoders"
)

func TestKafka(t *testing.T) {
	publishers, err := google.NewPublisherFactory(context.Background(), google.PublisherConfig{
		ProjectID:          "",
		CreateMissingTopic: true,
		Marshaler: &google.PubSubMarshaler{
			Marshaler: encoders.NoAddressMarshaler{},
		},
	})

	require.NoError(t, err)
	require.NotNil(t, publishers)

	subscribers, err := google.NewSubscriptionFactory(context.Background(), google.SubscriberConfig{
		ProjectID: "",
		Unmarshaler: &google.PubSubUnmarshaler{
			Unmarshaler: encoders.NoAddressUnmarshaler{},
		},
	})

	require.NoError(t, err)
	require.NotNil(t, subscribers)

	factory := google.PubSubFactory(func(factory *google.PublisherFactory, topic string) (pubsubs.Publisher, error) {
		return factory.Publisher(topic, &gpubsub.PublishSettings{})
	}, func(factory *google.SubscriptionFactory, topic string, id string, receiver pubsubs.Receiver) (pubsubs.Subscription, error) {
		return factory.Subscribe(topic, id, &gpubsub.SubscriptionConfig{}, receiver)
	})(publishers, subscribers)

	benches.PubSubFactoryTestSuite(t, factory)
}
