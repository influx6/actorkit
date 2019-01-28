package google_test

import (
	"context"
	"testing"
	"time"

	gpubsub "cloud.google.com/go/pubsub"
	"github.com/gokit/actorkit/internal"
	"github.com/gokit/actorkit/pubsubs"
	"github.com/gokit/actorkit/pubsubs/google"
	"github.com/gokit/actorkit/pubsubs/internal/benches"
	"github.com/gokit/actorkit/pubsubs/internal/encoders"
	"github.com/stretchr/testify/require"
)

func TestGooglePubSub(t *testing.T) {
	publishers, err := google.NewPublisherSubscriberFactory(context.Background(), google.Config{
		CreateMissingTopic: true,
		ProjectID:          "actorkit",
		Log:                &internal.TLog{},
		Marshaler: &google.PubSubMarshaler{
			Now:       time.Now,
			Marshaler: encoders.NoAddressMarshaler{},
		},
		Unmarshaler: &google.PubSubUnmarshaler{
			Unmarshaler: encoders.NoAddressUnmarshaler{},
		},
	})

	require.NoError(t, err)
	require.NotNil(t, publishers)

	factory := google.PubSubFactory(func(factory *google.PublisherSubscriberFactory, topic string) (pubsubs.Publisher, error) {
		return factory.Publisher(topic, &gpubsub.PublishSettings{})
	}, func(factory *google.PublisherSubscriberFactory, topic string, id string, receiver pubsubs.Receiver) (pubsubs.Subscription, error) {
		return factory.Subscribe(topic, id, &gpubsub.SubscriptionConfig{}, receiver)
	})(publishers)

	benches.PubSubFactoryTestSuite(t, factory)
}
